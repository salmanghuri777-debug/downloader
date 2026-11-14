"""Local direct-download engine: queue, Range resume, retries, limits, and history."""
from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time
import urllib.parse
import uuid
from pathlib import Path


class JobError(Exception):
    pass


class Cancelled(Exception):
    pass


class RangeUnsupported(Exception):
    pass


class JobControl:
    def __init__(self, speed_limit: int = 0):
        self.resume = threading.Event()
        self.resume.set()
        self.cancel = threading.Event()
        self.lock = threading.Lock()
        self.speed_limit = speed_limit
        self.bytes_received = 0
        self.last_db_update = 0.0
        self.started = time.monotonic()

    def checkpoint(self):
        if self.cancel.is_set():
            raise Cancelled()
        while not self.resume.wait(0.25):
            if self.cancel.is_set():
                raise Cancelled()
        if self.cancel.is_set():
            raise Cancelled()

    def throttle(self, amount: int):
        if self.speed_limit <= 0:
            return
        with self.lock:
            self.bytes_received += amount
            target = self.bytes_received / self.speed_limit
            delay = max(0.0, target - (time.monotonic() - self.started))
        deadline = time.monotonic() + delay
        while time.monotonic() < deadline:
            self.checkpoint()
            time.sleep(min(0.2, deadline - time.monotonic()))


class DownloadEngine:
    def __init__(self, state_dir: Path, download_dir: Path, validate_url, fetch, filename_for_response, sniff_type, safe_filename, max_active: int = 2, workers_per_file: int = 4):
        self.state_dir = Path(state_dir)
        self.parts_root = self.state_dir / "parts"
        self.db_path = self.state_dir / "history.sqlite3"
        self.download_dir = Path(download_dir)
        self.validate_url = validate_url
        self.fetch = fetch
        self.filename_for_response = filename_for_response
        self.sniff_type = sniff_type
        self.safe_filename = safe_filename
        self.max_active = max_active
        self.workers_per_file = workers_per_file
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_active, thread_name_prefix="ahd-job")
        self.controls: dict[str, JobControl] = {}
        self.futures: dict[str, concurrent.futures.Future] = {}
        self.lock = threading.RLock()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.parts_root.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_db()
        self._recover_jobs()

    def _connect(self):
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize_db(self):
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, url TEXT NOT NULL, filename TEXT NOT NULL, status TEXT NOT NULL,
                total_bytes INTEGER, received_bytes INTEGER NOT NULL DEFAULT 0, speed_limit INTEGER NOT NULL DEFAULT 0,
                supports_range INTEGER NOT NULL DEFAULT 0, error TEXT, filepath TEXT, sha256 TEXT,
                duplicate_of TEXT, created REAL NOT NULL, updated REAL NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS jobs_url_status ON jobs(url, status)")
            db.execute("CREATE INDEX IF NOT EXISTS jobs_created ON jobs(created DESC)")

    def _recover_jobs(self):
        now = time.time()
        with self._connect() as db:
            db.execute("UPDATE jobs SET status='queued', updated=?, error=NULL WHERE status='downloading'", (now,))
            rows = db.execute("SELECT id, speed_limit FROM jobs WHERE status='queued'").fetchall()
        for row in rows:
            self._submit(row["id"], row["speed_limit"])

    def _submit(self, job_id: str, speed_limit: int = 0):
        with self.lock:
            current = self.futures.get(job_id)
            if current is not None and not current.done():
                return
            control = self.controls.get(job_id)
            if control is None or control.cancel.is_set():
                control = JobControl(speed_limit)
                self.controls[job_id] = control
            self.futures[job_id] = self.executor.submit(self._run_job, job_id, control)

    def enqueue(self, raw_url: str, speed_limit_kbps: int = 0) -> dict:
        url = self.validate_url(raw_url)
        try:
            kbps = max(0, min(int(speed_limit_kbps or 0), 1024 * 1024))
        except (TypeError, ValueError):
            raise JobError("Speed limit must be a number of KB/s (or 0 for unlimited).")
        speed_limit = kbps * 1024
        now = time.time()
        with self._connect() as db:
            existing = db.execute("SELECT id FROM jobs WHERE url=? AND status IN ('queued','downloading','paused','complete','duplicate') ORDER BY created DESC LIMIT 1", (url,)).fetchone()
            if existing:
                job = self.get(existing["id"])
                job["alreadyQueued"] = True
                return job
            job_id = uuid.uuid4().hex
            name = self.safe_filename(urllib.parse.unquote(Path(urllib.parse.urlsplit(url).path).name) or "download")
            db.execute("INSERT INTO jobs(id,url,filename,status,speed_limit,created,updated) VALUES(?,?,?,?,?,?,?)", (job_id,url,name,"queued",speed_limit,now,now))
        self._submit(job_id, speed_limit)
        return self.get(job_id)

    def action(self, job_id: str, action: str) -> dict:
        job = self.get(job_id)
        if not job:
            raise JobError("Download job not found.")
        with self.lock:
            control = self.controls.get(job_id)
            if action == "pause":
                if job["status"] not in {"queued", "downloading"}:
                    return job
                if control is None:
                    control = JobControl(job["speedLimit"])
                    self.controls[job_id] = control
                control.resume.clear()
                self._set_status(job_id, "paused")
            elif action == "resume":
                if job["status"] not in {"paused", "failed", "queued"}:
                    return job
                if control is None or control.cancel.is_set():
                    control = JobControl(job["speedLimit"])
                    self.controls[job_id] = control
                control.resume.set()
                self._set_status(job_id, "queued", error=None)
                self._submit(job_id, job["speedLimit"])
            elif action == "cancel":
                if job["status"] in {"complete", "duplicate", "cancelled"}:
                    return job
                if control is None:
                    control = JobControl(job["speedLimit"])
                    self.controls[job_id] = control
                control.cancel.set()
                control.resume.set()
                self._set_status(job_id, "cancelled")
            else:
                raise JobError("Action must be pause, resume, or cancel.")
        return self.get(job_id)

    def get(self, job_id: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._public(row) if row else None

    def list_jobs(self, limit: int = 100) -> list[dict]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM jobs ORDER BY created DESC LIMIT ?", (max(1,min(limit,250)),)).fetchall()
        return [self._public(row) for row in rows]

    def _public(self, row):
        job = dict(row)
        job["speedLimitKbps"] = round((job.pop("speed_limit") or 0) / 1024)
        job["totalBytes"] = job.pop("total_bytes")
        job["receivedBytes"] = job.pop("received_bytes")
        job["supportsRange"] = bool(job.pop("supports_range"))
        job.pop("url", None)
        return job

    def _set_status(self, job_id: str, status: str, error: str | None = ""):
        with self._connect() as db:
            db.execute("UPDATE jobs SET status=?, error=?, updated=? WHERE id=?", (status,error,time.time(),job_id))

    def _set_metadata(self, job_id: str, filename: str, total: int | None, supports_range: bool):
        with self._connect() as db:
            db.execute("UPDATE jobs SET filename=?, total_bytes=?, supports_range=?, updated=? WHERE id=?", (filename,total,int(supports_range),time.time(),job_id))

    def _set_progress(self, job_id: str, value: int):
        now = time.monotonic()
        control = self.controls.get(job_id)
        if control:
            with control.lock:
                if now - control.last_db_update < 0.5:
                    return
                control.last_db_update = now
        with self._connect() as db:
            db.execute("UPDATE jobs SET received_bytes=?, updated=? WHERE id=?", (value,time.time(),job_id))

    def _run_job(self, job_id: str, control: JobControl):
        job = self.get(job_id)
        if not job:
            return
        part_dir = self.parts_root / job_id
        part_dir.mkdir(parents=True, exist_ok=True)
        try:
            control.checkpoint()
            with self._connect() as db:
                row = db.execute("SELECT url FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                return
            url = row["url"]
            probe_error = None
            for attempt in range(5):
                control.checkpoint()
                try:
                    filename, total, supports_range = self._probe(url)
                    probe_error = None
                    break
                except Exception as exc:
                    probe_error = exc
                    if attempt == 4:
                        raise
                    self._backoff(control, attempt)
            if probe_error:
                raise probe_error
            self._set_metadata(job_id, filename, total, supports_range)
            self._set_status(job_id, "downloading", error=None)
            if supports_range and total and total >= 8 * 1024 * 1024:
                try:
                    self._download_ranged(job_id,url,part_dir,control,total)
                except RangeUnsupported:
                    # Some servers advertise byte ranges but reject parallel requests.
                    # Fall back to one resumable stream rather than failing the job.
                    for part in part_dir.glob("segment-*.part"):
                        part.unlink(missing_ok=True)
                    self._set_metadata(job_id, filename, total, False)
                    self._download_single(job_id,url,part_dir,control,total,False)
            else:
                self._download_single(job_id,url,part_dir,control,total,supports_range)
            control.checkpoint()
            output_part = part_dir / "assembled.part"
            segments = sorted(path for path in part_dir.glob("segment-*.part") if path.is_file())
            with output_part.open("wb") as target:
                for segment in segments:
                    with segment.open("rb") as source:
                        shutil.copyfileobj(source,target,1024*1024)
            if total and output_part.stat().st_size != total:
                raise JobError("The completed file size does not match the server's size.")
            digest = self._sha256(output_part)
            with self._connect() as db:
                duplicate = db.execute("SELECT id FROM jobs WHERE sha256=? AND status='complete' AND id<>? LIMIT 1", (digest,job_id)).fetchone()
            if duplicate:
                output_part.unlink(missing_ok=True)
                self._set_duplicate(job_id,digest,duplicate["id"])
                shutil.rmtree(part_dir,ignore_errors=True)
                return
            output_path = self._unique_path(self.download_dir / self.safe_filename(filename))
            output_part.replace(output_path)
            with self._connect() as db:
                db.execute("UPDATE jobs SET status='complete',received_bytes=?,filepath=?,sha256=?,error=NULL,updated=? WHERE id=?", (output_path.stat().st_size,str(output_path),digest,time.time(),job_id))
            shutil.rmtree(part_dir,ignore_errors=True)
        except Cancelled:
            self._set_status(job_id,"cancelled",error=None)
            shutil.rmtree(part_dir,ignore_errors=True)
        except Exception as exc:
            with self._connect() as db:
                db.execute("UPDATE jobs SET status='failed',error=?,updated=? WHERE id=?", (str(exc)[:500],time.time(),job_id))
        finally:
            with self.lock:
                if self.controls.get(job_id) is control and control.cancel.is_set():
                    self.controls.pop(job_id,None)

    def _set_duplicate(self, job_id: str, digest: str, duplicate_of: str):
        with self._connect() as db:
            db.execute("UPDATE jobs SET status='duplicate',sha256=?,duplicate_of=?,received_bytes=total_bytes,error=NULL,updated=? WHERE id=?", (digest,duplicate_of,time.time(),job_id))

    def _probe(self, url: str) -> tuple[str,int|None,bool]:
        response=self.fetch(url,"bytes=0-4095")
        with response:
            final_url=response.geturl()
            prefix=response.read(4096)
            detected=self.sniff_type(prefix)
            filename=self.filename_for_response(response,final_url,detected)
            total=None
            content_range=response.headers.get("Content-Range","")
            match=re.fullmatch(r"bytes\s+\d+-\d+/(\d+)",content_range.strip(),re.I)
            if match:
                total=int(match.group(1))
            elif response.headers.get("Content-Length"):
                try: total=int(response.headers["Content-Length"])
                except ValueError: total=None
            supports=response.status==206 and bool(match)
            return filename,total,supports

    def _segment_paths(self, part_dir: Path, count: int):
        return [part_dir / f"segment-{index}.part" for index in range(count)]

    def _download_ranged(self, job_id: str, url: str, part_dir: Path, control: JobControl, total: int):
        count=self.workers_per_file
        chunk=(total+count-1)//count
        ranges=[(index*chunk,min(total-1,(index+1)*chunk-1)) for index in range(count) if index*chunk<total]
        paths=self._segment_paths(part_dir,len(ranges))
        initial=sum(min(path.stat().st_size,ranges[i][1]-ranges[i][0]+1) for i,path in enumerate(paths) if path.exists())
        with control.lock: control.bytes_received=initial
        self._set_progress(job_id,initial)
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ranges),thread_name_prefix="ahd-range") as pool:
            futures=[pool.submit(self._download_segment,job_id,url,part_dir,control,total,start,end,index,True) for index,(start,end) in enumerate(ranges)]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def _download_single(self, job_id: str, url: str, part_dir: Path, control: JobControl, total: int|None, supports_range: bool):
        path=part_dir/"segment-0.part"
        offset=path.stat().st_size if path.exists() else 0
        if total and offset>total: path.unlink(missing_ok=True);offset=0
        with control.lock: control.bytes_received=offset
        self._set_progress(job_id,offset)
        for attempt in range(5):
            control.checkpoint()
            offset=path.stat().st_size if path.exists() else 0
            header=f"bytes={offset}-" if offset and supports_range else None
            if offset and not supports_range:
                path.unlink(missing_ok=True);offset=0
            try:
                response=self.fetch(url,header)
                with response:
                    content_range = response.headers.get("Content-Range", "")
                    range_match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", content_range.strip(), re.I)
                    if offset and (response.status != 206 or not range_match or int(range_match.group(1)) != offset):
                        path.unlink(missing_ok=True);offset=0
                        raise RangeUnsupported("The server stopped supporting resume; restarting from the beginning.")
                    mode="ab" if offset and response.status==206 else "wb"
                    with path.open(mode) as out:
                        while True:
                            control.checkpoint()
                            block=response.read(64*1024)
                            if not block: break
                            out.write(block)
                            control.throttle(len(block))
                            self._set_progress(job_id,path.stat().st_size)
                    return
            except Cancelled: raise
            except Exception:
                if attempt==4: raise
                if path.exists() and not supports_range: path.unlink(missing_ok=True)
                self._backoff(control,attempt)
        raise JobError("Download retries were exhausted.")

    def _download_segment(self, job_id: str, url: str, part_dir: Path, control: JobControl, total: int, start: int, end: int, index: int, supports_range: bool):
        path=part_dir/f"segment-{index}.part"
        expected=end-start+1
        if path.exists() and path.stat().st_size>=expected:
            with control.lock: control.bytes_received+=0
            return
        for attempt in range(5):
            control.checkpoint()
            offset=path.stat().st_size if path.exists() else 0
            if offset>expected: path.unlink(missing_ok=True);offset=0
            request_start=start+offset
            try:
                response=self.fetch(url,f"bytes={request_start}-{end}")
                with response:
                    content_range = response.headers.get("Content-Range", "")
                    range_match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+)", content_range.strip(), re.I)
                    if response.status != 206 or not range_match or (int(range_match.group(1)), int(range_match.group(2)), int(range_match.group(3))) != (request_start, end, total):
                        raise RangeUnsupported("The server rejected a parallel range request.")
                    mode="ab" if offset else "wb"
                    with path.open(mode) as out:
                        while True:
                            control.checkpoint()
                            block=response.read(min(64*1024,expected-offset))
                            if not block: break
                            out.write(block);offset+=len(block)
                            control.throttle(len(block))
                            current=sum(p.stat().st_size for p in self._segment_paths(part_dir,self.workers_per_file) if p.exists())
                            self._set_progress(job_id,current)
                            if offset>=expected: break
                    if path.stat().st_size!=expected:
                        raise JobError("A download segment ended early; retrying it.")
                    return
            except Cancelled: raise
            except Exception:
                if attempt==4: raise
                self._backoff(control,attempt)
        raise JobError("A download segment failed after retries.")

    @staticmethod
    def _backoff(control: JobControl, attempt: int):
        delay=min(8,2**attempt)
        end=time.monotonic()+delay
        while time.monotonic()<end:
            control.checkpoint();time.sleep(min(0.2,end-time.monotonic()))

    @staticmethod
    def _sha256(path: Path) -> str:
        digest=hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda:source.read(1024*1024),b""): digest.update(block)
        return digest.hexdigest()

    @staticmethod
    def _unique_path(path: Path) -> Path:
        if not path.exists(): return path
        suffix=path.suffix
        stem=path.stem
        index=1
        while True:
            candidate=path.with_name(f"{stem} ({index}){suffix}")
            if not candidate.exists(): return candidate
            index+=1
