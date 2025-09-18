# orchestrate_maps_describe.py
import os
import time
import queue
import threading
import subprocess
from pathlib import Path
import pandas as pd
import re

# ============ КОНФИГ ============
DATA_DIR = Path("gmapsdata")
DATA_DIR.mkdir(parents=True, exist_ok=True)
CENTROIDS_CSV = Path("jakarta_fitness_dots_needed.csv")

# Флаги "тишины" и логов:
# QUIET=1 (по умолчанию) — не печатать служебные сообщения
# FOLLOW_LOGS=0 (по умолчанию) — не читать docker logs -f
QUIET = os.getenv("QUIET", "1") != "0"
FOLLOW_LOGS = os.getenv("FOLLOW_LOGS", "0") == "1"

USE_FAST_MODE = False
SCRAPER_IMAGE = "gosom/google-maps-scraper"
ZOOM = 18
RADIUS = 350
LANG = "id"
DEPTH = 2
CONCURRENCY = 1
WARMUP_SECONDS = 40
STALL_WINDOW_SECONDS = 120
EXIT_ON_INACTIVITY = "10m"
MAX_JOB_SECONDS = 10 * 60
NAME_PREFIX = "gmaps_keywords_"
DOCKER_PLATFORM = os.getenv("DOCKER_PLATFORM", "").strip()
MAX_OBJECTS = 3  # Остановка после 3 объектов

# ================================
# ---------- УТИЛИТЫ ----------
def _log(msg: str):
    if not QUIET:
        print(msg)

def run(cmd, timeout=None):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except Exception as e:
        return 1, "", str(e)

def make_container_name(idx):
    return f"{NAME_PREFIX}{idx}_{int(time.time())}"

def cleanup_old(prefix=NAME_PREFIX):
    rc, out, _ = run(["docker", "ps", "-a", "--format", "{{.Names}}"])
    if rc != 0:
        return
    for nm in out.splitlines():
        if nm.startswith(prefix):
            run(["docker", "rm", "-f", nm])

def docker_run_cmd(lat, lon, keywords_path, results_path, name):
    cmd = ["docker", "run", "-d", "--rm", "--name", name]
    if DOCKER_PLATFORM:
        cmd += ["--platform", DOCKER_PLATFORM]
    cmd += [
        "-v", f"{os.getcwd()}/gmapsdata:/gmapsdata",
        SCRAPER_IMAGE,
        "-c", "6",
        "-input", f"/gmapsdata/{keywords_path.name}",
        "-results", f"/gmapsdata/{results_path.name}",
        "-geo", f"{lat},{lon}",
        "-zoom", str(ZOOM),
        "-radius", str(RADIUS),
        "-depth", str(DEPTH),
        "-lang", LANG,
        "-exit-on-inactivity", EXIT_ON_INACTIVITY,
    ]
    if USE_FAST_MODE:
        cmd.append("-fast-mode")
    return cmd

def container_is_running(name):
    rc, out, _ = run(["docker", "inspect", "-f", "{{.State.Running}}", name])
    return rc == 0 and "true" in out.lower()

def stop_container(name):
    run(["docker", "stop", "-t", "10", name], timeout=20)
    run(["docker", "rm", "-f", name], timeout=20)

def follow_logs(name, idx, stop_event):
    """Опционально следим за логами контейнера, чтобы досрочно остановиться по паттернам.
       Сообщения в консоль НЕ выводим."""
    if not FOLLOW_LOGS:
        return
    try:
        p = subprocess.Popen(
            ["docker", "logs", "-f", name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        for line in p.stdout:
            if not line:
                break
            # Без печати: лишь распознаём сигналы для ускоренной остановки
            if re.search(r"[1-9]\s+places\s+found", line):
                stop_event.set()
            elif re.search(r"failed to parse search results: empty business list", line):
                stop_event.set()
    except Exception:
        pass

def check_results_file(results_file, idx):
    try:
        df_results = pd.read_csv(results_file)
        return len(df_results)
    except pd.errors.EmptyDataError:
        return 0
    except Exception:
        return 0

def safe_touch(p: Path):
    """Создать пустой файл, если нет. Без падения на PermissionError (docker root)."""
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            with open(p, "a", encoding="utf-8"):
                pass
    except PermissionError:
        # Файл может принадлежать root из контейнера — просто пропускаем.
        pass
    except Exception:
        pass

# ---------- ДЖОБА ----------
def run_once(idx, title, lat, lon, results_file):
    queries = [str(title).strip(), "fitness", "gym", "yoga studio", "pylates studio"]
    if not queries[0]:
        safe_touch(results_file)
        return 0

    # Записываем все запросы в один файл
    keywords_path = DATA_DIR / f"keywords_{idx}.txt"
    with open(keywords_path, "w", encoding="utf-8") as f:
        for query in queries:
            f.write(query + "\n")

    if not os.access(DATA_DIR, os.W_OK):
        safe_touch(results_file)
        return 0

    name = make_container_name(idx)
    cmd = docker_run_cmd(lat, lon, keywords_path, results_file, name)

    rc, out, err = run(cmd)
    if rc != 0 or not out:
        safe_touch(results_file)
        return 0

    stop_event = threading.Event()
    log_thread = threading.Thread(target=follow_logs, args=(name, idx, stop_event), daemon=True)
    log_thread.start()

    t_start = time.time()
    last_size = None
    last_change = None
    file_checked = False
    num_rows = 0

    try:
        while True:
            if not container_is_running(name):
                break

            elapsed = time.time() - t_start
            if elapsed >= MAX_JOB_SECONDS:
                stop_container(name)
                break

            if results_file.exists():
                if not file_checked:
                    file_checked = True

                num_rows = check_results_file(results_file, idx)
                if num_rows >= MAX_OBJECTS:
                    stop_container(name)
                    break

                size = results_file.stat().st_size
                if last_change is None:
                    last_change = time.time()
                    last_size = size
                else:
                    if size != last_size:
                        last_change = time.time()
                        last_size = size
                    else:
                        if (time.time() - last_change) >= STALL_WINDOW_SECONDS:
                            stop_container(name)
                            break
            else:
                if elapsed >= WARMUP_SECONDS + STALL_WINDOW_SECONDS:
                    stop_container(name)
                    break

            # Сигнал от логов (если FOLLOW_LOGS=1)
            if stop_event.is_set():
                # Даём скрэперу доли секунды дописать файл и останавливаем
                time.sleep(0.5)
                stop_container(name)
                break

            time.sleep(0.2)
    finally:
        if container_is_running(name):
            stop_container(name)

    if not results_file.exists():
        safe_touch(results_file)

    return num_rows

# ---------- ПУЛ ----------
def worker(tasks: queue.Queue):
    while True:
        try:
            idx, title, plus_code, lat, lon, results_file = tasks.get_nowait()
        except queue.Empty:
            return
        try:
            if results_file.exists():
                if results_file.stat().st_size == 0:
                    # Удаляем пустышку и пытаемся снова
                    try:
                        results_file.unlink()
                    except Exception:
                        pass
                else:
                    num_rows = check_results_file(results_file, idx)
                    if num_rows >= MAX_OBJECTS:
                        # НЕ вызываем task_done здесь — он будет вызван в finally
                        continue
            run_once(idx, title, lat, lon, results_file)
        except Exception:
            # Без печати ошибок — создаём пустой файл, чтобы пометить как обработанный
            safe_touch(results_file)
        finally:
            tasks.task_done()

# ---------- MAIN ----------
if __name__ == "__main__":
    df = pd.read_csv(CENTROIDS_CSV)
    required_cols = {"title", "plus_code", "latitude", "longitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"В {CENTROIDS_CSV} нет колонок: {missing}")

    tasks = queue.Queue()
    for i, r in df.iterrows():
        title = r["title"]
        plus = r["plus_code"]
        lat = float(r["latitude"])
        lon = float(r["longitude"])
        idx = i + 1
        results_file = DATA_DIR / f"results_jakarta_{idx}.csv"
        tasks.put((idx, title, plus, lat, lon, results_file))

    cleanup_old()

    threads = []
    for _ in range(CONCURRENCY):
        t = threading.Thread(target=worker, args=(tasks,), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
