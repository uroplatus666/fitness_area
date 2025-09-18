import os
import time
import queue
import threading
import subprocess
from pathlib import Path
import pandas as pd

# ============ КОНФИГ ============
DATA_DIR = Path("gmapsdata")
DATA_DIR.mkdir(parents=True, exist_ok=True)

CENTROIDS_CSV = Path("jakarta_centroids_1500.csv")  # при желании подставьте другое имя

KEYWORDS = ["fitness",
            "gym",
            "yoga studio",
            "pylates studio"]

ZOOM = 17
RADIUS = 1100
LANG = "en"
USE_FAST_MODE = False
SCRAPER_IMAGE = "gosom/google-maps-scraper"

# Одновременных контейнеров
CONCURRENCY = 2

# 600 KiB
MIN_BYTES_THRESHOLD = 600 * 1024

# Один проход (без ретраев)
DEPTH = 15
WARMUP_SECONDS = 180          # первые ~3 минуты парсер обычно молчит
STALL_WINDOW_SECONDS = 220    # 2 минуты «тишины» -> останавливаем контейнер
EXIT_ON_INACTIVITY = "8m"     # внутренняя страховка скрэпера
MAX_JOB_SECONDS = 20 * 60     # жёсткий потолок: 20 минут

# Имя-префикс контейнеров
NAME_PREFIX = "gmaps_jakarta_"

# Если образ не работает на вашей архитектуре (M1/M2 и т.п.)
DOCKER_PLATFORM = os.getenv("DOCKER_PLATFORM", "").strip()

# --- ТИШИНА В КОНСОЛИ ---
# QUIET=1 (по умолчанию) — ничего не печатать; QUIET=0 — печатать минимум.
QUIET = os.getenv("QUIET", "1") != "0"
# FOLLOW_LOGS=1 — включить поток docker logs -f (по умолчанию выкл, чтобы не спамить)
FOLLOW_LOGS = os.getenv("FOLLOW_LOGS", "0") == "1"

def _log(msg: str):
    if not QUIET:
        print(msg)

# ================================

KEYWORDS_FILE = DATA_DIR / "keywords.txt"
KEYWORDS_FILE.write_text("\n".join(KEYWORDS), encoding="utf-8")

# Подготовка задач из CSV
df = pd.read_csv(CENTROIDS_CSV)
# df = df.iloc[165:]  # при необходимости оставьте срез; по умолчанию весь файл

tasks = queue.Queue()
for i, row in df.iterrows():
    lat = float(row["latitude"])
    lon = float(row["longitude"])
    idx = i + 1
    results_file = DATA_DIR / f"results_jakarta_{idx}.csv"
    tasks.put((idx, lat, lon, results_file))

# ---------- ВСПОМОГАТЕЛЬНЫЕ ----------
def run(cmd, timeout=None):
    """Запуск команды, возвращает (rc, stdout, stderr). Не кидает исключение по rc != 0."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except Exception as e:
        return 1, "", str(e)

def make_container_name(idx):
    return f"{NAME_PREFIX}{idx}_{int(time.time())}"

def cleanup_old(prefix=NAME_PREFIX):
    """Удалить все контейнеры с указанным префиксом (в том числе зависшие)."""
    rc, out, _ = run(["docker", "ps", "-a", "--format", "{{.Names}}"])
    if rc != 0:
        return
    for nm in out.splitlines():
        if nm.startswith(prefix):
            run(["docker", "rm", "-f", nm])

def docker_run_cmd(lat, lon, results_file, name):
    """Собрать команду docker run в фоне (-d) с уникальным именем и --rm."""
    cmd = ["docker", "run", "-d", "--rm", "--name", name]
    if DOCKER_PLATFORM:
        cmd += ["--platform", DOCKER_PLATFORM]
    cmd += [
        "-v", f"{os.getcwd()}/gmapsdata:/gmapsdata",
        SCRAPER_IMAGE,
        "-c", "8",
        "-input", "/gmapsdata/keywords.txt",
        "-results", f"/gmapsdata/{results_file.name}",
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
    # Мягко
    run(["docker", "stop", "-t", "10", name], timeout=20)
    # На всякий случай (если --rm не удалил)
    run(["docker", "rm", "-f", name], timeout=20)

def follow_logs(name, idx):
    """Поток для логов: docker logs -f NAME. По умолчанию ОТКЛЮЧЕНО (FOLLOW_LOGS=0)."""
    if not FOLLOW_LOGS:
        return
    try:
        p = subprocess.Popen(["docker", "logs", "-f", name],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             text=True,
                             bufsize=1)
        for line in p.stdout:
            if not line:
                break
            _log(f"[{idx}] {line.rstrip()}")
    except Exception:
        pass

# ---------- ОСНОВНАЯ ЛОГИКА ДЖОБЫ ----------
def run_once(idx, lat, lon, results_file):
    name = make_container_name(idx)
    cmd = docker_run_cmd(lat, lon, results_file, name)

    rc, out, err = run(cmd)
    if rc != 0 or not out:
        _log(f"[{idx}] Не удалось запустить контейнер: rc={rc}, err={err}")
        return

    container_id = out.splitlines()[-1].strip()
    _log(f"[{idx}] started container {name} ({container_id})")

    # Стартуем поток логов (если явно включили FOLLOW_LOGS)
    log_thread = threading.Thread(target=follow_logs, args=(name, idx), daemon=True)
    log_thread.start()

    t_start = time.time()
    last_size = None
    last_change = None

    try:
        while True:
            # 1) контейнер жив?
            if not container_is_running(name):
                _log(f"[{idx}] container {name} is not running.")
                break

            elapsed = time.time() - t_start

            # 2) жёсткий потолок
            if elapsed >= MAX_JOB_SECONDS:
                _log(f"[{idx}] hit MAX_JOB_SECONDS={MAX_JOB_SECONDS}s -> stop {name}")
                stop_container(name)
                break

            # 3) Контроль появления/роста файла (после прогрева)
            if elapsed >= WARMUP_SECONDS:
                if results_file.exists():
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
                                _log(f"[{idx}] stall {STALL_WINDOW_SECONDS}s on {results_file} -> stop {name}")
                                stop_container(name)
                                break
                else:
                    # Файл ещё не создан: если прошло STALL_WINDOW_SECONDS после прогрева — стоп
                    if (elapsed - WARMUP_SECONDS) >= STALL_WINDOW_SECONDS:
                        _log(f"[{idx}] no file after warmup+stall -> stop {name}")
                        stop_container(name)
                        break

            time.sleep(1.5)
    finally:
        # На всякий случай добиваем контейнер (если ещё жив)
        if container_is_running(name):
            stop_container(name)

    # Итог: файл сохраняем как есть
    if not results_file.exists():
        results_file.touch()
        _log(f"[{idx}] Результат пустой: {results_file} (0 bytes)")
    else:
        sz = results_file.stat().st_size
        _log(f"[{idx}] Результат: {results_file} ({sz} bytes)")
        if sz < MIN_BYTES_THRESHOLD:
            _log(f"[{idx}] Внимание: файл меньше порога {MIN_BYTES_THRESHOLD} байт, но сохранён как есть.")

    _log(f"[{idx}] Время: {int(time.time() - t_start)} сек.")

# ---------- ПУЛ РАБОТНИКОВ ----------
def worker():
    while True:
        try:
            idx, lat, lon, results_file = tasks.get_nowait()
        except queue.Empty:
            return
        try:
            if results_file.exists():
                _log(f"[{idx}] Уже есть: {results_file} ({results_file.stat().st_size} bytes). Пропуск.")
            else:
                run_once(idx, lat, lon, results_file)
        except Exception as e:
            _log(f"[{idx}] Ошибка: {e}")
        finally:
            tasks.task_done()

if __name__ == "__main__":
    # Уборка «хвостов» от прошлых запусков
    cleanup_old()

    threads = []
    for _ in range(CONCURRENCY):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    _log("Все задачи обработаны.")
