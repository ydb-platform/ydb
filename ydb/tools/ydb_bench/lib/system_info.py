import os
import platform
import subprocess


def _physical_memory_bytes():
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def _cpu_model():
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as stream:
            for line in stream:
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass

    try:
        result = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        pass

    processor = platform.processor().strip()
    return processor or None


def collect_system_info():
    uname = platform.uname()
    return {
        "architecture": platform.machine(),
        "cpu_count": os.cpu_count(),
        "cpu_model": _cpu_model(),
        "physical_memory_bytes": _physical_memory_bytes(),
        "uname": {
            "machine": uname.machine,
            "node": uname.node,
            "release": uname.release,
            "system": uname.system,
            "version": uname.version,
        },
    }
