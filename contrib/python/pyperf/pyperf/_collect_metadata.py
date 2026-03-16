import datetime
import os
import platform
import re
import socket
import sys
import time
try:
    import resource
except ImportError:
    resource = None

try:
    from pyperf._utils import USE_PSUTIL
    if not USE_PSUTIL:
        psutil = None
    else:
        import psutil
except ImportError:
    psutil = None

import pyperf
from pyperf._cli import format_metadata
from pyperf._cpu_utils import (format_cpu_list,
                               parse_cpu_list, get_isolated_cpus,
                               get_logical_cpu_count, format_cpu_infos,
                               set_cpu_affinity)
from pyperf._formatter import format_timedelta, format_datetime
from pyperf._process_time import get_max_rss
from pyperf._utils import (MS_WINDOWS,
                           open_text, read_first_line, sysfs_path, proc_path)
if MS_WINDOWS:
    from pyperf._win_memory import check_tracking_memory, get_peak_pagefile_usage


def normalize_text(text):
    text = str(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def collect_python_metadata(metadata):
    # Implementation
    impl = pyperf.python_implementation()
    metadata['python_implementation'] = impl

    # Version
    version = platform.python_version()

    match = re.search(r'\[(PyPy [^ ]+)', sys.version)
    if match:
        version = '%s (Python %s)' % (match.group(1), version)

    bits = platform.architecture()[0]
    if bits:
        if bits == '64bit':
            bits = '64-bit'
        elif bits == '32bit':
            bits = '32-bit'
        version = '%s (%s)' % (version, bits)

    # '74667320778e' in 'Python 2.7.12+ (2.7:74667320778e,'
    match = re.search(r'^[^(]+\([^:]+:([a-f0-9]{6,}\+?),', sys.version)
    if match:
        revision = match.group(1)
    else:
        # 'bbd45126bc691f669c4ebdfbd74456cd274c6b92'
        # in 'Python 2.7.10 (bbd45126bc691f669c4ebdfbd74456cd274c6b92,'
        match = re.search(r'^[^(]+\(([a-f0-9]{6,}\+?),', sys.version)
        if match:
            revision = match.group(1)
        else:
            revision = None
    if revision:
        version = '%s revision %s' % (version, revision)
    metadata['python_version'] = version

    if sys.executable:
        metadata['python_executable'] = sys.executable

    # timer
    info = time.get_clock_info('perf_counter')
    metadata['timer'] = ('%s, resolution: %s'
                         % (info.implementation,
                            format_timedelta(info.resolution)))

    # PYTHONHASHSEED
    if os.environ.get('PYTHONHASHSEED'):
        hash_seed = os.environ['PYTHONHASHSEED']
        try:
            if hash_seed != "random":
                hash_seed = int(hash_seed)
        except ValueError:
            pass
        else:
            metadata['python_hash_seed'] = hash_seed

    # compiler
    python_compiler = normalize_text(platform.python_compiler())
    if python_compiler:
        metadata['python_compiler'] = python_compiler

    # CFLAGS
    try:
        import sysconfig
    except ImportError:
        pass
    else:
        cflags = sysconfig.get_config_var('CFLAGS')
        if cflags:
            cflags = normalize_text(cflags)
            metadata['python_cflags'] = cflags

        config_args = sysconfig.get_config_var('CONFIG_ARGS')
        if config_args:
            config_args = normalize_text(config_args)
            metadata['python_config_args'] = config_args

    # GC disabled?
    try:
        import gc
    except ImportError:
        pass
    else:
        if not gc.isenabled():
            metadata['python_gc'] = 'disabled'


def read_proc(path):
    path = proc_path(path)
    try:
        with open_text(path) as fp:
            for line in fp:
                yield line.rstrip()
    except OSError:
        return


def collect_linux_metadata(metadata):
    # ASLR
    for line in read_proc('sys/kernel/randomize_va_space'):
        if line == '0':
            metadata['aslr'] = 'No randomization'
        elif line == '1':
            metadata['aslr'] = 'Conservative randomization'
        elif line == '2':
            metadata['aslr'] = 'Full randomization'
        break


def get_cpu_affinity():
    if hasattr(os, 'sched_getaffinity'):
        return os.sched_getaffinity(0)

    if psutil is not None:
        proc = psutil.Process()
        # cpu_affinity() is only available on Linux, Windows and FreeBSD
        if hasattr(proc, 'cpu_affinity'):
            return proc.cpu_affinity()

    return None


def collect_system_metadata(metadata):
    metadata['platform'] = platform.platform(True, False)
    if sys.platform.startswith('linux'):
        collect_linux_metadata(metadata)

    # on linux, load average over 1 minute
    for line in read_proc("loadavg"):
        fields = line.split()
        loadavg = fields[0]
        metadata['load_avg_1min'] = float(loadavg)

        if len(fields) >= 4 and '/' in fields[3]:
            runnable_threads = fields[3].split('/', 1)[0]
            runnable_threads = int(runnable_threads)
            metadata['runnable_threads'] = runnable_threads

    if 'load_avg_1min' not in metadata and hasattr(os, 'getloadavg'):
        metadata['load_avg_1min'] = os.getloadavg()[0]

    # Hostname
    hostname = socket.gethostname()
    if hostname:
        metadata['hostname'] = hostname

    # Boot time
    boot_time = None
    for line in read_proc("stat"):
        if not line.startswith("btime "):
            continue
        boot_time = int(line[6:])
        break

    if boot_time is None and psutil:
        boot_time = psutil.boot_time()

    if boot_time is not None:
        btime = datetime.datetime.fromtimestamp(boot_time)
        metadata['boot_time'] = format_datetime(btime)
        metadata['uptime'] = time.time() - boot_time


def collect_memory_metadata(metadata):
    if resource is not None:
        metadata["mem_max_rss"] = get_max_rss(children=False)

    # Note: Don't collect VmPeak of /proc/self/status on Linux because it is
    # not accurate. See pyperf._linux_memory for more accurate memory metrics.

    # On Windows, use GetProcessMemoryInfo() if available
    if MS_WINDOWS and not check_tracking_memory():
        usage = get_peak_pagefile_usage()
        if usage:
            metadata['mem_peak_pagefile_usage'] = usage


def collect_cpu_freq(metadata, cpus):
    # Parse /proc/cpuinfo: search for 'cpu MHz' (Intel) or 'clock' (Power8)
    cpu_set = set(cpus)
    cpu_freq = {}
    cpu = None
    for line in read_proc('cpuinfo'):
        line = line.rstrip()

        if line.startswith('processor'):
            # Intel format, example where \t is a tab (U+0009 character):
            # processor\t: 7
            # model name\t: Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz
            # cpu MHz\t\t: 800.009
            match = re.match(r'^processor\s*: ([0-9]+)', line)
            if match is None:
                # IBM Z
                # Example: "processor 0: version = 00,  identification = [...]"
                match = re.match(r'^processor ([0-9]+): ', line)
                if match is None:
                    # unknown /proc/cpuinfo format: silently ignore and exit
                    return

            cpu = int(match.group(1))
            if cpu not in cpu_set:
                # skip this CPU
                cpu = None

        elif line.startswith('cpu MHz') and cpu is not None:
            # Intel: 'cpu MHz : 1261.613'
            mhz = line.split(':', 1)[-1].strip()
            mhz = float(mhz)
            mhz = int(round(mhz))
            cpu_freq[cpu] = '%s MHz' % mhz

        elif line.startswith('clock') and line.endswith('MHz') and cpu is not None:
            # Power8: 'clock : 3425.000000MHz'
            mhz = line[:-3].split(':', 1)[-1].strip()
            mhz = float(mhz)
            mhz = int(round(mhz))
            cpu_freq[cpu] = '%s MHz' % mhz

    if not cpu_freq:
        return

    metadata['cpu_freq'] = '; '.join(format_cpu_infos(cpu_freq))


def get_cpu_config(cpu):
    sys_cpu_path = sysfs_path("devices/system/cpu")
    info = []

    path = os.path.join(sys_cpu_path, "cpu%s/cpufreq/scaling_driver" % cpu)
    scaling_driver = read_first_line(path)
    if scaling_driver:
        info.append('driver:%s' % scaling_driver)

    if scaling_driver == 'intel_pstate':
        path = os.path.join(sys_cpu_path, "intel_pstate/no_turbo")
        no_turbo = read_first_line(path)
        if no_turbo == '1':
            info.append('intel_pstate:no turbo')
        elif no_turbo == '0':
            info.append('intel_pstate:turbo')

    path = os.path.join(sys_cpu_path, "cpu%s/cpufreq/scaling_governor" % cpu)
    scaling_governor = read_first_line(path)
    if scaling_governor:
        info.append('governor:%s' % scaling_governor)

    return info


def collect_cpu_config(metadata, cpus):
    nohz_full = read_first_line(sysfs_path('devices/system/cpu/nohz_full'))
    if nohz_full:
        nohz_full = parse_cpu_list(nohz_full)

    isolated = get_isolated_cpus()
    if isolated:
        isolated = set(isolated)

    configs = {}
    for cpu in cpus:
        config = get_cpu_config(cpu)
        if nohz_full and cpu in nohz_full:
            config.append('nohz_full')
        if isolated and cpu in isolated:
            config.append('isolated')
        if config:
            configs[cpu] = ', '.join(config)
    config = format_cpu_infos(configs)

    cpuidle = read_first_line('/sys/devices/system/cpu/cpuidle/current_driver')
    if cpuidle:
        config.append('idle:%s' % cpuidle)

    if not config:
        return
    metadata['cpu_config'] = '; '.join(config)


def get_cpu_temperature(path, cpu_temp):
    hwmon_name = read_first_line(os.path.join(path, 'name'))
    if not hwmon_name.startswith('coretemp'):
        return

    index = 1
    while True:
        template = os.path.join(path, "temp%s_%%s" % index)

        try:
            temp_label = read_first_line(template % 'label', error=True)
        except OSError:
            break

        temp_input = read_first_line(template % 'input', error=True)
        temp_input = float(temp_input) / 1000
        # On Python 2, u"%.0f\xb0C" introduces unicode errors if the
        # locale encoding is ASCII, so use a space.
        temp_input = "%.0f C" % temp_input

        item = '%s:%s=%s' % (hwmon_name, temp_label, temp_input)
        cpu_temp.append(item)

        index += 1


def collect_cpu_temperatures(metadata):
    path = sysfs_path("class/hwmon")
    try:
        names = os.listdir(path)
    except OSError:
        return None

    cpu_temp = []
    for name in names:
        hwmon = os.path.join(path, name)
        get_cpu_temperature(hwmon, cpu_temp)
    if not cpu_temp:
        return None

    metadata['cpu_temp'] = ', '.join(cpu_temp)


def collect_cpu_affinity(metadata, cpu_affinity, cpu_count):
    if not cpu_affinity:
        return
    if not cpu_count:
        return

    # CPU affinity
    if set(cpu_affinity) == set(range(cpu_count)):
        return

    metadata['cpu_affinity'] = format_cpu_list(cpu_affinity)


def collect_cpu_model(metadata):
    for line in read_proc("cpuinfo"):
        if line.startswith('model name'):
            model_name = line.split(':', 1)[1].strip()
            if model_name:
                metadata['cpu_model_name'] = model_name
            break

        if line.startswith('machine'):
            machine = line.split(':', 1)[1].strip()
            if machine:
                metadata['cpu_machine'] = machine
            break


def collect_cpu_metadata(metadata):
    collect_cpu_model(metadata)

    # CPU count
    cpu_count = get_logical_cpu_count()
    if cpu_count:
        metadata['cpu_count'] = cpu_count

    cpu_affinity = get_cpu_affinity()
    collect_cpu_affinity(metadata, cpu_affinity, cpu_count)

    all_cpus = cpu_affinity
    if not all_cpus and cpu_count:
        all_cpus = tuple(range(cpu_count))

    if all_cpus:
        collect_cpu_freq(metadata, all_cpus)
        collect_cpu_config(metadata, all_cpus)

    collect_cpu_temperatures(metadata)


def collect_metadata(process=True):
    metadata = {'perf_version': pyperf.__version__, 'date': format_datetime(datetime.datetime.now())}

    collect_system_metadata(metadata)
    collect_cpu_metadata(metadata)
    if process:
        collect_python_metadata(metadata)
        collect_memory_metadata(metadata)

    return metadata


def cmd_collect_metadata(args):
    filename = args.output
    if filename and os.path.exists(filename):
        print("ERROR: The JSON file %r already exists" % filename)
        sys.exit(1)

    cpus = args.affinity
    if cpus:
        if not set_cpu_affinity(cpus):
            print("ERROR: failed to set the CPU affinity")
            sys.exit(1)
    else:
        cpus = get_isolated_cpus()
        if cpus:
            set_cpu_affinity(cpus)
            # ignore if set_cpu_affinity() failed

    run = pyperf.Run([1.0])
    metadata = run.get_metadata()
    if metadata:
        print("Metadata:")
        for line in format_metadata(metadata):
            print(line)

    if filename:
        run = run._update_metadata({'name': 'metadata'})
        bench = pyperf.Benchmark([run])
        bench.dump(filename)
