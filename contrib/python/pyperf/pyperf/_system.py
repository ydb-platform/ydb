import errno
import os.path
import platform
import re
import struct
import subprocess
import sys

from pyperf._cli import display_title
from pyperf._cpu_utils import (parse_cpu_list,
                               get_logical_cpu_count, get_isolated_cpus,
                               format_cpu_list, format_cpu_infos,
                               parse_cpu_mask, format_cpus_as_mask)
from pyperf._utils import (read_first_line, sysfs_path, proc_path, open_text,
                           popen_communicate)


MSR_IA32_MISC_ENABLE = 0x1a0
MSR_IA32_MISC_ENABLE_TURBO_DISABLE_BIT = 38

OS_LINUX = sys.platform.startswith('linux')
PLATFORM_X86 = platform.machine() in ('x86', 'x86_64', 'amd64')


def is_root():
    return (os.getuid() == 0)


def is_permission_error(exc):
    return exc.errno in (errno.EACCES, errno.EPERM)


def write_text(filename, content):
    with open_text(filename, write=True) as fp:
        fp.write(content)
        fp.flush()


def run_cmd(cmd):
    try:
        # ignore stdout and stderr
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return 127
        else:
            raise

    popen_communicate(proc)
    proc.wait()
    return proc.returncode


def get_output(cmd):
    try:
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                universal_newlines=True)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return (127, '')
        else:
            raise

    stdout = popen_communicate(proc)[0]
    exitcode = proc.returncode
    return (exitcode, stdout)


def use_intel_pstate():
    cpu = 0
    path = sysfs_path("devices/system/cpu/cpu%s/cpufreq/scaling_driver" % cpu)
    scaling_driver = read_first_line(path)
    return (scaling_driver == 'intel_pstate')


class Operation:
    @staticmethod
    def available():
        return True

    def __init__(self, name, system):
        self.name = name
        self.system = system
        self.permission_error = False
        self.tuned_for_benchmarks = None

    def advice(self, msg):
        self.system.advice('%s: %s' % (self.name, msg))

    def log_state(self, msg):
        self.system.log_state('%s: %s' % (self.name, msg))

    def log_action(self, msg):
        self.system.log_action('%s: %s' % (self.name, msg))

    def warning(self, msg):
        self.system.warning('%s: %s' % (self.name, msg))

    def error(self, msg):
        self.system.error('%s: %s' % (self.name, msg))

    def check_permission_error(self, exc):
        if is_permission_error(exc):
            self.permission_error = True

    def read_first_line(self, path):
        try:
            return read_first_line(path, error=True)
        except OSError as exc:
            self.check_permission_error(exc)
            return ''

    def show(self):
        pass

    def write(self, tune):
        pass


class IntelPstateOperation(Operation):
    @staticmethod
    def available():
        return use_intel_pstate()


class TurboBoostMSR(Operation):
    """
    Get/Set Turbo Boost mode of x86 CPUs using /dev/cpu/N/msr
    """

    @staticmethod
    def available():
        return OS_LINUX and PLATFORM_X86 and not use_intel_pstate()

    def __init__(self, system):
        super().__init__('Turbo Boost (MSR)', system)
        self.cpu_states = {}
        self.have_device = True

    def read_msr(self, cpu, reg_num, use_warnings=False):
        path = '/dev/cpu/%s/msr' % cpu
        size = struct.calcsize('Q')
        if size != 8:
            raise ValueError("need a 64-bit unsigned integer type")
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                if hasattr(os, 'pread'):
                    data = os.pread(fd, size, reg_num)
                else:
                    os.lseek(fd, reg_num, os.SEEK_SET)
                    data = os.read(fd, size)
            finally:
                os.close(fd)
        except OSError as exc:
            self.check_permission_error(exc)
            msg = "Failed to read MSR %#x from %s: %s" % (reg_num, path, exc)
            if use_warnings:
                self.warning(msg)
            else:
                self.error(msg)
            if exc.errno == errno.ENOENT:
                self.have_device = False
                msg = "Try to load the msr kernel module: sudo modprobe msr"
                if use_warnings:
                    self.warning(msg)
                else:
                    self.error(msg)
            return None

        return struct.unpack('Q', data)[0]

    def read_cpu(self, cpu):
        reg = self.read_msr(cpu, MSR_IA32_MISC_ENABLE, use_warnings=True)
        if reg is None:
            return False

        msr = bool(reg & (1 << MSR_IA32_MISC_ENABLE_TURBO_DISABLE_BIT))
        self.cpu_states[cpu] = (not msr)
        return True

    def show(self):
        if not self.have_device:
            return

        for cpu in self.system.cpus:
            if not self.read_cpu(cpu):
                break

        enabled = set()
        disabled = set()
        for cpu, state in self.cpu_states.items():
            if state:
                enabled.add(cpu)
            else:
                disabled.add(cpu)

        text = []
        if enabled:
            text.append('CPU %s: enabled' % format_cpu_list(enabled))
        if disabled:
            text.append('CPU %s: disabled' % format_cpu_list(disabled))
        if text:
            self.log_state(', '.join(text))

        self.tuned_for_benchmarks = (not enabled)
        if enabled:
            self.advice('Disable Turbo Boost on CPU %s to get more reliable '
                        'CPU frequency' % format_cpu_list(enabled))

    def write_msr(self, cpu, reg_num, value):
        path = '/dev/cpu/%s/msr' % cpu
        size = struct.calcsize('Q')
        if size != 8:
            raise ValueError("need a 64-bit unsigned integer type")
        data = struct.pack('Q', value)
        try:
            fd = os.open(path, os.O_WRONLY)
            try:
                if hasattr(os, 'pwrite'):
                    os.pwrite(fd, data, reg_num)
                else:
                    os.lseek(fd, reg_num, os.SEEK_SET)
                    os.write(fd, data)
            finally:
                os.close(fd)
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Failed to write %#x into MSR %#x using %s: %s"
                       % (value, reg_num, path, exc))
            return False

        return True

    def write_cpu(self, cpu, enabled):
        value = self.read_msr(cpu, MSR_IA32_MISC_ENABLE)
        if value is None:
            return False

        mask = (1 << MSR_IA32_MISC_ENABLE_TURBO_DISABLE_BIT)
        if not enabled:
            new_value = value | mask
        else:
            new_value = value & ~mask

        if new_value == value:
            return True

        if not self.write_msr(cpu, MSR_IA32_MISC_ENABLE, new_value):
            return False

        state = "enabled" if enabled else "disabled"
        self.log_action("Turbo Boost %s on CPU %s: MSR %#x set to %#x"
                        % (state, cpu, MSR_IA32_MISC_ENABLE, new_value))
        return True

    def write(self, tune):
        enabled = (not tune)
        if tune:
            cpus = self.system.cpus
        else:
            cpus = range(self.system.logical_cpu_count)

        for cpu in cpus:
            if not self.write_cpu(cpu, enabled):
                break


class TurboBoostIntelPstate(IntelPstateOperation):
    """
    Get/Set Turbo Boost mode of Intel CPUs by reading from/writing into
    /sys/devices/system/cpu/intel_pstate/no_turbo of the intel_pstate driver.
    """

    def __init__(self, system):
        super().__init__('Turbo Boost (intel_pstate)', system)
        self.path = sysfs_path("devices/system/cpu/intel_pstate/no_turbo")
        self.enabled = None

    def read_turbo_boost(self):
        no_turbo = self.read_first_line(self.path)
        if no_turbo == '1':
            self.enabled = False
        elif no_turbo == '0':
            self.enabled = True
        else:
            self.error("Invalid no_turbo value: %r" % no_turbo)
            self.enabled = None

    def show(self):
        self.read_turbo_boost()
        if self.enabled is not None:
            state = 'enabled' if self.enabled else 'disabled'
            self.log_state("Turbo Boost %s" % state)

            self.tuned_for_benchmarks = (not self.enabled)
            if self.enabled:
                self.advice('Disable Turbo Boost to get more reliable '
                            'CPU frequency')

    def write(self, tune):
        enable = (not tune)

        self.read_turbo_boost()
        if self.enabled == enable:
            # no_turbo already set to the expected value
            return

        content = '0' if enable else '1'
        try:
            write_text(self.path, content)
        except OSError as exc:
            # don't log a permission error if the user is root: permission
            # error as root means that Turbo Boost is disabled in the BIOS
            if not is_root():
                self.check_permission_error(exc)

            action = 'enable' if enable else 'disable'
            msg = "Failed to %s Turbo Boost" % action
            disabled_in_bios = is_permission_error(exc) and is_root()
            if disabled_in_bios:
                msg += " (Turbo Boost disabled in the BIOS?)"
            msg = "%s: failed to write into %s: %s" % (msg, self.path, exc)

            if disabled_in_bios:
                self.log_action("WARNING: %s" % msg)
            else:
                self.error(msg)
            return

        msg = "%r written into %s" % (content, self.path)
        action = 'enabled' if enable else 'disabled'
        self.log_action("Turbo Boost %s: %s" % (action, msg))


class CPUGovernor(Operation):
    """
    Get/Set CPU scaling governor
    """
    BENCHMARK_GOVERNOR = 'performance'

    @staticmethod
    def available():
        return os.path.exists(
            sysfs_path("devices/system/cpu/cpu0/cpufreq/scaling_governor")
        )

    def __init__(self, system):
        super().__init__('CPU scaling governor', system)
        self.device_syspath = sysfs_path("devices/system/cpu")

    def read_governor(self, cpu):
        filename = os.path.join(self.device_syspath, 'cpu%s/cpufreq/scaling_governor' % cpu)
        try:
            with open(filename, "r") as fp:
                return fp.readline().rstrip()
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Unable to read CPU scaling governor from %s" % filename)
            return None

    def show(self):
        cpus = {}
        for cpu in range(self.system.logical_cpu_count):
            governor = self.read_governor(cpu)
            if governor is not None:
                cpus[cpu] = governor

        infos = format_cpu_infos(cpus)
        if not infos:
            return
        self.log_state('; '.join(infos))

        self.tuned_for_benchmarks = all(
            governor == self.BENCHMARK_GOVERNOR for governor in cpus.values()
        )
        if not self.tuned_for_benchmarks:
            self.advice('Use CPU scaling governor %r'
                        % self.BENCHMARK_GOVERNOR)

    def write_governor(self, filename, new_governor):
        with open(filename, "r") as fp:
            governor = fp.readline().rstrip()

        if new_governor == governor:
            return False

        with open(filename, "w") as fp:
            fp.write(new_governor)
        return True

    def write_cpu(self, cpu, tune):
        governor = self.read_governor(cpu)
        if not governor:
            self.warning("Unable to read governor of CPU %s" % (cpu))
            return False

        new_governor = self.BENCHMARK_GOVERNOR if tune else "powersave"
        filename = os.path.join(self.device_syspath, 'cpu%s/cpufreq/scaling_governor' % cpu)
        try:
            return self.write_governor(filename, new_governor)
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Unable to write governor of CPU %s: %s"
                       % (cpu, exc))

    def write(self, tune):
        modified = []
        for cpu in self.system.cpus:
            if self.write_cpu(cpu, tune):
                modified.append(cpu)
            if self.permission_error:
                break

        if modified:
            cpus = format_cpu_list(modified)
            if tune:
                action = "set to performance"
            else:
                action = "reset to powersave"
            self.log_action("CPU scaling governor of CPUs %s %s" % (cpus, action))


class LinuxScheduler(Operation):
    """
    Check isolcpus=cpus and rcu_nocbs=cpus parameters of the Linux kernel
    command line.
    """

    @staticmethod
    def available():
        return OS_LINUX

    def __init__(self, system):
        super().__init__('Linux scheduler', system)
        self.ncpu = None
        self.linux_version = None

    def show(self):
        self.ncpu = get_logical_cpu_count()
        if self.ncpu is None:
            self.error("Unable to get the number of CPUs")
            return

        release = os.uname()[2]
        try:
            version_txt = release.split('-', 1)[0]
            self.linux_version = tuple(map(int, version_txt.split('.')))
        except ValueError:
            self.error("Failed to get the Linux version: release=%r" % release)
            return

        # isolcpus= parameter existed prior to 2.6.12-rc2 (2005)
        # which is first commit of the Linux git repository
        self.check_isolcpus()

        # Commit 3fbfbf7a3b66ec424042d909f14ba2ddf4372ea8 added rcu_nocbs
        if self.linux_version >= (3, 8):
            self.check_rcu_nocbs()

    def check_isolcpus(self):
        isolated = get_isolated_cpus()
        if isolated:
            self.log_state('Isolated CPUs (%s/%s): %s'
                           % (len(isolated), self.ncpu,
                              format_cpu_list(isolated)))
        elif self.ncpu > 1:
            self.log_state('No CPU is isolated')
            self.advice('Use isolcpus=<cpu list> kernel parameter '
                        'to isolate CPUs')

    def read_rcu_nocbs(self):
        cmdline = self.read_first_line(proc_path('cmdline'))
        if not cmdline:
            return

        match = re.search(r'\brcu_nocbs=([^ ]+)', cmdline)
        if not match:
            return

        cpus = match.group(1)
        return parse_cpu_list(cpus)

    def check_rcu_nocbs(self):
        rcu_nocbs = self.read_rcu_nocbs()
        if rcu_nocbs:
            self.log_state('RCU disabled on CPUs (%s/%s): %s'
                           % (len(rcu_nocbs), self.ncpu,
                              format_cpu_list(rcu_nocbs)))
        elif self.ncpu > 1:
            self.advice('Use rcu_nocbs=<cpu list> kernel parameter '
                        '(with isolcpus) to not schedule RCU '
                        'on isolated CPUs')


class ASLR(Operation):
    # randomize_va_space procfs existed prior to 2.6.12-rc2 (2005)
    # which is first commit of the Linux git repository

    STATE = {'0': 'No randomization',
             '1': 'Conservative randomization',
             '2': 'Full randomization'}
    path = proc_path("sys/kernel/randomize_va_space")

    @classmethod
    def available(cls):
        return os.path.exists(cls.path)

    def __init__(self, system):
        super().__init__('ASLR', system)

    def show(self):
        line = self.read_first_line(self.path)
        try:
            state = self.STATE[line]
        except KeyError:
            self.error("Failed to read %s" % self.path)
            return

        self.log_state(state)
        self.tuned_for_benchmarks = (line == '2')
        if not self.tuned_for_benchmarks:
            self.advice("Enable full randomization: write 2 into %s"
                        % self.path)

    def write(self, tune):
        value = self.read_first_line(self.path)
        if not value:
            return

        new_value = '2'
        if new_value == value:
            return

        try:
            write_text(self.path, new_value)
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Failed to write into %s: %s" % (self.path, exc))
        else:
            self.log_action("Full randomization enabled: %r written into %s"
                            % (new_value, self.path))


class CPUFrequency(Operation):
    """
    Read/Write /sys/devices/system/cpu/cpuN/cpufreq/scaling_min_freq.
    """

    @staticmethod
    def available():
        # On virtual machines, there is no cpufreq directory
        return os.path.exists(sysfs_path("devices/system/cpu/cpu0/cpufreq"))

    def __init__(self, system):
        super().__init__('CPU Frequency', system)
        self.device_syspath = sysfs_path("devices/system/cpu")

    def read_cpu(self, cpu):
        path = os.path.join(self.device_syspath, 'cpu%s/cpufreq' % cpu)

        scaling_min_freq = self.read_first_line(os.path.join(path, "scaling_min_freq"))
        scaling_max_freq = self.read_first_line(os.path.join(path, "scaling_max_freq"))
        if not scaling_min_freq or not scaling_max_freq:
            self.warning("Unable to read scaling_min_freq "
                         "or scaling_max_freq of CPU %s" % cpu)
            return

        min_mhz = int(scaling_min_freq) // 1000
        max_mhz = int(scaling_max_freq) // 1000
        if min_mhz != max_mhz:
            freq = ('min=%s MHz, max=%s MHz'
                    % (min_mhz, max_mhz))
        else:
            freq = 'min=max=%s MHz' % max_mhz
        return freq

    def show(self):
        cpus = {}
        for cpu in range(self.system.logical_cpu_count):
            freq = self.read_cpu(cpu)
            if freq is not None:
                cpus[cpu] = freq

        infos = format_cpu_infos(cpus)
        if not infos:
            return
        self.log_state('; '.join(infos))

    def read_freq(self, filename):
        try:
            with open(filename, "rb") as fp:
                return fp.readline()
        except OSError as exc:
            self.check_permission_error(exc)
            return None

    def write_freq(self, filename, new_freq):
        with open(filename, "rb") as fp:
            freq = fp.readline()

        if new_freq == freq:
            return False

        with open(filename, "wb") as fp:
            fp.write(new_freq)
        return True

    def write_cpu(self, cpu, tune):
        cpu_path = os.path.join(self.device_syspath, 'cpu%s/cpufreq' % cpu)

        name = "cpuinfo_max_freq" if tune else "cpuinfo_min_freq"
        freq = self.read_freq(os.path.join(cpu_path, name))
        if not freq:
            self.warning("Unable to read %s of CPU %s" % (name, cpu))
            return False

        filename = os.path.join(cpu_path, "scaling_min_freq")
        try:
            return self.write_freq(filename, freq)
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Unable to write scaling_max_freq of CPU %s: %s"
                       % (cpu, exc))

    def write(self, tune):
        modified = []
        for cpu in self.system.cpus:
            if self.write_cpu(cpu, tune):
                modified.append(cpu)
            if self.permission_error:
                break

        if modified:
            cpus = format_cpu_list(modified)
            if tune:
                action = "set to the maximum frequency"
            else:
                action = "reset to the minimum frequency"
            self.log_action("Minimum frequency of CPU %s %s" % (cpus, action))


class IRQAffinity(Operation):
    # /proc/irq/N/smp_affinity existed prior to 2.6.12-rc2 (2005)
    # which is first commit of the Linux git repository

    irq_path = proc_path('irq')

    @classmethod
    def available(cls):
        return os.path.exists(cls.irq_path)

    def __init__(self, system):
        super().__init__('IRQ affinity', system)
        self.irq_affinity_path = os.path.join(self.irq_path, "%s/smp_affinity")
        self.default_affinity_path = os.path.join(self.irq_path, 'default_smp_affinity')

        self.systemctl = True
        self.irqs = None

    def read_irqbalance_systemctl(self):
        cmd = ('systemctl', 'status', 'irqbalance')
        exitcode, stdout = get_output(cmd)
        if not stdout:
            # systemctl is not installed? ignore errors
            self.systemctl = False
            return

        match = re.search(r"^ *Loaded: (.*)$", stdout, flags=re.MULTILINE)
        if not match:
            self.error("Failed to parse systemctl loaded state: %r" % stdout)
            return
        self.systemctl = True

        loaded = match.group(1)
        if loaded.startswith('not-found'):
            # irqbalance service is not installed: do nothing
            return

        match = re.search(r"^ *Active: ([^ ]+)", stdout, flags=re.MULTILINE)
        if not match:
            self.error("Failed to parse systemctl active state: %r" % stdout)
            return

        active = match.group(1)
        if active in ('active', 'activating'):
            return True
        elif active in ('inactive', 'deactivating', 'dead'):
            return False
        else:
            self.error("Unknown service state: %r" % active)

    def read_irqbalance_service(self):
        cmd = ('service', 'irqbalance', 'status')
        exitcode, stdout = get_output(cmd)
        if not stdout:
            # failed to the the status: ignore
            return

        stdout = stdout.rstrip()
        state = stdout.split(' ', 1)[-1]
        if state.startswith('stop'):
            return False
        elif state.startswith('start'):
            return True
        else:
            self.error("Unknown service state: %r" % stdout)

    def read_irqbalance_state(self):
        active = self.read_irqbalance_systemctl()
        if self.systemctl is False:
            active = self.read_irqbalance_service()
        return active

    def parse_affinity(self, mask):
        mask = parse_cpu_mask(mask)
        cpus = []
        for cpu in range(self.system.logical_cpu_count):
            cpu_mask = 1 << cpu
            if cpu_mask & mask:
                cpus.append(cpu)
        return cpus

    def read_default_affinity(self):
        mask = self.read_first_line(self.default_affinity_path)
        if not mask:
            return

        return self.parse_affinity(mask)

    def get_irqs(self):
        if self.irqs is None:
            filenames = os.listdir(self.irq_path)
            self.irqs = [int(name) for name in filenames if name.isdigit()]
            self.irqs.sort()
        return self.irqs

    def read_irq_affinity(self, irq):
        path = self.irq_affinity_path % irq
        mask = self.read_first_line(path)
        if not mask:
            self.error("Failed to read %s" % path)
            return

        return self.parse_affinity(mask)

    def read_irqs_affinity(self):
        affinity = {}
        for irq in self.get_irqs():
            if self.permission_error:
                break
            cpus = self.read_irq_affinity(irq)
            if cpus is not None:
                affinity[irq] = cpus
        return affinity

    def show(self):
        irqbalance_active = self.read_irqbalance_state()
        if irqbalance_active is not None:
            state = 'active' if irqbalance_active else 'inactive'
            self.log_state("irqbalance service: %s" % state)

        default_smp_affinity = self.read_default_affinity()
        if default_smp_affinity:
            self.log_state("Default IRQ affinity: CPU %s"
                           % format_cpu_list(default_smp_affinity))

        irq_affinity = self.read_irqs_affinity()
        if irq_affinity:
            infos = {irq: 'CPU %s' % format_cpu_list(cpus)
                     for irq, cpus in irq_affinity.items()}
            infos = format_cpu_infos(infos)
            infos = ['IRQ %s' % info for info in infos]
            self.log_state('IRQ affinity: %s' % '; '.join(infos))

    def write_irqbalance_service(self, enable):
        irqbalance_active = self.read_irqbalance_state()
        if irqbalance_active is None:
            # systemd service missing or failed to get its state:
            # don't try to start/stop the irqbalance service
            return

        if irqbalance_active == enable:
            # service is already in the expected state: nothing to do
            return

        action = 'start' if enable else 'stop'
        if self.systemctl is False:
            cmd = ('service', 'irqbalance', action)
        else:
            cmd = ('systemctl', action, 'irqbalance')
        exitcode = run_cmd(cmd)
        if exitcode:
            self.error('Failed to %s irqbalance service: '
                       '%s failed with exit code %s'
                       % (action, ' '.join(cmd), exitcode))
            return

        action = 'Start' if enable else 'Stop'
        self.log_action("%s irqbalance service" % action)

    def write_default(self, new_affinity):
        default_smp_affinity = self.read_default_affinity()
        if new_affinity == default_smp_affinity:
            return

        mask = format_cpus_as_mask(new_affinity)
        try:
            write_text(self.default_affinity_path, mask)
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Failed to write %r into %s: %s"
                       % (mask, self.default_affinity_path, exc))
        else:
            self.log_action("Set default affinity to CPU %s"
                            % format_cpu_list(new_affinity))

    def write_irq(self, irq, cpus):
        path = self.irq_affinity_path % irq
        mask = format_cpus_as_mask(cpus)
        try:
            write_text(path, mask)
            return True
        except OSError as exc:
            self.check_permission_error(exc)
            # EIO means that the IRQ doesn't support SMP affinity:
            # ignore the error
            if exc.errno != errno.EIO:
                self.error("Failed to write %r into %s: %s"
                           % (mask, path, exc))
            return False

    def write_irqs(self, new_cpus):
        affinity = self.read_irqs_affinity()
        modified = []
        for irq in self.get_irqs():
            if self.permission_error:
                break

            cpus = affinity.get(irq)
            if new_cpus == cpus:
                continue

            if self.write_irq(irq, new_cpus):
                modified.append(irq)

        if modified:
            self.log_action("Set affinity of IRQ %s to CPU %s"
                            % (format_cpu_list(modified), format_cpu_list(new_cpus)))

    def write(self, tune):
        cpus = range(self.system.logical_cpu_count)
        if tune:
            excluded = set(self.system.cpus)
            # Only compute the subset if excluded is not the full list of cpus
            if excluded != set(cpus):
                cpus = (cpu for cpu in cpus if cpu not in excluded)
        cpus = list(cpus)

        self.write_irqbalance_service(not tune)
        self.write_default(cpus)
        self.write_irqs(cpus)


class CheckNOHZFullIntelPstate(IntelPstateOperation):

    def __init__(self, system):
        super().__init__('Check nohz_full', system)

    def show(self):
        nohz_full = self.read_first_line(sysfs_path('devices/system/cpu/nohz_full'))
        if not nohz_full:
            return

        nohz_full = parse_cpu_list(nohz_full)
        if not nohz_full:
            return

        used = set(self.system.cpus) | set(nohz_full)
        if not used:
            return

        self.advice("WARNING: nohz_full is enabled on CPUs %s which use the "
                    "intel_pstate driver, whereas intel_pstate is incompatible "
                    "with nohz_full"
                    % format_cpu_list(used))
        self.advice("See https://bugzilla.redhat.com/show_bug.cgi?id=1378529")
        self.tuned_for_benchmarks = False


class PowerSupply(Operation):
    path = sysfs_path('class/power_supply')

    @classmethod
    def available(cls):
        return os.path.exists(cls.path)

    def __init__(self, system):
        super().__init__('Power supply', system)

    def read_power_supply(self):
        # Python implementation of the on_ac_power shell script
        for name in os.listdir(self.path):
            # Ignore "USB" and "Battery" types
            filename = os.path.join(self.path, name, 'type')
            sys_type = self.read_first_line(filename)
            if sys_type.strip() != "Mains":
                continue

            filename = os.path.join(self.path, name, 'online')
            if not os.path.exists(filename):
                continue

            line = self.read_first_line(filename)
            if line == '1':
                return True
            if line == '0':
                return False
            self.error("Failed to parse %s: %r" % (filename, line))
            break

        return None

    def show(self):
        plugged = self.read_power_supply()
        if plugged is None:
            return

        state = 'plugged' if plugged else 'unplugged'
        self.log_state('the power cable is %s' % state)
        if not plugged:
            self.advice('The power cable must be plugged')


class PerfEvent(Operation):
    # Minimise time spent by the Linux perf kernel profiler.

    BENCHMARK_RATE = 1
    path = proc_path("sys/kernel/perf_event_max_sample_rate")

    @classmethod
    def available(cls):
        return os.path.exists(cls.path)

    def __init__(self, system):
        super().__init__('Perf event', system)

    def read_max_sample_rate(self):
        line = self.read_first_line(self.path)
        if not line:
            return None
        return int(line)

    def show(self):
        max_sample_rate = self.read_max_sample_rate()
        if not max_sample_rate:
            return

        self.log_state("Maximum sample rate: %s per second" % max_sample_rate)
        self.tuned_for_benchmarks = (max_sample_rate == self.BENCHMARK_RATE)
        if not self.tuned_for_benchmarks:
            self.advice("Set max sample rate to %s" % self.BENCHMARK_RATE)

    def write(self, tune):
        if tune:
            new_rate = self.BENCHMARK_RATE
        else:
            new_rate = 100000

        max_sample_rate = self.read_max_sample_rate()
        if max_sample_rate == new_rate:
            return

        try:
            write_text(self.path, str(new_rate))
        except OSError as exc:
            self.check_permission_error(exc)
            self.error("Failed to write into %s: %s" % (self.path, exc))
        else:
            self.log_action("Max sample rate set to %s per second" % new_rate)


OPERATIONS = [
    # Generic operations
    PerfEvent,
    ASLR,
    LinuxScheduler,
    CPUFrequency,
    IRQAffinity,
    PowerSupply,

    # Setting the CPU scaling governor resets no_turbo
    # and so must be set before Turbo Boost
    CPUGovernor,

    # Intel Pstate Operations
    TurboBoostIntelPstate,
    CheckNOHZFullIntelPstate,

    # X86 Operations
    TurboBoostMSR,
]


class System:
    def __init__(self):
        self.operations = []
        self.actions = []
        self.states = []
        self.advices = []
        self.warnings = []
        self.errors = []

        self.tuned = True

        self.logical_cpu_count = None
        # CPUs used for benchmarking: tuple of CPU identifiers
        self.cpus = None

        self.operations = []
        for operation_class in OPERATIONS:
            if not operation_class.available():
                continue
            operation = operation_class(self)
            self.operations.append(operation)

    def advice(self, msg):
        self.advices.append(msg)

    def log_state(self, msg):
        self.states.append(msg)

    def log_action(self, msg):
        self.actions.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def error(self, msg):
        self.errors.append(msg)

    def write_messages(self, title, messages):
        if not messages:
            return

        print()
        display_title(title)
        for msg in messages:
            print(msg)

    def run_operations(self, action):
        if action == 'tune':
            print("Tune the system configuration to run benchmarks")
        elif action == 'reset':
            print("Reset system configuration")
        else:
            print("Show the system configuration")

        if action in ('tune', 'reset'):
            tune = (action == 'tune')

            for operation in self.operations:
                operation.write(tune)

        for operation in self.operations:
            operation.show()

        if any(operation.permission_error for operation in self.operations):
            msg = "ERROR: At least one operation failed with permission error"
            if not is_root():
                msg += ", retry as root"
            if action == 'show':
                self.warning(msg)
            else:
                self.error(msg)

    def init(self, args):
        if not self.operations:
            print("WARNING: no operation available for your platform")
            sys.exit()

        self.logical_cpu_count = get_logical_cpu_count()
        if not self.logical_cpu_count:
            print("ERROR: failed to get the number of logical CPUs")
            sys.exit(1)

        isolated = get_isolated_cpus()
        if isolated:
            self.cpus = tuple(isolated)
        elif args.affinity:
            self.cpus = tuple(args.affinity)
        else:
            self.cpus = tuple(range(self.logical_cpu_count))
        # The list of cpus must be sorted to avoid useless write in operations
        assert sorted(self.cpus) == list(self.cpus)

        self.log_state("CPU: use %s logical CPUs: %s"
                       % (len(self.cpus), format_cpu_list(self.cpus)))

    def render_messages(self, action):
        self.write_messages("Actions", self.actions)
        self.write_messages("System state", self.states)
        # Advices are for tuning: hide them for reset
        if action != 'reset':
            self.write_messages("Advices", self.advices)
        self.write_messages("Warnings", self.warnings)
        self.write_messages("Errors", self.errors)

        if action == 'show':
            self.tuned = all(operation.tuned_for_benchmarks in (True, None)
                             for operation in self.operations)
            print()
            if self.tuned and not self.errors:
                print("OK! System ready for benchmarking")
            else:
                print('Run "%s -m pyperf system tune" to tune the system '
                      'configuration to run benchmarks'
                      % os.path.basename(sys.executable))

    def main(self, action, args):
        self.init(args)
        self.run_operations(action)
        self.render_messages(action)
        if self.errors:
            sys.exit(1)
        if not self.tuned:
            sys.exit(2)
