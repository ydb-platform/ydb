"""Parser function used to convert Atop structs in 1.26 into "parseable" output based on the signature name."""

from atoparser import utils
from atoparser.structs import atop_1_26

# Disable the following pylint warnings to allow functions to match a consistent type across all parseables.
# This helps simplify calls allow dynamic function lookups to have consistent input arguments.
# pylint: disable=unused-argument,invalid-name


def parse_cpu(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'cpu' parseable representing per core usage."""
    for index, cpu in enumerate(sstat.cpu.cpu):
        if index >= sstat.cpu.nrcpu:
            # Core list contains 100 entries, but only up the count specified in the cpu stat are valid.
            break
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "ticks": header.hertz,
            "proc": index,
            "system": cpu.stime,
            "user": cpu.utime,
            "niced": cpu.ntime,
            "idle": cpu.itime,
            "wait": cpu.wtime,
            "irq": cpu.Itime,
            "softirq": cpu.Stime,
            "steal": cpu.steal,
            "guest": cpu.guest,
        }
        yield values


def parse_CPL(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'CPL' parseable representing system load."""
    values = {
        "timestamp": record.curtime,
        "interval": record.interval,
        "procs": sstat.cpu.nrcpu,
        "load_1": sstat.cpu.lavg1,
        "load_5": sstat.cpu.lavg5,
        "load_15": sstat.cpu.lavg15,
        "context_switches": sstat.cpu.csw,
        "interrupts": sstat.cpu.devint,
    }
    yield values


def parse_CPU(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Statistics for Atop 'CPU' parseable representing usage across cores combined."""
    values = {
        "timestamp": record.curtime,
        "interval": record.interval,
        "ticks": header.hertz,
        "procs": sstat.cpu.nrcpu,
        "system": sstat.cpu.all.stime,
        "user": sstat.cpu.all.utime,
        "niced": sstat.cpu.all.ntime,
        "idle": sstat.cpu.all.itime,
        "wait": sstat.cpu.all.wtime,
        "irq": sstat.cpu.all.Itime,
        "softirq": sstat.cpu.all.Stime,
        "steal": sstat.cpu.all.steal,
        "guest": sstat.cpu.all.guest,
    }
    yield values


def parse_DSK(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'DSK' parseable representing disk/drive usage."""
    for disk in sstat.dsk.dsk:
        if not disk.name:
            # Disk list contains 256 entries, but only up the first empty name are valid.
            break
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "name": disk.name.decode(),
            "io_ms": disk.io_ms,
            "reads": disk.nread,
            "read_sectors": disk.nrsect,
            "writes": disk.nwrite,
            "write_sectors": disk.nwsect,
        }
        yield values


def parse_LVM(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'LVM' parseable representing logical volume usage."""
    for lvm in sstat.dsk.lvm:
        if not lvm.name:
            # LVM list contains 256 entries, but only up the first empty name are valid.
            break
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "name": lvm.name.decode(),
            "io_ms": lvm.io_ms,
            "reads": lvm.nread,
            "read_sectors": lvm.nrsect,
            "writes": lvm.nwrite,
            "write_sectors": lvm.nwsect,
        }
        yield values


def parse_MDD(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'MDD' parseable representing multiple device drive usage."""
    for mdd in sstat.dsk.mdd:
        if not mdd.name:
            # MDD list contains 256 entries, but only up the first empty name are valid.
            break
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "name": mdd.name.decode(),
            "io_ms": mdd.io_ms,
            "reads": mdd.nread,
            "read_sectors": mdd.nrsect,
            "writes": mdd.nwrite,
            "write_sectors": mdd.nwsect,
        }
        yield values


def parse_MEM(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'MEM' parseable representing memory usage."""
    values = {
        "timestamp": record.curtime,
        "interval": record.interval,
        "page_size": header.pagesize,
        "phys_mem": sstat.mem.physmem,
        "free_mem": sstat.mem.freemem,
        "page_cache": sstat.mem.cachemem,
        "buffer_cache": sstat.mem.buffermem,
        "slab": sstat.mem.slabmem,
        "dirty_pages": sstat.mem.cachedrt,
    }
    yield values


def parse_NETL(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'NET' parseable representing network usage on lower interfaces."""
    for interface in sstat.intf.intf:
        if not interface.name:
            # Interface list contains 32 entries, but only up the first empty name are valid.
            break
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "name": interface.name.decode(),
            "pkt_received": interface.rpack,
            "byte_received": interface.rbyte,
            "pkt_transmitted": interface.spack,
            "bytes_transmitted": interface.sbyte,
            "speed": interface.speed,
            "duplex": int.from_bytes(interface.duplex, byteorder="big"),
        }
        yield values


def parse_NETU(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'NET' parseable representing network usage on upper interfaces."""
    values = {
        "timestamp": record.curtime,
        "interval": record.interval,
        "name": "upper",
        "tcp_pkt_received": sstat.net.tcp.InSegs,
        "tcp_pkt_transmitted": sstat.net.tcp.OutSegs,
        "udp_pkt_received": sstat.net.udpv4.InDatagrams + sstat.net.udpv6.Udp6InDatagrams,
        "udp_pkt_transmitted": sstat.net.udpv4.OutDatagrams + sstat.net.udpv6.Udp6OutDatagrams,
        "ip_pkt_received": sstat.net.ipv4.InReceives + sstat.net.ipv6.Ip6InReceives,
        "ip_pkt_transmitted": sstat.net.ipv4.OutRequests + sstat.net.ipv6.Ip6OutRequests,
        "ip_pkt_delivered": sstat.net.ipv4.InDelivers + sstat.net.ipv6.Ip6InDelivers,
        "ip_pkt_forwarded": sstat.net.ipv4.ForwDatagrams + sstat.net.ipv6.Ip6OutForwDatagrams,
    }
    yield values


def parse_PAG(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'PAG' parseable representing paging space usage."""
    values = {
        "timestamp": record.curtime,
        "interval": record.interval,
        "page_size": header.pagesize,
        "page_scans": sstat.mem.pgscans,
        "alloc_stalls": sstat.mem.allocstall,
        "swapins": sstat.mem.swins,
        "swapouts": sstat.mem.swouts,
    }
    yield values


def parse_PRC(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'PRC' parseable representing process cpu usage."""
    for stat in tstats:
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "pid": stat.gen.pid,
            "name": stat.gen.name.decode(),
            "state": stat.gen.state.decode(),
            "ticks": header.hertz,
            "user_consumption": stat.cpu.utime,
            "system_consumption": stat.cpu.stime,
            "nice": stat.cpu.nice,
            "priority": stat.cpu.prio,
            "priority_realtime": stat.cpu.rtprio,
            "policy": stat.cpu.policy,
            "cpu": stat.cpu.curcpu,
            "sleep": stat.cpu.sleepavg,
        }
        yield values


def parse_PRD(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'PRD' parseable representing process drive usage."""
    for stat in tstats:
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "pid": stat.gen.pid,
            "name": stat.gen.name.decode(),
            "state": stat.gen.state.decode(),
            "kernel_patch": "y" if header.supportflags & atop_1_26.PATCHSTAT else "n",
            "standard_io": "y" if header.supportflags & atop_1_26.IOSTAT else "n",
            "reads": stat.dsk.rio,
            "read_sectors": stat.dsk.rsz,
            "writes": stat.dsk.wio,
            "written_sectors": stat.dsk.wsz,
            "cancelled_sector_writes": stat.dsk.cwsz,
        }
        yield values


def parse_PRG(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'PRG' parseable representing process generic details."""
    for stat in tstats:
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "pid": stat.gen.pid,
            "name": stat.gen.name.decode(),
            "state": stat.gen.state.decode(),
            "real_uid": stat.gen.ruid,
            "real_gid": stat.gen.rgid,
            "tgid": stat.gen.pid,  # This is a duplicate of pid per atop documentation.
            "threads": stat.gen.nthr,
            "exit_code": stat.gen.excode,
            "start_time": stat.gen.btime,
            "cmd": stat.gen.cmdline.decode(),
            "ppid": stat.gen.ppid,
            "running_threads": stat.gen.nthrrun,
            "sleeping_threads": stat.gen.nthrslpi,
            "dead_threads": stat.gen.nthrslpu,
            "effective_uid": stat.gen.euid,
            "effective_gid": stat.gen.egid,
            "saved_uid": stat.gen.suid,
            "saved_gid": stat.gen.sgid,
            "filesystem_uid": stat.gen.fsuid,
            "filesystem_gid": stat.gen.fsgid,
            "elapsed_time": stat.gen.elaps,
        }
        yield values


def parse_PRM(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'PRM' parseable representing process memory usage."""
    for stat in tstats:
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "pid": stat.gen.pid,
            "name": stat.gen.name.decode(),
            "state": stat.gen.state.decode(),
            "page": header.pagesize,
            "vsize": stat.mem.vmem * 1024,
            "rsize": stat.mem.rmem * 1024,
            "ssize": stat.mem.shtext * 1024,
            "vgrowth": stat.mem.vgrow * 1024,
            "rgrowth": stat.mem.rgrow * 1024,
            "minor_faults": stat.mem.minflt,
            "major_faults": stat.mem.majflt,
        }
        yield values


def parse_PRN(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'PRN' parseable representing process network activity."""
    for stat in tstats:
        values = {
            "timestamp": record.curtime,
            "interval": record.interval,
            "pid": stat.gen.pid,
            "name": stat.gen.name.decode(),
            "state": stat.gen.state.decode(),
            "kernel_patch": "y" if header.supportflags & atop_1_26.PATCHSTAT else "n",
            "tcp_transmitted": stat.net.tcpsnd,
            "tcp_transmitted_size": stat.net.tcpssz,
            "tcp_received": stat.net.tcprcv,
            "tcp_received_size": stat.net.tcprsz,
            "udp_transmitted": stat.net.udpsnd,
            "udp_transmitted_size": stat.net.udpssz,
            "udp_received": stat.net.udprcv,
            "udp_received_size": stat.net.udprsz,
            "raw_transmitted": stat.net.rawsnd,
            "raw_received": stat.net.rawrcv,
        }
        yield values


def parse_SWP(
    header: utils.Header,
    record: utils.Record,
    sstat: utils.SStat,
    tstats: list[utils.TStat],
) -> dict:
    """Retrieves statistics for Atop 'SWP' parseable representing swap space usage."""
    values = {
        "timestamp": record.curtime,
        "interval": record.interval,
        "page_size": header.pagesize,
        "swap": sstat.mem.totswap,
        "free_swap": sstat.mem.freeswap,
        "committed_space": sstat.mem.committed,
        "committed_limit": sstat.mem.commitlim,
    }
    yield values
