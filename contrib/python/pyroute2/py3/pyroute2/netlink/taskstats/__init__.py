'''
TaskStats module
================

All that you should know about TaskStats, is that you should not
use it. But if you have to, ok::

    import os
    from pyroute2 import TaskStats
    ts = TaskStats()
    ts.get_pid_stat(os.getpid())

It is not implemented normally yet, but some methods are already
usable.
'''

import struct

from pyroute2.netlink import NLM_F_REQUEST, genlmsg, nla, nla_struct
from pyroute2.netlink.generic import (
    AsyncGenericNetlinkSocket,
    GenericNetlinkSocket,
)

TASKSTATS_CMD_UNSPEC = 0  # Reserved
TASKSTATS_CMD_GET = 1  # user->kernel request/get-response
TASKSTATS_CMD_NEW = 2


class tcmd(genlmsg):
    nla_map = (
        ('TASKSTATS_CMD_ATTR_UNSPEC', 'none'),
        ('TASKSTATS_CMD_ATTR_PID', 'uint32'),
        ('TASKSTATS_CMD_ATTR_TGID', 'uint32'),
        ('TASKSTATS_CMD_ATTR_REGISTER_CPUMASK', 'asciiz'),
        ('TASKSTATS_CMD_ATTR_DEREGISTER_CPUMASK', 'asciiz'),
    )


class TStatsDecoder:
    def decode(self):
        nla_struct.decode(self)
        command = self['ac_comm']
        if isinstance(command, bytes):
            command = command.decode('utf-8')
        self['ac_comm'] = command[: command.find('\0')]


class TStatsBase:
    class tstats_vB(nla_struct, TStatsDecoder):
        fields = (
            ('version', 'H'),  # 2
            ('ac_exitcode', 'I'),  # 4
            ('ac_flag', 'B'),  # 1
            ('ac_nice', 'B'),  # 1 --- 10
            ('cpu_count', 'Q'),  # 8
            ('cpu_delay_total', 'Q'),  # 8
            ('blkio_count', 'Q'),  # 8
            ('blkio_delay_total', 'Q'),  # 8
            ('swapin_count', 'Q'),  # 8
            ('swapin_delay_total', 'Q'),  # 8
            ('cpu_run_real_total', 'Q'),  # 8
            ('cpu_run_virtual_total', 'Q'),  # 8
            ('ac_comm', '32s'),  # 32 +++ 112
            ('ac_sched', 'B'),  # 1
            ('__ac_pad', '3x'),  # 3
            # (the ac_uid field is aligned(8), so we add more padding)
            ('__implicit_pad', '4x'),  # 4
            ('ac_uid', 'I'),  # 4  +++ 120
            ('ac_gid', 'I'),  # 4
            ('ac_pid', 'I'),  # 4
            ('ac_ppid', 'I'),  # 4
            ('ac_btime', 'I'),  # 4  +++ 136
            ('ac_etime', 'Q'),  # 8  +++ 144
            ('ac_utime', 'Q'),  # 8
            ('ac_stime', 'Q'),  # 8
            ('ac_minflt', 'Q'),  # 8
            ('ac_majflt', 'Q'),  # 8
            ('coremem', 'Q'),  # 8
            ('virtmem', 'Q'),  # 8
            ('hiwater_rss', 'Q'),  # 8
            ('hiwater_vm', 'Q'),  # 8
            ('read_char', 'Q'),  # 8
            ('write_char', 'Q'),  # 8
            ('read_syscalls', 'Q'),  # 8
            ('write_syscalls', 'Q'),  # 8
            ('read_bytes', 'Q'),  # ...
            ('write_bytes', 'Q'),
            ('cancelled_write_bytes', 'Q'),
            ('nvcsw', 'Q'),
            ('nivcsw', 'Q'),
            ('ac_utimescaled', 'Q'),
            ('ac_stimescaled', 'Q'),
            ('cpu_scaled_run_real_total', 'Q'),
        )

    class tstats_v15(nla_struct, TStatsDecoder):
        fields = (
            ('version', 'H'),
            ('ac_exitcode', 'I'),
            ('ac_flag', 'B'),
            ('ac_nice', 'B'),
            #
            ('cpu_count', 'Q'),
            ('cpu_delay_total', 'Q'),
            ('cpu_delay_max', 'Q'),
            ('cpu_delay_min', 'Q'),
            #
            ('blkio_count', 'Q'),
            ('blkio_delay_total', 'Q'),
            ('blkio_delay_max', 'Q'),
            ('blkio_delay_min', 'Q'),
            #
            ('swapin_count', 'Q'),
            ('swapin_delay_total', 'Q'),
            ('swapin_delay_max', 'Q'),
            ('swapin_delay_min', 'Q'),
            #
            ('cpu_run_real_total', 'Q'),
            ('cpu_run_virtual_total', 'Q'),
            #
            ('ac_comm', '32s'),
            ('ac_sched', 'B'),
            ('__ac_pad', '3x'),
            # (the ac_uid field is aligned(8), so we add more padding)
            ('__implicit_pad', '4x'),
            ('ac_uid', 'I'),
            ('ac_gid', 'I'),
            ('ac_pid', 'I'),
            ('ac_ppid', 'I'),
            ('ac_btime', 'I'),
            ('ac_etime', 'Q'),
            ('ac_utime', 'Q'),
            ('ac_stime', 'Q'),
            ('ac_minflt', 'Q'),
            ('ac_majflt', 'Q'),
            ('coremem', 'Q'),
            ('virtmem', 'Q'),
            ('hiwater_rss', 'Q'),
            ('hiwater_vm', 'Q'),
            ('read_char', 'Q'),
            ('write_char', 'Q'),
            ('read_syscalls', 'Q'),
            ('write_syscalls', 'Q'),
            #
            ('read_bytes', 'Q'),
            ('write_bytes', 'Q'),
            ('cancelled_write_bytes', 'Q'),
            #
            ('nvcsw', 'Q'),
            ('nivcsw', 'Q'),
            #
            ('ac_utimescaled', 'Q'),
            ('ac_stimescaled', 'Q'),
            ('cpu_scaled_run_real_total', 'Q'),
            #
            ('freepages_count', 'Q'),
            ('freepages_delay_total', 'Q'),
            ('freepages_delay_max', 'Q'),
            ('freepages_delay_min', 'Q'),
            #
            ('thrashing_count', 'Q'),
            ('thrashing_delay_total', 'Q'),
            ('thrashing_delay_max', 'Q'),
            ('thrashing_delay_min', 'Q'),
            #
            ('ac_btime64', 'Q'),
            #
            ('compact_count', 'Q'),
            ('compact_delay_total', 'Q'),
            ('compact_delay_max', 'Q'),
            ('compact_delay_min', 'Q'),
            #
            ('ac_tgid', 'I'),
            #
            ('ac_tgetime', 'Q'),
            #
            ('ac_exe_dev', 'Q'),
            ('ac_exe_inode', 'Q'),
            #
            ('wpcopy_count', 'Q'),
            ('wpcopy_delay_total', 'Q'),
            ('wpcopy_delay_max', 'Q'),
            ('wpcopy_delay_min', 'Q'),
            #
            ('irq_count', 'Q'),
            ('irq_delay_total', 'Q'),
            ('irq_delay_max', 'Q'),
            ('irq_delay_min', 'Q'),
        )

    @staticmethod
    def versioned_stats(self, *argv, **kwarg):
        data = kwarg['data']
        offset = kwarg['offset'] + struct.calcsize('HH')  # + NL header
        end = offset + struct.calcsize('H')  # + uint16, version
        (version,) = struct.unpack('H', data[offset:end])
        if version == 15:
            return self.tstats_v15
        return self.tstats_vB


class taskstatsmsg(genlmsg, TStatsBase):
    nla_map = (
        ('TASKSTATS_TYPE_UNSPEC', 'none'),
        ('TASKSTATS_TYPE_PID', 'uint32'),
        ('TASKSTATS_TYPE_TGID', 'uint32'),
        ('TASKSTATS_TYPE_STATS', 'versioned_stats'),
        ('TASKSTATS_TYPE_AGGR_PID', 'aggr_pid'),
        ('TASKSTATS_TYPE_AGGR_TGID', 'aggr_tgid'),
    )

    class aggr_id(nla, TStatsBase):
        nla_map = (
            ('TASKSTATS_TYPE_UNSPEC', 'none'),
            ('TASKSTATS_TYPE_PID', 'uint32'),
            ('TASKSTATS_TYPE_TGID', 'uint32'),
            ('TASKSTATS_TYPE_STATS', 'versioned_stats'),
        )

    class aggr_pid(aggr_id):
        pass

    class aggr_tgid(aggr_id):
        pass


class AsyncTaskStats(AsyncGenericNetlinkSocket):
    async def bind(self):
        await super().bind('TASKSTATS', taskstatsmsg)

    async def get_pid_stat(self, pid):
        '''
        Get taskstats for a process. Pid should be an integer.
        '''
        msg = tcmd()
        msg['cmd'] = TASKSTATS_CMD_GET
        msg['version'] = 1
        msg['attrs'].append(['TASKSTATS_CMD_ATTR_PID', pid])
        return await self.nlm_request(msg, self.prid, msg_flags=NLM_F_REQUEST)

    async def _register_mask(self, cmd, mask):
        msg = tcmd()
        msg['cmd'] = TASKSTATS_CMD_GET
        msg['version'] = 1
        msg['attrs'].append([cmd, mask])
        # there is no response to this request
        await self.put(msg, self.prid, msg_flags=NLM_F_REQUEST)

    async def register_mask(self, mask):
        '''
        Start the accounting for a processors by a mask. Mask is
        a string, e.g.::
            0,1 -- first two CPUs
            0-4,6-10 -- CPUs from 0 to 4 and from 6 to 10

        Though the kernel has a procedure, that cleans up accounting,
        when it is not used, it is recommended to run deregister_mask()
        before process exit.
        '''
        await self._register_mask('TASKSTATS_CMD_ATTR_REGISTER_CPUMASK', mask)

    async def deregister_mask(self, mask):
        '''
        Stop the accounting.
        '''
        await self._register_mask(
            'TASKSTATS_CMD_ATTR_DEREGISTER_CPUMASK', mask
        )


class TaskStats(GenericNetlinkSocket):

    async_class = AsyncTaskStats

    def bind(self):
        return self._run_with_cleanup(self.asyncore.bind)

    def get_pid_stat(self, pid):
        return self._run_sync_cleanup(self.asyncore.get_pid_stat, pid)

    def register_mask(self, mask):
        return self._run_with_cleanup(self.asyncore.register_mask, mask)

    def unregister_mask(self, mask):
        return self._run_with_cleanup(self.asyncore.unregister_mask, mask)
