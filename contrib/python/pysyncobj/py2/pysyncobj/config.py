
class FAIL_REASON:
    SUCCESS = 0             #: Command successfully applied.
    QUEUE_FULL = 1          #: Commands queue full
    MISSING_LEADER = 2      #: Leader is currently missing (leader election in progress, or no connection)
    DISCARDED = 3           #: Command discarded (cause of new leader elected and another command was applied instead)
    NOT_LEADER = 4          #: Leader has changed, old leader did not have time to commit command.
    LEADER_CHANGED = 5      #: Simmilar to NOT_LEADER - leader has changed without command commit.
    REQUEST_DENIED = 6      #: Command denied

class SERIALIZER_STATE:
    NOT_SERIALIZING = 0     #: Serialization not started or already finished.
    SERIALIZING = 1         #: Serialization in progress.
    SUCCESS = 2             #: Serialization successfully finished (should be returned only one time after finished).
    FAILED = 3              #: Serialization failed (should be returned only one time after finished).

class SyncObjConf(object):
    """PySyncObj configuration object"""

    def __init__(self, **kwargs):

        #: Encrypt session with specified password.
        #: Install `cryptography` module to be able to set password.
        self.password = kwargs.get('password', None)

        #: Disable autoTick if you want to call onTick manually.
        #: Otherwise it will be called automatically from separate thread.
        self.autoTick = kwargs.get('autoTick', True)
        self.autoTickPeriod = kwargs.get('autoTickPeriod', 0.05)

        #: Commands queue is used to store commands before real processing.
        self.commandsQueueSize = kwargs.get('commandsQueueSize', 100000)

        #: After randomly selected timeout (in range from minTimeout to maxTimeout)
        #: leader considered dead, and leader election starts.
        self.raftMinTimeout = kwargs.get('raftMinTimeout', 0.4)

        #: Same as raftMinTimeout
        self.raftMaxTimeout = kwargs.get('raftMaxTimeout', 1.4)

        #: Interval of sending append_entries (ping) command.
        #: Should be less than raftMinTimeout.
        self.appendEntriesPeriod = kwargs.get('appendEntriesPeriod', 0.1)

        #: When no data received for connectionTimeout - connection considered dead.
        #: Should be more than raftMaxTimeout.
        self.connectionTimeout = kwargs.get('connectionTimeout', 3.5)

        #: Interval between connection attempts.
        #: Will try to connect to offline nodes each connectionRetryTime.
        self.connectionRetryTime = kwargs.get('connectionRetryTime', 5.0)

        #: When leader has no response from the majority of the cluster
        #: for leaderFallbackTimeout - it will fallback to follower state.
        #: Should be more than appendEntriesPeriod.
        self.leaderFallbackTimeout = kwargs.get('leaderFallbackTimeout', 30.0)

        #: Send multiple entries in a single command.
        #: Enabled (default) - improve overall performance (requests per second)
        #: Disabled - improve single request speed (don't wait till batch ready)
        self.appendEntriesUseBatch = kwargs.get('appendEntriesUseBatch', True)

        #: Max number of bytes per single append_entries command.
        self.appendEntriesBatchSizeBytes = kwargs.get('appendEntriesBatchSizeBytes', 2 ** 16)

        #: Bind address (address:port). Default - None.
        #: If None - selfAddress is used as bindAddress.
        #: Could be useful if selfAddress is not equal to bindAddress.
        #: Eg. with routers, nat, port forwarding, etc.
        self.bindAddress = kwargs.get('bindAddress', None)

        #: Preferred address type. Default - ipv4.
        #: None - no preferences, select random available.
        #: ipv4 - prefer ipv4 address type, if not available us ipv6.
        #: ipv6 - prefer ipv6 address type, if not available us ipv4.
        self.preferredAddrType = kwargs.get('preferredAddrType', 'ipv4')

        #: Size of send buffer for sockets.
        self.sendBufferSize = kwargs.get('sendBufferSize', 2 ** 16)

        #: Size of receive for sockets.
        self.recvBufferSize = kwargs.get('recvBufferSize', 2 ** 16)

        #: Time to cache dns requests (improves performance,
        #: no need to resolve address for each connection attempt).
        self.dnsCacheTime = kwargs.get('dnsCacheTime', 600.0)

        #: Time to cache failed dns request.
        self.dnsFailCacheTime = kwargs.get('dnsFailCacheTime', 30.0)

        #: Log will be compacted after it reach minEntries size or
        #: minTime after previous compaction.
        self.logCompactionMinEntries = kwargs.get('logCompactionMinEntries', 5000)

        #: Log will be compacted after it reach minEntries size or
        #: minTime after previous compaction.
        self.logCompactionMinTime = kwargs.get('logCompactionMinTime', 300)

        #: If true - each node will start log compaction in separate time window.
        #: eg. node1 in 12.00-12.10, node2 in 12.10-12.20, node3 12.20 - 12.30,
        #: then again node1 12.30-12.40, node2 12.40-12.50, etc.
        self.logCompactionSplit = kwargs.get('logCompactionSplit', False)

        #: Max number of bytes per single append_entries command
        #: while sending serialized object.
        self.logCompactionBatchSize = kwargs.get('logCompactionBatchSize', 2 ** 16)

        #: If true - commands will be enqueued and executed after leader detected.
        #: Otherwise - `FAIL_REASON.MISSING_LEADER <#pysyncobj.FAIL_REASON.MISSING_LEADER>`_ error will be emitted.
        #: Leader is missing when esteblishing connection or when election in progress.
        self.commandsWaitLeader = kwargs.get('commandsWaitLeader', True)

        #: File to store full serialized object. Save full dump on disc when doing log compaction.
        #: None - to disable store.
        self.fullDumpFile = kwargs.get('fullDumpFile', None)

        #: File to store operations journal. Save each record as soon as received.
        self.journalFile = kwargs.get('journalFile', None)

        #: Will try to bind port every bindRetryTime seconds until success.
        self.bindRetryTime = kwargs.get('bindRetryTime', 1.0)

        #: Max number of attempts to bind port (default 0, unlimited).
        self.maxBindRetries = kwargs.get('maxBindRetries', 0)

        #: This callback will be called as soon as SyncObj sync all data from leader.
        self.onReady = kwargs.get('onReady', None)

        #: This callback will be called for every change of SyncObj state.
        #: Arguments: onStateChanged(oldState, newState).
        #: WARNING: there could be multiple leaders at the same time!
        self.onStateChanged = kwargs.get('onStateChanged', None)

        #: If enabled - cluster configuration could be changed dynamically.
        self.dynamicMembershipChange = kwargs.get('dynamicMembershipChange', False)

        #: Sockets poller:
        #:  * `auto` - auto select best available on current platform
        #:  * `select` - use select poller
        #:  * `poll` - use poll poller
        self.pollerType = kwargs.get('pollerType', 'auto')

        #: Use fork if available when serializing on disk.
        self.useFork = kwargs.get('useFork', True)

        #: Custom serialize function, it will be called when logCompaction (fullDump) happens.
        #: If specified - there should be a custom deserializer too.
        #: Arguments: serializer(fileName, data)
        #: data - some internal stuff that is *required* to be serialized with your object data.
        self.serializer = kwargs.get('serializer', None)

        #: Check custom serialization state, for async serializer.
        #: Should return one of `SERIALIZER_STATE <#pysyncobj.SERIALIZER_STATE>`_.
        self.serializeChecker = kwargs.get('serializeChecker', None)

        #: Custom deserialize function, it will be called when restore from fullDump.
        #: If specified - there should be a custom serializer too.
        #: Should return data - internal stuff that was passed to serialize.
        self.deserializer = kwargs.get('deserializer', None)

        #: This callback will be called when cluster is switched to new version.
        #: onCodeVersionChanged(oldVer, newVer)
        self.onCodeVersionChanged = kwargs.get('onCodeVersionChanged', None)

        #: TCP socket keepalive
        #: (keepalive_time_seconds, probe_intervals_seconds, max_fails_count)
        #: Set to None to disable
        self.tcp_keepalive = kwargs.get('tcp_keepalive', (16, 3, 5))

    def validate(self):
        assert self.autoTickPeriod > 0
        assert self.commandsQueueSize >= 0
        assert self.raftMinTimeout > self.appendEntriesPeriod * 3
        assert self.raftMaxTimeout > self.raftMinTimeout
        assert self.appendEntriesPeriod > 0
        assert self.leaderFallbackTimeout > self.appendEntriesPeriod
        assert self.connectionTimeout >= self.raftMaxTimeout
        assert self.connectionRetryTime >= 0
        assert self.appendEntriesBatchSizeBytes > 0
        assert self.sendBufferSize > 0
        assert self.recvBufferSize > 0
        assert self.dnsCacheTime>= 0
        assert self.dnsFailCacheTime >= 0
        assert self.logCompactionMinEntries >= 2
        assert self.logCompactionMinTime > 0
        assert self.logCompactionBatchSize > 0
        assert self.bindRetryTime > 0
        assert (self.deserializer is None) == (self.serializer is None)
        if self.serializer is not None:
            assert self.fullDumpFile is not None
        assert self.preferredAddrType in ('ipv4', 'ipv6', None)
        if self.tcp_keepalive is not None:
            assert isinstance(self.tcp_keepalive, tuple)
            assert len(self.tcp_keepalive) == 3
            for i in range(3):
                assert isinstance(self.tcp_keepalive[i], int)
                assert self.tcp_keepalive[i] > 0
