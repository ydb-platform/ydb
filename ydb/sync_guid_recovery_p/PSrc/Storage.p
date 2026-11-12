// Durable disk records are co-located in this model store, not in shared P globals.
// Each command/ack is an atomic durable operation. Surveys still use independent
// asynchronous proxies; no implementation code reads monitor/oracle state.
machine TRecoveryStorage {
  var driver: machine;
  var transport: machine;
  var topology: ETopology;
  var diskCount: int;
  var disks: seq[TVDisk];
  var roundOwner: int;
  var roundPeers: set[int];
  var roundDone: set[int];

  start state Serving {
    entry (config: (Driver: machine, Transport: machine, Fresh: bool, Topology: ETopology)) {
      var i: int;
      var j: int;
      var disk: TVDisk;
      driver = config.Driver;
      transport = config.Transport;
      topology = config.Topology;
      diskCount = DiskCount(topology);
      while (i < diskCount) {
        disk = default(TVDisk);
        disk.Online = true;
        disk.Ready = !config.Fresh;
        disk.Incarnation = i + 1;
        disk.LogStartLsn = 1;
        if (!config.Fresh) { disk.Local = (State = LocalFinal, Guid = i + 1, DbBirthLsn = 0); }
        j = 0;
        while (j < diskCount) {
          if (config.Fresh) {
            disk.PeerGuidInfo += (j, (State = RemoteEmpty, Guid = 0));
          } else {
            disk.PeerGuidInfo += (j, (State = RemoteFinal, Guid = j + 1));
          }
          disk.Cursors += (j, (Guid = j + 1, SyncedLsn = 0));
          j = j + 1;
        }
        disks += (i, disk);
        i = i + 1;
      }
      announce eSeed, (Established = !config.Fresh, Topology = topology);
      send driver, eAck;
    }
    on eStop do (owner: int) {
      disks[owner].Online = false; // Preserve every durable field.
      send driver, eAck;
    }
    on eWipe do (owner: int) {
      var i: int;
      disks[owner].Ready = false;
      disks[owner].Local = default(TLocalGuidInfo);
      disks[owner].Incarnation = disks[owner].Incarnation + diskCount;
      disks[owner].HullBlock = 0;
      disks[owner].LogBlock = 0;
      disks[owner].LogLsn = 0;
      disks[owner].LogStartLsn = 1;
      while (i < diskCount) {
        disks[owner].PeerGuidInfo[i] = default(TSyncGuidInfo);
        disks[owner].Cursors[i] = default(TSyncState);
        i = i + 1;
      }
      send driver, eAck;
    }
    on eWriteBlock do (generation: int) {
      var i: int;
      var acks: set[int];
      while (i < diskCount) {
        if (disks[i].Online && disks[i].Ready) {
          disks[i].HullBlock = generation;
          disks[i].LogBlock = generation;
          disks[i].LogLsn = disks[i].LogLsn + 1;
          acks += (i);
        }
        i = i + 1;
      }
      // Abstract a successful DSProxy TEvBlock, after durable acknowledgements.
      assert HasGroupQuorum(topology, acks);
      announce eBlockCommitted, generation;
      send driver, eAck;
    }
    on eRunPropagators do {
      var source: int;
      var peer: int;
      // Allow the existing periodic propagators to settle, not history relaying.
      while (source < diskCount) {
        if (disks[source].Online && disks[source].Ready) {
          peer = 0;
          while (peer < diskCount) {
            if (disks[peer].Online) {
              disks[peer].PeerGuidInfo[source] =
                (State = RemoteFinal, Guid = disks[source].Local.Guid);
            }
            peer = peer + 1;
          }
        }
        source = source + 1;
      }
      send driver, eAck;
    }
    on eStartRecovery do (owner: int) {
      disks[owner].Online = true;
      disks[owner].Ready = false;
      announce eGuidRecoveryStarted, (Owner = owner, Local = disks[owner].Local);
      new TVDiskGuidRecoveryActor((Storage = this, Owner = owner, Local = disks[owner].Local, Topology = topology));
    }
    on eSurvey do (req: (Source: machine, Owner: int)) {
      var i: int;
      while (i < diskCount) {
        // TEvVSyncGuid is served during GUID recovery, including the self read.
        if (disks[i].Online) {
          new TVDiskGuidProxy((Transport = transport,
            Envelope = (Target = req.Source, Owner = req.Owner,
              Reply = (VDiskId = i, Info = disks[i].PeerGuidInfo[req.Owner]))));
        }
        i = i + 1;
      }
    }
    on ePublishGuid do (req: (Source: machine, Owner: int, Info: TSyncGuidInfo)) {
      var i: int;
      var acks: set[int];
      while (i < diskCount) {
        if (disks[i].Online) {
          // TSyncer::Handle(TEvVSyncGuid) does not veto a different old Final.
          disks[i].PeerGuidInfo[req.Owner] = req.Info;
          acks += (i);
        }
        i = i + 1;
      }
      assert HasGroupQuorum(topology, acks);
      send req.Source, TEvVDiskGuidWritten, acks;
    }
    on TEvSyncerCommit do (req: (Source: machine, Owner: int, Local: TLocalGuidInfo)) {
      disks[req.Owner].Local = req.Local;
      announce eLocalCommitted, (Owner = req.Owner, Local = req.Local);
      send req.Source, TEvSyncerCommitDone;
    }
    on TEvVDiskGuidRecovered do (result: TGuidRecovered) {
      var i: int;
      var sources: set[int];
      var generation: int;
      var owner: int;
      owner = result.Owner;
      if (result.Outcome.Decision == LostData) {
        // TSyncerRecoverLostDataActor: full-sync quorum EXCLUDES self.
        // A recovering/erased source cannot serve a successful empty snapshot.
        while (i < diskCount) {
          if (i != owner && disks[i].Online && disks[i].Ready) {
            sources += (i);
            if (disks[i].HullBlock > generation) { generation = disks[i].HullBlock; }
            disks[owner].Cursors[i] = (Guid = disks[i].Incarnation, SyncedLsn = disks[i].LogLsn);
          }
          i = i + 1;
        }
        assert HasFullSyncQuorum(topology, sources);
        disks[owner].HullBlock = generation;
        disks[owner].LogLsn = disks[owner].LogLsn + 1;
        announce eCallOsiris, owner;
        disks[owner].Local =
          (State = LocalFinal, Guid = result.Outcome.Guid, DbBirthLsn = disks[owner].LogLsn);
        disks[owner].LogStartLsn = disks[owner].Local.DbBirthLsn + 1;
        // Imported Hull metadata is NOT appended to the outgoing SyncLog.
        announce eLocalCommitted, (Owner = owner, Local = disks[owner].Local);
      }
      if (result.Outcome.Decision != Inconsistency) { disks[owner].Ready = true; }
      send driver, eRecoveryReady, result;
    }
    on eStartRound do (owner: int) {
      var i: int;
      assert disks[owner].Online && disks[owner].Ready;
      roundOwner = owner;
      roundPeers = default(set[int]);
      roundDone = default(set[int]);
      announce eRoundStarted, owner;
      while (i < diskCount) {
        if (i != owner && disks[i].Online && disks[i].Ready) {
          roundPeers += (i);
          new TSyncerJobTask((Storage = this, Owner = owner, Peer = i, Current = disks[owner].Cursors[i]));
        }
        i = i + 1;
      }
      assert HasGroupQuorum(topology, roundPeers);
    }
    on TEvVSync do (req: TVSyncRequest) {
      var peer: TVDisk;
      peer = disks[req.Peer];
      assert peer.Online && peer.Ready;
      if (req.SyncState.Guid != peer.Incarnation) {
        send req.Job, TEvVSyncResult, (Status = RESTART,
          SyncState = (Guid = peer.Incarnation, SyncedLsn = peer.Local.DbBirthLsn), Block = 0);
      } else if (req.SyncState.SyncedLsn + 1 < peer.LogStartLsn) {
        send req.Job, TEvVSyncResult, (Status = NODATA,
          SyncState = (Guid = peer.Incarnation, SyncedLsn = peer.Local.DbBirthLsn), Block = 0);
      } else if (peer.LogBlock > 0 && req.SyncState.SyncedLsn < peer.LogLsn) {
        send req.Job, TEvVSyncResult, (Status = OK,
          SyncState = (Guid = peer.Incarnation, SyncedLsn = peer.LogLsn), Block = peer.LogBlock);
      } else {
        send req.Job, TEvVSyncResult, (Status = ALREADY, SyncState = req.SyncState, Block = 0);
      }
    }
    on eFullSyncRequest do (req: TVSyncRequest) {
      assert disks[req.Peer].Online && disks[req.Peer].Ready;
      send req.Job, TEvVSyncResult, (Status = OK,
        SyncState = (Guid = disks[req.Peer].Incarnation, SyncedLsn = disks[req.Peer].LogLsn),
        Block = disks[req.Peer].HullBlock);
    }
    on eImportMetadata do (req: (Job: machine, Owner: int, Peer: int, Block: int, SyncState: TSyncState)) {
      if (req.Block > disks[req.Owner].HullBlock) { disks[req.Owner].HullBlock = req.Block; }
      send req.Job, TEvLocalSyncDataResult;
    }
    on TEvSyncerJobDone do (result: (Owner: int, Peer: int, SyncState: TSyncState, Status: ESyncStatus)) {
      assert result.Owner == roundOwner && result.Peer in roundPeers;
      assert !(result.Peer in roundDone);
      assert result.Status == SyncDone || result.Status == DisksSynced;
      disks[result.Owner].Cursors[result.Peer] = result.SyncState;
      roundDone += (result.Peer);
      announce eSyncPeerDone, (Owner = result.Owner, Peer = result.Peer, Status = result.Status);
      if (sizeof(roundDone) == sizeof(roundPeers)) {
        send driver, eRoundFinished,
          (Owner = roundOwner, HullBlock = disks[roundOwner].HullBlock, Peers = sizeof(roundDone));
      }
    }
  }
}

machine TSyncerJobTask {
  var storage: machine;
  var owner: int;
  var peer: int;
  var current: TSyncState;
  start state EWaitRemote {
    entry (config: (Storage: machine, Owner: int, Peer: int, Current: TSyncState)) {
      storage = config.Storage;
      owner = config.Owner;
      peer = config.Peer;
      current = config.Current;
      NextRequest();
    }
    on TEvVSyncResult do (result: TVSyncResult) {
      if (result.Status == RESTART) {
        // HandleRestart: EJustSync remains EJustSync; no metadata imported.
        current = result.SyncState;
        NextRequest();
      } else if (result.Status == NODATA) {
        // PrepareToFullRecovery/ContinueInFullRecoveryMode, unlike RESTART.
        send storage, eFullSyncRequest, (Job = this, Owner = owner, Peer = peer, SyncState = result.SyncState);
      } else if (result.Status == ALREADY) {
        current = result.SyncState;
        ReplyAndDie(DisksSynced);
      } else {
        current = result.SyncState;
        send storage, eImportMetadata,
          (Job = this, Owner = owner, Peer = peer, Block = result.Block, SyncState = current);
        goto EWaitLocal;
      }
    }
  }
  state EWaitLocal {
    on TEvLocalSyncDataResult do { ReplyAndDie(SyncDone); }
  }
  fun NextRequest() {
    send storage, TEvVSync, (Job = this, Owner = owner, Peer = peer, SyncState = current);
  }
  fun ReplyAndDie(status: ESyncStatus) {
    send storage, TEvSyncerJobDone, (Owner = owner, Peer = peer, SyncState = current, Status = status);
    raise halt;
  }
}
