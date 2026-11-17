event eReleaseImportAck: machine;

// Relevant scheduler contract: retry EFullRecover after failure, keep OldSyncState,
// and report SyncDone only after local import acknowledgement.
machine FullSyncRetryProof {
  var failed: bool;
  var fullSyncRequested: bool;
  var applied: bool;
  var ackSent: bool;
  start state Checking {
    entry {
      announce eScenarioStarted;
      new TSyncerJobTask((Storage = this, Owner = 0, Peer = 1,
        Current = (Guid = 1, SyncedLsn = 7), FullRecovery = false));
    }
    on TEvVSync do (req: TVSyncRequest) {
      var status: EReplyStatus;
      assert !failed && req.SyncState == (Guid = 1, SyncedLsn = 7);
      status = RESTART;
      if ($) { status = NODATA; }
      send req.Job, TEvVSyncResult,
        (Status = status, SyncState = (Guid = 2, SyncedLsn = 10), Block = 0);
    }
    on eFullSyncRequest do (req: TVSyncRequest) {
      if (!failed) {
        send req.Job, eFullSyncFragment,
          (Status = OK, SyncState = (Guid = 2, SyncedLsn = 20), Block = 0);
        send req.Job, TEvVSyncResult,
          (Status = INTERRUPTED, SyncState = (Guid = 2, SyncedLsn = 20), Block = 0);
      } else {
        assert fullSyncRequested;
        send req.Job, TEvVSyncResult,
          (Status = OK, SyncState = (Guid = 2, SyncedLsn = 20), Block = 42);
      }
    }
    on eImportMetadata do (req: (Job: machine, Owner: int, Peer: int, Block: int, SyncState: TSyncState)) {
      assert failed && fullSyncRequested && req.Block == 42 && !applied;
      applied = true;
      send this, eReleaseImportAck, req.Job;
    }
    on eReleaseImportAck do (job: machine) {
      ackSent = true;
      send job, TEvLocalSyncDataResult;
    }
    on TEvSyncerJobDone do (result: (Owner: int, Peer: int, SyncState: TSyncState, Status: ESyncStatus)) {
      if (!failed) {
        assert result.Status == Error && !applied;
        assert result.SyncState == (Guid = 1, SyncedLsn = 7), "Incomplete full-sync advanced OldSyncState";
        failed = true;
        fullSyncRequested = true;
        new TSyncerJobTask((Storage = this, Owner = 0, Peer = 1,
          Current = result.SyncState, FullRecovery = fullSyncRequested));
      } else {
        assert applied && ackSent && result.Status == SyncDone, "SyncDone preceded local import acknowledgement";
        assert result.SyncState == (Guid = 2, SyncedLsn = 20);
        fullSyncRequested = false;
        announce eScenarioFinished;
      }
    }
  }
}

test tcProofFullSyncRetry [main=FullSyncRetryProof]:
  assert ScenarioCompletes in { FullSyncRetryProof, TSyncerJobTask };
