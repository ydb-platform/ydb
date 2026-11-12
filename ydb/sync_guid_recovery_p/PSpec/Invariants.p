spec NeverReinitializeExistingVDisk observes eSeed, eGuidRecoveryStarted, eLocalCommitted, TEvVDiskGuidRecovered {
  var initialized: set[int];
  var initializedBefore: map[int, bool];
  var identity: map[int, int];
  start state Watching {
    on eSeed do (config: (Established: bool, Topology: ETopology)) {
      var i: int;
      if (config.Established) {
        while (i < DiskCount(config.Topology)) { initialized += (i); identity[i] = i + 1; i = i + 1; }
      }
    }
    on eGuidRecoveryStarted do (req: (Owner: int, Local: TLocalGuidInfo)) {
      initializedBefore[req.Owner] = req.Owner in initialized;
    }
    on eLocalCommitted do (commit: (Owner: int, Local: TLocalGuidInfo)) {
      if (commit.Local.State == LocalFinal && !(commit.Owner in initialized)) {
        initialized += (commit.Owner);
        identity[commit.Owner] = commit.Local.Guid;
      }
    }
    on TEvVDiskGuidRecovered do (result: TGuidRecovered) {
      if (initializedBefore[result.Owner]) {
        assert result.Outcome.Decision != FirstRun,
          "Existing VDisk was classified FirstRun after its GUID witnesses were erased";
        if (result.Outcome.Decision != Inconsistency) {
          assert result.Outcome.Guid == identity[result.Owner], "Eternal GUID changed";
        }
      }
    }
  }
}

spec PreservedIdentityRemainsRecoverable observes eGuidRecoveryStarted, eSurveyVote, eGuidVerdict {
  var local: map[int, TLocalGuidInfo];
  var conflict: map[int, bool];
  start state Watching {
    on eGuidRecoveryStarted do (req: (Owner: int, Local: TLocalGuidInfo)) {
      local[req.Owner] = req.Local;
      conflict[req.Owner] = false;
    }
    on eSurveyVote do (vote: (Owner: int, Info: TSyncGuidInfo)) {
      if (vote.Info.State == RemoteFinal && vote.Info.Guid != local[vote.Owner].Guid) {
        conflict[vote.Owner] = true;
      }
    }
    on eGuidVerdict do (result: (Owner: int, Decision: EDecision)) {
      if (local[result.Owner].State == LocalFinal && !conflict[result.Owner]) {
        assert result.Decision != Inconsistency,
          "Intact local Final rejected after an Empty first reply quorum";
      }
    }
  }
}

spec MetadataIsCompleteWhenSynced observes eSeed, eBlockCommitted, eRoundStarted, eSyncPeerDone, eRoundFinished {
  var topology: ETopology;
  var committed: int;
  var expected: int;
  var owner: int;
  var completed: set[int];
  start state Watching {
    on eSeed do (config: (Established: bool, Topology: ETopology)) { topology = config.Topology; }
    on eBlockCommitted do (generation: int) { committed = generation; }
    on eRoundStarted do (disk: int) {
      owner = disk;
      expected = committed;
      completed = default(set[int]);
    }
    on eSyncPeerDone do (result: (Owner: int, Peer: int, Status: ESyncStatus)) {
      assert result.Owner == owner && !(result.Peer in completed);
      assert result.Status == SyncDone || result.Status == DisksSynced;
      completed += (result.Peer);
    }
    on eRoundFinished do (result: (Owner: int, HullBlock: int, Peers: int)) {
      assert result.Owner == owner && sizeof(completed) == result.Peers && HasGroupQuorum(topology, completed);
      assert result.HullBlock >= expected,
        format("All {0} peers synced, but Hull Block generation {1} misses committed {2}",
          result.Peers, result.HullBlock, expected);
    }
  }
}

// Controls cannot pass just by abandoning the bounded scenario before its check.
spec ScenarioCompletes observes eScenarioStarted, eScenarioFinished {
  start state Idle { on eScenarioStarted goto Pending; }
  hot state Pending { on eScenarioFinished goto Done; }
  state Done { }
}
