// Exercise every post-verdict recovery phase with conflicting late replies.
machine ClosedSurveyProof {
  var recovery: machine;
  var guid: int;
  var probes: int;
  start state Checking {
    entry {
      var mode: int;
      var local: TLocalGuidInfo;
      mode = choose(3); // genuine initialization, intact Final, lost data
      guid = 1;
      if (mode == 0) { guid = 10000; }
      if (mode == 1) { local = (State = LocalFinal, Guid = guid, DbBirthLsn = 7); }
      announce eScenarioStarted;
      recovery = new TVDiskGuidRecoveryActor(
        (Storage = this, Owner = 0, Local = local, Topology = Block4Plus2));
    }
    on eSurvey do (req: (Source: machine, Owner: int)) {
      var i: int;
      var info: TSyncGuidInfo;
      assert req.Source == recovery && req.Owner == 0;
      while (i < 6) {
        info = default(TSyncGuidInfo);
        if (guid == 1 && i == 0) { info = (State = RemoteFinal, Guid = guid); }
        send recovery, TEvVDiskGuidObtained, (VDiskId = i, Info = info, RecoveredGuid = 0);
        i = i + 1;
      }
    }
    on ePublishGuid do (req: (Source: machine, Owner: int, Info: TSyncGuidInfo)) {
      var acks: set[int];
      var i: int;
      assert req.Source == recovery && req.Owner == 0 && req.Info.Guid == guid;
      Probe();
      while (i < 8) { acks += (i); i = i + 1; }
      send recovery, TEvVDiskGuidWritten, acks;
    }
    on TEvSyncerCommit do (req: (Source: machine, Owner: int, Local: TLocalGuidInfo)) {
      assert req.Source == recovery && req.Owner == 0 && req.Local.Guid == guid;
      Probe();
      send recovery, TEvSyncerCommitDone;
    }
    on TEvVDiskGuidRecovered do (result: TGuidRecovered) {
      assert result.Owner == 0 && result.Outcome.Guid == guid;
      assert result.Outcome.Decision != Inconsistency;
      assert probes > 0;
      Probe(); // Finished must also discard a late conflicting reply.
      announce eScenarioFinished;
    }
  }
  fun Probe() {
    probes = probes + 1;
    send recovery, TEvVDiskGuidObtained,
      (VDiskId = 7, Info = (State = RemoteFinal, Guid = 2), RecoveredGuid = 2);
  }
}

spec ClosedSurveyIsSilent observes eGuidVerdict, eSurveyVote, TEvVDiskGuidRecovered {
  var closed: bool;
  var recovered: bool;
  start state Watching {
    on eGuidVerdict do { assert !closed; closed = true; }
    on eSurveyVote do { assert !closed, "Closed survey accepted another vote"; }
    on TEvVDiskGuidRecovered do { assert closed && !recovered; recovered = true; }
  }
}

test tcProofClosedSurvey [main=ClosedSurveyProof]:
  assert ClosedSurveyIsSilent, ScenarioCompletes in
  { ClosedSurveyProof, TVDiskGuidRecoveryActor };
