// Exercise every post-survey recovery phase with conflicting late replies.
machine ClosedSurveyProof {
  var recovery: machine;
  var guid: int;
  var probes: int;
  var history: seq[int];
  start state Checking {
    entry {
      var mode: int;
      var local: TLocalGuidInfo;
      var i: int;
      mode = choose(3); // genuine initialization, intact Final, lost data
      guid = 1;
      if (mode == 0) { guid = 10000; }
      if (mode == 1) { local = (State = LocalFinal, Guid = guid, DbBirthLsn = 7); }
      while (i < 8) { history += (i, 0); i = i + 1; }
      announce eScenarioStarted;
      recovery = new TVDiskGuidRecoveryActor(
        (Storage = this, Owner = 0, Local = local, Topology = Block4Plus2, History = history));
    }
    on eSurvey do (req: (Source: machine, Owner: int)) {
      var i: int;
      var info: TSyncGuidInfo;
      var replyHistory: seq[int];
      assert req.Source == recovery && req.Owner == 0;
      while (i < 6) {
        info = default(TSyncGuidInfo);
        replyHistory = history;
        if (guid == 1 && i == 0) {
          info = (State = RemoteFinal, Guid = guid);
          replyHistory[0] = guid;
        }
        send recovery, TEvVDiskGuidObtained, (VDiskId = i, Info = info, RecoveredGuid = 0, History = replyHistory);
        i = i + 1;
      }
    }
    on eCommitGuidHistory do (req: (Source: machine, Owner: int, History: seq[int])) {
      assert req.Source == recovery && req.Owner == 0;
      if (guid == 1) { assert req.History[0] == 1; }
      else { assert req.History[0] == 0; }
      history = req.History;
      Probe();
      send recovery, TEvSyncerCommitDone;
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
    var conflictingHistory: seq[int];
    probes = probes + 1;
    conflictingHistory = history;
    conflictingHistory[0] = 2;
    send recovery, TEvVDiskGuidObtained,
      (VDiskId = 7, Info = (State = RemoteFinal, Guid = 2), RecoveredGuid = 2, History = conflictingHistory);
  }
}

spec ClosedSurveyIsSilent observes eCommitGuidHistory, TEvSyncerCommitDone, eGuidVerdict, eSurveyVote, TEvVDiskGuidRecovered {
  var closed: bool;
  var committed: bool;
  var decided: bool;
  var recovered: bool;
  start state Watching {
    on eCommitGuidHistory do { assert !closed; closed = true; }
    on TEvSyncerCommitDone do {
      if (!decided) { assert closed && !committed; committed = true; }
    }
    on eGuidVerdict do { assert committed && !decided; decided = true; }
    on eSurveyVote do { assert !closed, "Closed survey accepted another vote"; }
    on TEvVDiskGuidRecovered do { assert decided && !recovered; recovered = true; }
  }
}

test tcProofClosedSurvey [main=ClosedSurveyProof]:
  assert ClosedSurveyIsSilent, ScenarioCompletes in
  { ClosedSurveyProof, TVDiskGuidRecoveryActor };
