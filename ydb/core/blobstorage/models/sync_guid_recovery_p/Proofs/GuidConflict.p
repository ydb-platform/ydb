// A minority RemoteFinal is immutable evidence; tentative InProgress is not.
machine GuidConflictProof {
  start state Checking {
    entry {
      var votes: seq[TGuidReply];
      var i: int;
      var history: seq[int];
      var info: TSyncGuidInfo;
      var result: TDecision;
      var tentative: bool;
      announce eScenarioStarted;
      tentative = $;
      while (i < 8) { history += (i, 0); i = i + 1; }
      i = 0;
      while (i < 8) {
        info = default(TSyncGuidInfo);
        if (i == 0) {
          info = (State = RemoteFinal, Guid = 2);
          if (tentative) { info.State = InProgress; }
        }
        votes += (i, (VDiskId = i, Info = info, RecoveredGuid = 0, History = history));
        i = i + 1;
      }
      result = ReachAVerdict(Block4Plus2,
        (State = LocalFinal, Guid = 1, DbBirthLsn = 7), votes, 1);
      if (tentative) {
        assert result.Decision == Good && result.Guid == 1 && result.Step == ACTION_WriteFinalToQuorum;
      } else { assert result.Decision == Inconsistency; }
      announce eScenarioFinished;
    }
  }
}

test tcProofGuidConflict [main=GuidConflictProof]:
  assert ScenarioCompletes in { GuidConflictProof };
