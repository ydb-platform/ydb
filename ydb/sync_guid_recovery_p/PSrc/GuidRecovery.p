// Port of TDecisionMaker::{VDiskQuorumDecision,HandleQuorum*,CheckLocalAndRemoteGuid}.
// See README for pinned C++ sources and abstraction boundaries.
fun ReachAVerdict(local: TLocalGuidInfo, votes: seq[TSyncGuidInfo]): TDecision {
  var vote: TSyncGuidInfo;
  var finalGuid: int;
  var finalCount: int;
  var progress: map[int, int];
  var guid: int;
  var most: int;
  var count: int;
  var step: EFirstRunStep;
  var decision: EDecision;

  assert sizeof(votes) == 6;
  foreach (vote in votes) {
    if (vote.State == RemoteFinal) {
      if (finalCount > 0 && finalGuid != vote.Guid) {
        return (Decision = Inconsistency, Guid = 0, Step = STATE__Terminated);
      }
      finalGuid = vote.Guid;
      finalCount = finalCount + 1;
    } else if (vote.State == InProgress) {
      if (!(vote.Guid in progress)) { progress[vote.Guid] = 0; }
      progress[vote.Guid] = progress[vote.Guid] + 1;
    }
  }
  if (finalCount > 0) {
    step = STATE__Terminated;
    if (finalCount < 6) { step = ACTION_WriteFinalToQuorum; }
    if (local.State == LocalEmpty) { decision = LostData; }
    else if (local.State == Selected) {
      decision = FirstRun;
      if (finalCount == 6) { step = ACTION_WriteFinalLocally; }
    } else if (local.State == Lost) { decision = LostData; }
    else { decision = Good; }
    // HandleQuorumFinal(Lost) deliberately has no local/remote GUID check.
    if (local.State == Selected || local.State == LocalFinal ||
        (local.State == Lost && finalCount < 6)) {
      if (local.Guid != finalGuid) { decision = Inconsistency; }
    }
    return (Decision = decision, Guid = finalGuid, Step = step);
  }
  if (sizeof(progress) > 0) {
    // C++ TMap plus strict > chooses the smallest GUID on equal counts.
    foreach (guid in keys(progress)) {
      count = progress[guid];
      if (count > most || (count == most && guid < finalGuid)) {
        most = count;
        finalGuid = guid;
      }
    }
    decision = FirstRun;
    if (local.State == Lost || local.State == LocalFinal ||
        (local.State == Selected && local.Guid != finalGuid)) {
      decision = Inconsistency;
    }
    return (Decision = decision, Guid = finalGuid, Step = ACTION_WriteInProgressToQuorum);
  }
  if (local.State == LocalEmpty) {
    return (Decision = FirstRun, Guid = 0, Step = ACTION_GenerateGuid);
  }
  return (Decision = Inconsistency, Guid = 0, Step = STATE__Terminated);
}

machine TVDiskGuidRecoveryActor {
  var storage: machine;
  var owner: int;
  var local: TLocalGuidInfo;
  var seen: set[int];
  var votes: seq[TSyncGuidInfo];
  var outcome: TDecision;

  start state Survey {
    entry (config: (Storage: machine, Owner: int, Local: TLocalGuidInfo)) {
      storage = config.Storage;
      owner = config.Owner;
      local = config.Local;
      send storage, eSurvey, (Source = this, Owner = owner);
    }
    on TEvVDiskGuidObtained do (reply: TGuidReply) {
      if (reply.VDiskId in seen) { return; }
      seen += (reply.VDiskId);
      votes += (sizeof(votes), reply.Info);
      announce eSurveyVote, (Owner = owner, Info = reply.Info);
      if (sizeof(votes) == 6) {
        outcome = ReachAVerdict(local, votes);
        announce eGuidVerdict, (Owner = owner, Decision = outcome.Decision);
        if (outcome.Decision == Inconsistency) { Finish(); }
        if (outcome.Step == ACTION_GenerateGuid) {
          // Fresh symbolic GUID; disjoint from seeded eternal GUIDs 1..8.
          outcome.Guid = 10000 + owner;
          outcome.Step = ACTION_WriteInProgressToQuorum;
        }
        if (outcome.Step == ACTION_WriteInProgressToQuorum) {
          send storage, ePublishGuid,
            (Source = this, Owner = owner, Info = (State = InProgress, Guid = outcome.Guid));
          goto WritingInProgress;
        }
        if (outcome.Step == ACTION_WriteFinalToQuorum) { PublishFinal(); }
        if (outcome.Step == ACTION_WriteFinalLocally) { CommitFinal(); }
        SettleOrFinish();
      }
    }
  }
  state WritingInProgress {
    ignore TEvVDiskGuidObtained;
    on TEvVDiskGuidWritten do (count: int) {
      assert count >= 6;
      send storage, TEvSyncerCommit,
        (Source = this, Owner = owner,
         Local = (State = Selected, Guid = outcome.Guid, DbBirthLsn = local.DbBirthLsn));
      goto WritingSelected;
    }
  }
  state WritingSelected {
    ignore TEvVDiskGuidObtained;
    on TEvSyncerCommitDone do { PublishFinal(); }
  }
  state WritingFinal {
    ignore TEvVDiskGuidObtained;
    on TEvVDiskGuidWritten do (count: int) {
      assert count >= 6;
      // EFirstRunMode::PublishOnly must not overwrite local state/birth LSN.
      if (outcome.Decision == FirstRun) { CommitFinal(); }
      SettleOrFinish();
    }
  }
  state WritingLocalFinal {
    ignore TEvVDiskGuidObtained;
    on TEvSyncerCommitDone do { Finish(); }
  }
  state SettlingLost {
    ignore TEvVDiskGuidObtained;
    on TEvSyncerCommitDone do { Finish(); }
  }
  fun PublishFinal() {
    send storage, ePublishGuid,
      (Source = this, Owner = owner, Info = (State = RemoteFinal, Guid = outcome.Guid));
    goto WritingFinal;
  }
  fun CommitFinal() {
    send storage, TEvSyncerCommit,
      (Source = this, Owner = owner, Local = (State = LocalFinal, Guid = outcome.Guid, DbBirthLsn = 0));
    goto WritingLocalFinal;
  }
  fun SettleOrFinish() {
    if (outcome.Decision == LostData) {
      send storage, TEvSyncerCommit,
        (Source = this, Owner = owner,
         Local = (State = Lost, Guid = outcome.Guid, DbBirthLsn = local.DbBirthLsn));
      goto SettlingLost;
    }
    Finish();
  }
  fun Finish() {
    send storage, TEvVDiskGuidRecovered, (Owner = owner, Outcome = outcome);
    // A completed/cancelled proxy can still have a reply in flight.
    goto Finished;
  }
  state Finished { ignore TEvVDiskGuidObtained; }
}

// Independent proxy actors permit response reordering; snapshots are actual reads.
machine TVDiskGuidProxy {
  start state SendReply {
    entry (config: (Transport: machine, Envelope: TGuidEnvelope)) {
      send config.Transport, eGuidEnvelope, config.Envelope;
      raise halt;
    }
  }
}

// Test transport: a finite reply delay, never a fabricated GUID/Empty response.
machine SurveyTransport {
  var driver: machine;
  var mode: EScenario;
  var configured: bool;
  var delivered: int;
  var pending: seq[TGuidEnvelope];
  start state Routing {
    entry (source: machine) { driver = source; }
    on eConfigureDelay do (scenario: EScenario) {
      mode = scenario;
      configured = true;
      send driver, eAck;
    }
    on eGuidEnvelope do (env: TGuidEnvelope) {
      var hold: bool;
      var buffered: TGuidEnvelope;
      if (configured && env.Owner == 0 && delivered < 6) {
        hold = (mode == DelayedSelfGuid && env.Reply.VDiskId == 0) ||
          (mode != DelayedSelfGuid && (env.Reply.VDiskId == 6 || env.Reply.VDiskId == 7));
        if (hold) { pending += (sizeof(pending), env); return; }
        delivered = delivered + 1;
      }
      send env.Target, TEvVDiskGuidObtained, env.Reply;
      if (delivered == 6) {
        foreach (buffered in pending) {
          send buffered.Target, TEvVDiskGuidObtained, buffered.Reply;
        }
        pending = default(seq[TGuidEnvelope]);
      }
    }
  }
}
