// TDecisionMaker with durable identity evidence separate from current quorum votes.
// See MODEL.md for pinned C++ sources and abstraction boundaries.
fun ReachAVerdict(topology: ETopology, local: TLocalGuidInfo, votes: seq[TGuidReply]): TDecision {
  var reply: TGuidReply;
  var vote: TSyncGuidInfo;
  var finalGuid: int;
  var knownGuid: int;
  var finalDisks: set[int];
  var progress: map[int, int];
  var guid: int;
  var most: int;
  var count: int;
  var step: EFirstRunStep;
  var decision: EDecision;

  if (local.State == LocalFinal || local.State == Lost) { knownGuid = local.Guid; }
  foreach (reply in votes) {
    if (reply.RecoveredGuid != 0) {
      if (knownGuid != 0 && knownGuid != reply.RecoveredGuid) {
        return (Decision = Inconsistency, Guid = 0, Step = STATE__Terminated);
      }
      knownGuid = reply.RecoveredGuid;
    }
    vote = reply.Info;
    if (vote.State == RemoteFinal) {
      if (sizeof(finalDisks) > 0 && finalGuid != vote.Guid) {
        return (Decision = Inconsistency, Guid = 0, Step = STATE__Terminated);
      }
      finalGuid = vote.Guid;
      finalDisks += (reply.VDiskId);
    } else if (vote.State == InProgress) {
      if (!(vote.Guid in progress)) { progress[vote.Guid] = 0; }
      progress[vote.Guid] = progress[vote.Guid] + 1;
    }
  }
  if (knownGuid != 0) {
    // Only incompatible Final evidence conflicts with an immutable identity.
    if (finalGuid != 0 && finalGuid != knownGuid) {
      return (Decision = Inconsistency, Guid = 0, Step = STATE__Terminated);
    }
    finalGuid = knownGuid;
  }
  if (sizeof(finalDisks) > 0 || knownGuid != 0) {
    // A fact selects the old GUID but adds no Final responder IDs.
    step = STATE__Terminated;
    if (!HasGroupQuorum(topology, finalDisks)) { step = ACTION_WriteFinalToQuorum; }
    if (local.State == LocalEmpty) { decision = LostData; }
    else if (local.State == Selected) {
      decision = FirstRun;
      if (HasGroupQuorum(topology, finalDisks)) { step = ACTION_WriteFinalLocally; }
    } else if (local.State == Lost) { decision = LostData; }
    else { decision = Good; }
    if (local.State == Selected || local.State == LocalFinal ||
        (local.State == Lost && !HasGroupQuorum(topology, finalDisks))) {
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
  var topology: ETopology;
  var local: TLocalGuidInfo;
  var seen: set[int];
  var votes: seq[TGuidReply];
  var outcome: TDecision;

  start state Survey {
    entry (config: (Storage: machine, Owner: int, Local: TLocalGuidInfo, Topology: ETopology)) {
      var i: int;
      storage = config.Storage;
      owner = config.Owner;
      local = config.Local;
      topology = config.Topology;
      // Empty slots contribute no evidence; reply order is not protocol state.
      while (i < DiskCount(topology)) {
        votes += (i, default(TGuidReply));
        i = i + 1;
      }
      send storage, eSurvey, (Source = this, Owner = owner);
    }
    on TEvVDiskGuidObtained do (reply: TGuidReply) {
      if (reply.VDiskId in seen) { return; }
      seen += (reply.VDiskId);
      seen = CanonicalDisks(seen, DiskCount(topology));
      votes[reply.VDiskId] = reply;
      announce eSurveyVote, (Owner = owner, Info = reply.Info);
      if (HasGroupQuorum(topology, seen)) {
        outcome = ReachAVerdict(topology, local, votes);
        votes = default(seq[TGuidReply]);
        seen = default(set[int]);
        announce eGuidVerdict, (Owner = owner, Decision = outcome.Decision);
        if (outcome.Decision == Inconsistency) { Finish(); }
        if (outcome.Step == ACTION_GenerateGuid) {
          // Fresh symbolic GUID; disjoint from seeded eternal GUIDs.
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
    on TEvVDiskGuidWritten do (acks: set[int]) {
      assert HasGroupQuorum(topology, acks);
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
    on TEvVDiskGuidWritten do (acks: set[int]) {
      assert HasGroupQuorum(topology, acks);
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
    // Finished only discards late replies; none of these fields is read again.
    storage = default(machine);
    owner = 0;
    topology = default(ETopology);
    local = default(TLocalGuidInfo);
    seen = default(set[int]);
    votes = default(seq[TGuidReply]);
    outcome = default(TDecision);
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
  var topology: ETopology;
  var configured: bool;
  var delivered: set[int];
  var pending: seq[TGuidEnvelope];
  start state Routing {
    entry (source: machine) { driver = source; }
    on eConfigureDelay do (config: (Scenario: EScenario, Topology: ETopology)) {
      mode = config.Scenario;
      topology = config.Topology;
      configured = true;
      send driver, eAck;
    }
    on eGuidEnvelope do (env: TGuidEnvelope) {
      var hold: bool;
      var buffered: TGuidEnvelope;
      if (configured && env.Owner == 0 && !HasGroupQuorum(topology, delivered)) {
        if (topology == Mirror3dc) {
          // Five-reply quorums: {3,4,5,6,7} without self, {0,1,2,3,4} with self.
          hold = (mode == DelayedSelfGuid && (env.Reply.VDiskId < 3 || env.Reply.VDiskId == 8)) ||
            (mode != DelayedSelfGuid && env.Reply.VDiskId >= 5);
        } else {
          hold = (mode == DelayedSelfGuid && env.Reply.VDiskId == 0) ||
            (mode != DelayedSelfGuid && (env.Reply.VDiskId == 6 || env.Reply.VDiskId == 7));
        }
        if (hold) { pending += (sizeof(pending), env); return; }
        delivered += (env.Reply.VDiskId);
        delivered = CanonicalDisks(delivered, DiskCount(topology));
      }
      send env.Target, TEvVDiskGuidObtained, env.Reply;
      if (HasGroupQuorum(topology, delivered)) {
        foreach (buffered in pending) {
          send buffered.Target, TEvVDiskGuidObtained, buffered.Reply;
        }
        pending = default(seq[TGuidEnvelope]);
        configured = false;
        delivered = default(set[int]);
        mode = default(EScenario);
        topology = default(ETopology);
      }
    }
  }
}
