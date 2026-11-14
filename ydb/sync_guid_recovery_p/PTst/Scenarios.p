machine RollingWipeDriver {
  var scenario: EScenario;
  var topology: ETopology;
  var storage: machine;
  var transport: machine;
  var nextDisk: int;
  var wipeCount: int;
  start state Boot {
    entry (config: (Scenario: EScenario, Topology: ETopology)) {
      scenario = config.Scenario;
      topology = config.Topology;
      announce eScenarioStarted;
      transport = new SurveyTransport(this);
      storage = new TRecoveryStorage((Driver = this, Transport = transport, Fresh = scenario == FreshGroup, Topology = topology));
    }
    on eAck do {
      if (scenario == FreshGroup) { goto ConfiguringReturn; }
      send storage, eStop, 0;
      goto Stopped;
    }
  }
  state Stopped {
    on eAck do {
      if (scenario == FalseFirstRun || scenario == WipedWithWitnesses) {
        send storage, eWipe, 0;
        goto Erased;
      }
      WriteBlock();
    }
  }
  state Erased { on eAck do { WriteBlock(); } }
  state Written {
    on eAck do {
      wipeCount = DiskCount(topology) - 1;
      if (scenario == NoWipes || scenario == WipedWithWitnesses) { wipeCount = 0; }
      if (scenario == RetainedSource) { wipeCount = DiskCount(topology) - 2; }
      nextDisk = 1;
      goto Rolling;
    }
  }
  state Rolling {
    entry {
      if (nextDisk > wipeCount) { goto ConfiguringReturn; }
      send storage, eWipe, nextDisk;
    }
    on eAck do {
      send storage, eStartRecovery, nextDisk;
      goto RecoveringPeer;
    }
  }
  state RecoveringPeer {
    on eRecoveryReady do (result: TGuidRecovered) {
      // Setup check: each wipe really takes the lost-data recovery path.
      assert result.Owner == nextDisk && result.Outcome.Decision == LostData;
      send storage, eRunPropagators;
      goto SettledPeer;
    }
  }
  state SettledPeer {
    on eAck do { nextDisk = nextDisk + 1; goto Rolling; }
  }
  state ConfiguringReturn {
    entry { send transport, eConfigureDelay, (Scenario = scenario, Topology = topology); }
    on eAck do { send storage, eStartRecovery, 0; goto Returning; }
  }
  state Returning {
    on eRecoveryReady do (result: TGuidRecovered) {
      assert result.Owner == 0;
      if (scenario == FreshGroup || result.Outcome.Decision == Inconsistency) { Finish(); }
      send storage, eStartRound, 0;
      goto Synchronizing;
    }
  }
  state Synchronizing { on eRoundFinished do { Finish(); } }
  state Done { }
  fun WriteBlock() { send storage, eWriteBlock, 42; goto Written; }
  fun Finish() { announce eScenarioFinished; goto Done; }
}

machine TestStaleMetadata { start state Init { entry { new RollingWipeDriver((Scenario = StaleMetadata, Topology = Block4Plus2)); raise halt; } } }
machine TestDelayedSelfGuid { start state Init { entry { new RollingWipeDriver((Scenario = DelayedSelfGuid, Topology = Block4Plus2)); raise halt; } } }
machine TestFalseFirstRun { start state Init { entry { new RollingWipeDriver((Scenario = FalseFirstRun, Topology = Block4Plus2)); raise halt; } } }
machine TestNoWipes { start state Init { entry { new RollingWipeDriver((Scenario = NoWipes, Topology = Block4Plus2)); raise halt; } } }
machine TestRetainedSource { start state Init { entry { new RollingWipeDriver((Scenario = RetainedSource, Topology = Block4Plus2)); raise halt; } } }
machine TestWipedWithWitnesses { start state Init { entry { new RollingWipeDriver((Scenario = WipedWithWitnesses, Topology = Block4Plus2)); raise halt; } } }
machine TestFreshGroup { start state Init { entry { new RollingWipeDriver((Scenario = FreshGroup, Topology = Block4Plus2)); raise halt; } } }

machine TestMirror3dcStaleMetadata { start state Init { entry { new RollingWipeDriver((Scenario = StaleMetadata, Topology = Mirror3dc)); raise halt; } } }
machine TestMirror3dcDelayedSelfGuid { start state Init { entry { new RollingWipeDriver((Scenario = DelayedSelfGuid, Topology = Mirror3dc)); raise halt; } } }
machine TestMirror3dcFalseFirstRun { start state Init { entry { new RollingWipeDriver((Scenario = FalseFirstRun, Topology = Mirror3dc)); raise halt; } } }
machine TestMirror3dcNoWipes { start state Init { entry { new RollingWipeDriver((Scenario = NoWipes, Topology = Mirror3dc)); raise halt; } } }
machine TestMirror3dcRetainedSource { start state Init { entry { new RollingWipeDriver((Scenario = RetainedSource, Topology = Mirror3dc)); raise halt; } } }
machine TestMirror3dcWipedWithWitnesses { start state Init { entry { new RollingWipeDriver((Scenario = WipedWithWitnesses, Topology = Mirror3dc)); raise halt; } } }
machine TestMirror3dcFreshGroup { start state Init { entry { new RollingWipeDriver((Scenario = FreshGroup, Topology = Mirror3dc)); raise halt; } } }
