machine RollingWipeDriver {
  var scenario: EScenario;
  var storage: machine;
  var transport: machine;
  var nextDisk: int;
  var wipeCount: int;
  start state Boot {
    entry (selected: EScenario) {
      scenario = selected;
      announce eScenarioStarted;
      transport = new SurveyTransport(this);
      storage = new TRecoveryStorage((Driver = this, Transport = transport, Fresh = scenario == FreshGroup));
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
      wipeCount = 7;
      if (scenario == NoWipes || scenario == WipedWithWitnesses) { wipeCount = 0; }
      if (scenario == RetainedSource) { wipeCount = 6; }
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
    entry { send transport, eConfigureDelay, scenario; }
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

machine TestStaleMetadata { start state Init { entry { new RollingWipeDriver(StaleMetadata); raise halt; } } }
machine TestDelayedSelfGuid { start state Init { entry { new RollingWipeDriver(DelayedSelfGuid); raise halt; } } }
machine TestFalseFirstRun { start state Init { entry { new RollingWipeDriver(FalseFirstRun); raise halt; } } }
machine TestNoWipes { start state Init { entry { new RollingWipeDriver(NoWipes); raise halt; } } }
machine TestRetainedSource { start state Init { entry { new RollingWipeDriver(RetainedSource); raise halt; } } }
machine TestWipedWithWitnesses { start state Init { entry { new RollingWipeDriver(WipedWithWitnesses); raise halt; } } }
machine TestFreshGroup { start state Init { entry { new RollingWipeDriver(FreshGroup); raise halt; } } }
