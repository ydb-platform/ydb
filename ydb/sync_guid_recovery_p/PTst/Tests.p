test tcStaleMetadata [main=TestStaleMetadata]:
  assert MetadataIsCompleteWhenSynced, ScenarioCompletes in
  { TestStaleMetadata, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcDelayedSelfGuid [main=TestDelayedSelfGuid]:
  assert PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestDelayedSelfGuid, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcFalseFirstRun [main=TestFalseFirstRun]:
  assert NeverReinitializeExistingVDisk, ScenarioCompletes in
  { TestFalseFirstRun, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcNoWipes [main=TestNoWipes]:
  assert MetadataIsCompleteWhenSynced, NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestNoWipes, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcRetainedSource [main=TestRetainedSource]:
  assert MetadataIsCompleteWhenSynced, NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestRetainedSource, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcWipedWithWitnesses [main=TestWipedWithWitnesses]:
  assert MetadataIsCompleteWhenSynced, NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestWipedWithWitnesses, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcFreshGroup [main=TestFreshGroup]:
  assert NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestFreshGroup, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
