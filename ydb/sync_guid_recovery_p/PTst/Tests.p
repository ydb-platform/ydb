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

test tcMirror3dcStaleMetadata [main=TestMirror3dcStaleMetadata]:
  assert MetadataIsCompleteWhenSynced, ScenarioCompletes in
  { TestMirror3dcStaleMetadata, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcMirror3dcDelayedSelfGuid [main=TestMirror3dcDelayedSelfGuid]:
  assert PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestMirror3dcDelayedSelfGuid, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcMirror3dcFalseFirstRun [main=TestMirror3dcFalseFirstRun]:
  assert NeverReinitializeExistingVDisk, ScenarioCompletes in
  { TestMirror3dcFalseFirstRun, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcMirror3dcNoWipes [main=TestMirror3dcNoWipes]:
  assert MetadataIsCompleteWhenSynced, NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestMirror3dcNoWipes, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcMirror3dcRetainedSource [main=TestMirror3dcRetainedSource]:
  assert MetadataIsCompleteWhenSynced, NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestMirror3dcRetainedSource, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcMirror3dcWipedWithWitnesses [main=TestMirror3dcWipedWithWitnesses]:
  assert MetadataIsCompleteWhenSynced, NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestMirror3dcWipedWithWitnesses, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
test tcMirror3dcFreshGroup [main=TestMirror3dcFreshGroup]:
  assert NeverReinitializeExistingVDisk, PreservedIdentityRemainsRecoverable, ScenarioCompletes in
  { TestMirror3dcFreshGroup, RollingWipeDriver, TRecoveryStorage, SurveyTransport,
    TVDiskGuidRecoveryActor, TVDiskGuidProxy, TSyncerJobTask };
