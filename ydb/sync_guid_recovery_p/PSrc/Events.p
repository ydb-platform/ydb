enum ETopology { Block4Plus2, Mirror3dc }
enum ELocalGuidState { LocalEmpty, Selected, Lost, LocalFinal }
enum ESyncGuidState { RemoteEmpty, InProgress, RemoteFinal }
enum EDecision { FirstRun, Good, LostData, Inconsistency }
enum EFirstRunStep {
  ACTION_GenerateGuid, ACTION_WriteInProgressToQuorum,
  ACTION_WriteFinalToQuorum, ACTION_WriteFinalLocally, STATE__Terminated
}
enum EReplyStatus { OK, RESTART, ALREADY, NODATA }
enum ESyncStatus { SyncDone, DisksSynced }
enum EScenario {
  StaleMetadata, DelayedSelfGuid, FalseFirstRun,
  NoWipes, RetainedSource, WipedWithWitnesses, FreshGroup
}
type TSyncGuidInfo = (State: ESyncGuidState, Guid: int);
type TLocalGuidInfo = (State: ELocalGuidState, Guid: int, DbBirthLsn: int);
type TSyncState = (Guid: int, SyncedLsn: int);
type TDecision = (Decision: EDecision, Guid: int, Step: EFirstRunStep);
type TVDisk = (Online: bool, Ready: bool, Local: TLocalGuidInfo,
  Incarnation: int, HullBlock: int, LogBlock: int, LogLsn: int,
  LogStartLsn: int, PeerGuidInfo: seq[TSyncGuidInfo], Cursors: seq[TSyncState]);
type TGuidReply = (VDiskId: int, Info: TSyncGuidInfo);
type TGuidEnvelope = (Target: machine, Owner: int, Reply: TGuidReply);
type TGuidRecovered = (Owner: int, Outcome: TDecision);
type TVSyncRequest = (Job: machine, Owner: int, Peer: int, SyncState: TSyncState);
type TVSyncResult = (Status: EReplyStatus, SyncState: TSyncState, Block: int);

event eAck;
event eStop: int;
event eWipe: int;
event eWriteBlock: int;
event eRunPropagators;
event eStartRecovery: int;
event eRecoveryReady: TGuidRecovered;
event eStartRound: int;
event eRoundFinished: (Owner: int, HullBlock: int, Peers: int);
event eSurvey: (Source: machine, Owner: int);
event eGuidEnvelope: TGuidEnvelope;
event TEvVDiskGuidObtained: TGuidReply;
event ePublishGuid: (Source: machine, Owner: int, Info: TSyncGuidInfo);
event TEvVDiskGuidWritten: set[int];
event TEvSyncerCommit: (Source: machine, Owner: int, Local: TLocalGuidInfo);
event TEvSyncerCommitDone;
event TEvVDiskGuidRecovered: TGuidRecovered;
event TEvVSync: TVSyncRequest;
event TEvVSyncResult: TVSyncResult;
event eFullSyncRequest: TVSyncRequest;
event eImportMetadata: (Job: machine, Owner: int, Peer: int, Block: int, SyncState: TSyncState);
event TEvLocalSyncDataResult;
event TEvSyncerJobDone: (Owner: int, Peer: int, SyncState: TSyncState, Status: ESyncStatus);
event eConfigureDelay: (Scenario: EScenario, Topology: ETopology);

// Observations only. The implementation never reads the monitors' history.
event eSeed: (Established: bool, Topology: ETopology);
event eScenarioStarted;
event eScenarioFinished;
event eGuidRecoveryStarted: (Owner: int, Local: TLocalGuidInfo);
event eSurveyVote: (Owner: int, Info: TSyncGuidInfo);
event eGuidVerdict: (Owner: int, Decision: EDecision);
event eLocalCommitted: (Owner: int, Local: TLocalGuidInfo);
event eBlockCommitted: int;
event eRoundStarted: int;
event eSyncPeerDone: (Owner: int, Peer: int, Status: ESyncStatus);
event eCallOsiris: int;
