#pragma once

#include "execution_unit.h"

namespace NKikimr {
namespace NDataShard {

THolder<TExecutionUnit> CreateCheckDataTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCheckWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateCheckSchemeTxUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateCheckSnapshotTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCheckDistributedEraseTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCheckCommitWritesTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateStoreDataTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateStoreWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateStoreSchemeTxUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateStoreSnapshotTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateStoreDistributedEraseTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateStoreCommitWritesTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateBuildAndWaitDependenciesUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateFinishProposeUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateFinishProposeWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateCompletedOperationsUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateWaitForPlanUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreatePlanQueueUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateLoadTxDetailsUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateLoadWriteDetailsUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateFinalizeDataTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateProtectSchemeEchoesUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateBuildDataTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateBuildDistributedEraseTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateBuildKqpDataTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateBuildWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateStoreAndSendOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateStoreAndSendWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreatePrepareDataTxInRSUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreatePrepareKqpDataTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreatePrepareWriteTxInRSUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreatePrepareDistributedEraseTxInRSUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateLoadAndWaitInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateExecuteDataTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateExecuteWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateExecuteKqpDataTxUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateExecuteDistributedEraseTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateExecuteCommitWritesTxUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCompleteOperationUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCompleteWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateExecuteKqpScanTxUnit(TDataShard& dataShard, TPipeline& pipeline);
THolder<TExecutionUnit> CreateMakeScanSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateWaitForStreamClearanceUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateReadTableScanUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateMakeSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateBuildSchemeTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreatePrepareSchemeTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateBackupUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateRestoreUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCreateTableUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateReceiveSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateReceiveSnapshotCleanupUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateAlterMoveShadowUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateAlterTableUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateDropTableUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateDirectOpUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCreatePersistentSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateDropPersistentSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCreateVolatileSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateDropVolatileSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateInitiateBuildIndexUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateFinalizeBuildIndexUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateDropIndexNoticeUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateMoveIndexUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateMoveTableUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCreateCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateAlterCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateDropCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateCheckReadUnit(TDataShard &dataShard, TPipeline &pipeline);
THolder<TExecutionUnit> CreateReadUnit(TDataShard &dataShard, TPipeline &pipeline);

} // namespace NDataShard
} // namespace NKikimr
