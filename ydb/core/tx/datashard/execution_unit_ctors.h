#pragma once

#include "execution_unit.h"

namespace NKikimr {
namespace NDataShard {

std::unique_ptr<TExecutionUnit> CreateCheckDataTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCheckWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateCheckSchemeTxUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateCheckSnapshotTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCheckDistributedEraseTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCheckCommitWritesTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreDataTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreSchemeTxUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreSnapshotTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreDistributedEraseTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreCommitWritesTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateBuildAndWaitDependenciesUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateFinishProposeUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateFinishProposeWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateCompletedOperationsUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateWaitForPlanUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreatePlanQueueUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateLoadTxDetailsUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateLoadWriteDetailsUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateFinalizeDataTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateProtectSchemeEchoesUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateBuildDataTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateBuildDistributedEraseTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateBuildWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreAndSendOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateStoreAndSendWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreatePrepareDataTxInRSUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreatePrepareWriteTxInRSUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreatePrepareDistributedEraseTxInRSUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateLoadAndWaitInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateLoadInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateBlockFailPointUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateExecuteDataTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateExecuteWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateExecuteDistributedEraseTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateExecuteCommitWritesTxUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCompleteOperationUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCompleteWriteUnit(TDataShard& dataShard, TPipeline& pipeline);
std::unique_ptr<TExecutionUnit> CreateMakeScanSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateWaitForStreamClearanceUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateReadTableScanUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateMakeSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateBuildSchemeTxOutRSUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreatePrepareSchemeTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateBackupUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateRestoreUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCreateTableUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateReceiveSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateReceiveSnapshotCleanupUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateAlterMoveShadowUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateAlterTableUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateDropTableUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateDirectOpUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCreatePersistentSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateDropPersistentSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCreateVolatileSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateDropVolatileSnapshotUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateInitiateBuildIndexUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreatePrepareIndexValidationUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateFinalizeBuildIndexUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateDropIndexNoticeUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateMoveIndexUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateMoveTableUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCreateCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateAlterCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateDropCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateRotateCdcStreamUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateCheckReadUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateReadUnit(TDataShard &dataShard, TPipeline &pipeline);
std::unique_ptr<TExecutionUnit> CreateTruncateUnit(TDataShard &dataShard, TPipeline &pipeline);

} // namespace NDataShard
} // namespace NKikimr
