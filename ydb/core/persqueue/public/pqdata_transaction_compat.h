#pragma once

#include <ydb/core/protos/pqdata_transaction.pb.h>

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NKikimr::NPQ {

bool HasCanonical(const NKikimrPQ::TWriteId& writeId);
void UpgradeFromLegacy(NKikimrPQ::TWriteId& writeId);
void DowngradeToLegacy(NKikimrPQ::TWriteId& writeId);
void EnsureCanonical(NKikimrPQ::TWriteId& writeId);

bool HasCanonical(const NKikimrPQ::TPartitionOperation& op);
void UpgradeFromLegacy(NKikimrPQ::TPartitionOperation& op);
void DowngradeToLegacy(NKikimrPQ::TPartitionOperation& op);
void EnsureCanonical(NKikimrPQ::TPartitionOperation& op);

void EnsureCanonical(NKikimrPQ::TDataTransaction& tx);
void DowngradeToLegacy(NKikimrPQ::TDataTransaction& tx);

bool IsReadTxOperation(const NKikimrPQ::TPartitionOperation& operation);
bool IsWriteTxOperation(const NKikimrPQ::TPartitionOperation& operation);
bool IsKafkaWriteOperation(const NKikimrPQ::TPartitionOperation& operation);
bool IsDeferredPublicationFinalizeOperation(const NKikimrPQ::TPartitionOperation& operation);
TMaybe<NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp> GetDeferredPublicationFinalizeOp(
    const NKikimrPQ::TPartitionOperation& operation);

bool HasTopicReadCommit(const NKikimrPQ::TPartitionOperation& operation);
bool HasKafkaReadCommit(const NKikimrPQ::TPartitionOperation& operation);

bool GetSkipConflictCheck(const NKikimrPQ::TPartitionOperation& operation);
ui32 GetSupportivePartition(const NKikimrPQ::TPartitionOperation& operation);

TString GetReadConsumer(const NKikimrPQ::TPartitionOperation& operation);
ui64 GetReadCommitOffsetsBegin(const NKikimrPQ::TPartitionOperation& operation);
ui64 GetReadCommitOffsetsEnd(const NKikimrPQ::TPartitionOperation& operation);
bool GetReadForceCommit(const NKikimrPQ::TPartitionOperation& operation);
bool GetReadKillReadSession(const NKikimrPQ::TPartitionOperation& operation);
bool GetReadOnlyCheckCommitedToFinish(const NKikimrPQ::TPartitionOperation& operation);
TString GetReadSessionId(const NKikimrPQ::TPartitionOperation& operation);

} // namespace NKikimr::NPQ
