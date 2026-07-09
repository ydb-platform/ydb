#pragma once

#include <ydb/core/protos/pqdata_transaction.pb.h>

namespace NKikimr::NPQ {

bool HasCanonical(const NKikimrPQ::TWriteId& writeId);
void UpgradeFromLegacy(NKikimrPQ::TWriteId& writeId);
void DowngradeToLegacy(NKikimrPQ::TWriteId& writeId);
void EnsureCanonical(NKikimrPQ::TWriteId& writeId);

bool HasCanonical(const NKikimrPQ::TPartitionOperation& op);
void UpgradeFromLegacy(NKikimrPQ::TPartitionOperation& op);
void DowngradeToLegacy(NKikimrPQ::TPartitionOperation& op);
void EnsureCanonical(NKikimrPQ::TPartitionOperation& op);

} // namespace NKikimr::NPQ
