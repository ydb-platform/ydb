#pragma once

#include <ydb/core/engine/minikql/change_collector_iface.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;
struct TUserTable;

NMiniKQL::IChangeCollector* CreateChangeCollector(TDataShard& dataShard, NTable::TDatabase& db, const TUserTable& table, bool isImmediateTx);
NMiniKQL::IChangeCollector* CreateChangeCollector(TDataShard& dataShard, NTable::TDatabase& db, ui64 tableId, bool isImmediateTx);

} // NDataShard
} // NKikimr
