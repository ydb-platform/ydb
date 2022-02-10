#pragma once
#include "defs.h"

#include <ydb/core/tablet_flat/flat_scan_iface.h>

namespace NKikimr {
namespace NDataShard {

class TReadTableProd : public IDestructable {
public:
    TReadTableProd(const TString &error, bool schemaChanged = false)
        : Error(error)
        , SchemaChanged(schemaChanged)
    {}

    TString Error;
    bool SchemaChanged;
};

TAutoPtr<NTable::IScan> CreateReadTableScan(ui64 txId,
                                        ui64 shardId,
                                        TUserTable::TCPtr tableInfo,
                                        const NKikimrTxDataShard::TReadTableTransaction &tx,
                                        TActorId sink,
                                        TActorId dataShard);

} // namespace NDataShard
} // namespace NKikimr
