#pragma once
#include "defs.h"
#include "datashard_user_table.h"

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tablet_flat/flat_scan_iface.h>

namespace Ydb {
class ResultSet;
}

namespace NKikimr {
namespace NDataShard {

class TReadTableProd : public IDestructable {
public:
    TReadTableProd(const TString& error, bool IsFatalError, bool schemaChanged)
        : Error(error)
        , IsFatalError(IsFatalError)
        , SchemaChanged(schemaChanged)
    {
    }

    TString Error;
    bool IsFatalError;
    bool SchemaChanged;
};

bool AddRowToYdbResultSet(
    Ydb::ResultSet& resultSet,
    TConstArrayRef<NScheme::TTypeInfo> types,
    TConstArrayRef<TCell> cells,
    TString& error);

TAutoPtr<NTable::IScan> CreateReadTableScan(ui64 txId,
                                        ui64 shardId,
                                        TUserTable::TCPtr tableInfo,
                                        const NKikimrTxDataShard::TReadTableTransaction &tx,
                                        TActorId sink,
                                        TActorId dataShard);

} // namespace NDataShard
} // namespace NKikimr
