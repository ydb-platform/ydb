#pragma once
#include "defs.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr {
namespace NTxProxy {

    enum class EReadTableFormat {
        OldResultSet,
        YdbResultSet,
        YdbResultSetWithNotNullSupport
    };

    struct TReadTableSettings {
        TActorId Owner;
        ui64 Cookie = 0;
        ui64 ProxyFlags = 0;
        TString DatabaseName;
        TString TablePath;
        TVector<TString> Columns;
        Ydb::Table::KeyRange KeyRange;
        ui64 MaxRows = Max<ui64>();
        TRowVersion ReadVersion = TRowVersion::Max();
        TString UserToken;
        EReadTableFormat DataFormat = EReadTableFormat::YdbResultSet;
        bool Ordered = false;
        bool RequireResultSet = false;
        ui64 MaxBatchSizeBytes = Max<ui64>();
        ui64 MaxBatchSizeRows = Max<ui64>();
    };

    IActor* CreateReadTableSnapshotWorker(const TReadTableSettings& settings);

} // namespace NTxProxy
} // namespace NKikimr
