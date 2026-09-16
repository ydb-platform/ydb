#pragma once

#include <ydb/core/protos/tx_columnshard.pb.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {

namespace NColumnShard {

inline Ydb::StatusIds::StatusCode ConvertToYdbStatus(NKikimrTxColumnShard::EResultStatus columnShardStatus) {
    switch (columnShardStatus) {
        case NKikimrTxColumnShard::UNSPECIFIED:
            return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;

        case NKikimrTxColumnShard::PREPARED:
        case NKikimrTxColumnShard::SUCCESS:
            return Ydb::StatusIds::SUCCESS;

        case NKikimrTxColumnShard::ABORTED:
            return Ydb::StatusIds::ABORTED;

        case NKikimrTxColumnShard::ERROR:
            return Ydb::StatusIds::GENERIC_ERROR;

        case NKikimrTxColumnShard::TIMEOUT:
            return Ydb::StatusIds::TIMEOUT;

        case NKikimrTxColumnShard::SCHEMA_ERROR:
        case NKikimrTxColumnShard::SCHEMA_CHANGED:
            return Ydb::StatusIds::SCHEME_ERROR;

        case NKikimrTxColumnShard::OVERLOADED:
            return Ydb::StatusIds::OVERLOADED;

        case NKikimrTxColumnShard::STORAGE_ERROR:
            return Ydb::StatusIds::UNAVAILABLE;

        default:
            return Ydb::StatusIds::GENERIC_ERROR;
    }
}
}   // namespace NColumnShard

}   // namespace NKikimr
