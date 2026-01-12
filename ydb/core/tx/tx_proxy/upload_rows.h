#pragma once
#include "defs.h"

#include <ydb/core/util/backoff.h>

namespace Ydb {
    class Type;
}

namespace NKikimr {

class TSerializedCellVec;

namespace NTxProxy {

enum class EUploadRowsMode {
    Normal,
    WriteToTableShadow,
    UpsertIfExists,
};

using TUploadTypes = TVector<std::pair<TString, Ydb::Type>>;
using TUploadRows = TVector<std::pair<TSerializedCellVec, TString>>;

IActor* CreateUploadRowsInternal(const TActorId& sender,
                                 const TString& database,
                                 const TString& table,
                                 std::shared_ptr<const TUploadTypes> types,
                                 std::shared_ptr<const TUploadRows> rows,
                                 EUploadRowsMode mode = EUploadRowsMode::Normal,
                                 bool writeToPrivateTable = false,
                                 bool writeToIndexImplTable = false,
                                 ui64 cookie = 0,
                                 TBackoff backoff = TBackoff(0));
} // namespace NTxProxy
} // namespace NKikimr
