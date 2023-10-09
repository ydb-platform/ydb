#pragma once
#include "defs.h"

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
                                 const TString& table,
                                 std::shared_ptr<TUploadTypes> types,
                                 std::shared_ptr<TUploadRows> rows,
                                 EUploadRowsMode mode = EUploadRowsMode::Normal,
                                 bool writeToPrivateTable = false);
} // namespace NTxProxy
} // namespace NKikimr
