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
};

IActor* CreateUploadRowsInternal(const TActorId& sender,
                                 const TString& table,
                                 std::shared_ptr<TVector<std::pair<TString, Ydb::Type> > > types,
                                 std::shared_ptr<TVector<std::pair<TSerializedCellVec, TString> > > rows,
                                 EUploadRowsMode mode = EUploadRowsMode::Normal,
                                 bool writeToPrivateTable = false);
} // namespace NTxProxy
} // namespace NKikimr
