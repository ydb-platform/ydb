#pragma once
#include "defs.h"

namespace arrow {
    class RecordBatch;
}

namespace Ydb {
    class Type;
}

namespace NKikimr::NTxProxy {

using TUploadTypes = TVector<std::pair<TString, Ydb::Type>>;

IActor* CreateUploadColumnsInternal(const TActorId& sender,
                                    const TString& table,
                                    std::shared_ptr<TUploadTypes> types,
                                    std::shared_ptr<arrow::RecordBatch> data,
                                    ui64 cookie = 0);

} // namespace NKikimr::NTxProxy
