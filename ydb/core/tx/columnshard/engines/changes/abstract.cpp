#include "abstract.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>

namespace NKikimr::NOlap {

TString TColumnEngineChanges::DebugString() const {
    TStringStream sb;
    sb << TypeString() << ":";
    DoDebugString(sb);
    return sb.Str();
}

TWriteIndexContext::TWriteIndexContext(NTabletFlatExecutor::TTransactionContext& txc, IDbWrapper& dbWrapper)
    : Txc(txc)
    , BlobManagerDb(std::make_shared<NColumnShard::TBlobManagerDb>(txc.DB))
    , DBWrapper(dbWrapper)
{

}

}
