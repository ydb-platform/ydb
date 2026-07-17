#include "datashard_txs.h"

#include <util/string/vector.h>

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

TDataShard::TTxStoreTablePath::TTxStoreTablePath(TDataShard *self, ui64 pathId, const TString &path)
    : TBase(self)
    , PathId(pathId)
    , Path(path)
{
}

bool TDataShard::TTxStoreTablePath::Execute(TTransactionContext &txc, const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxStoreTablePath::Execute at " << Self->TabletID());

    Y_ENSURE(Self->TableInfos.contains(PathId));

    txc.DB.NoMoreReadsForTx();

    TUserTable::TPtr copy = new TUserTable(*Self->TableInfos.at(PathId));
    copy->SetPath(Path);

    NIceDb::TNiceDb db(txc.DB);
    Self->PersistUserTable(db, PathId, *copy);
    Self->AddUserTable(TPathId(Self->GetPathOwnerId(), PathId), copy);

    return true;
}

void TDataShard::TTxStoreTablePath::Complete(const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxStoreTablePath::Complete at " << Self->TabletID());
}

}}
