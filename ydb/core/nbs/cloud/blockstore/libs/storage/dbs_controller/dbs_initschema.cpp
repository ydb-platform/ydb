#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDbsController::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDbsControllerActor::ExecuteInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDbsController::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);
    db.InitSchema();
}

void TDbsControllerActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxDbsController::TInitSchema& args)
{
    Y_UNUSED(args);

    ExecuteTx(ctx, CreateTx<TLoadState>());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
