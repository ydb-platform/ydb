#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDbsController::TLoadState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDbsControllerActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDbsController::TLoadState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TDbsControllerActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxDbsController::TLoadState& args)
{
    Y_UNUSED(args);

    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] State loaded, DbsController is ready",
        TabletID());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
