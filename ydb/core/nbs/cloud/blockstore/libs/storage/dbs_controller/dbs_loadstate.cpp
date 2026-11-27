#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::DBS_CONTROLLER

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

    YDB_LOG_INFO_CTX(ctx, "State loaded, DbsController is ready",
        {"tabletID", TabletID()});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
