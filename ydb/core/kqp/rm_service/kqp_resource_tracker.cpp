#include "kqp_rm_service.h"

#include <ydb/core/base/statestorage.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/kqp/common/kqp.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <yql/essentials/utils/yql_panic.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_RESOURCE_MANAGER


namespace NKikimr::NKqp::NRm {

namespace {

using namespace NKikimr;
using namespace NActors;


class TTakeResourcesSnapshotActor : public TActorBootstrapped<TTakeResourcesSnapshotActor> {
public:
    TTakeResourcesSnapshotActor(const TString& boardPath,
        TOnResourcesSnapshotCallback&& callback)
        : BoardPath(boardPath)
        , Callback(std::move(callback)) {}

    void Bootstrap() {
        auto boardLookup = CreateBoardLookupActor(BoardPath, SelfId(), EBoardLookupMode::Majority);
        BoardLookupId = Register(boardLookup);

        Become(&TTakeResourcesSnapshotActor::WorkState);
    }

    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, HandleWait);
            cFunc(TEvents::TSystem::Poison, PassAway);
            default:
                YDB_LOG_CRIT("Unexpected event",
                    {"type", ev->GetTypeRewrite()},
                    {"event", ev->GetTypeName()});
        }
    }

    void HandleWait(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BoardLookupId = {};

        TEvStateStorage::TEvBoardInfo* event = ev->Get();
        TVector<NKikimrKqp::TKqpNodeResources> resources;

        if (event->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            YDB_LOG_INFO("WhiteBoard",
                {"entries", event->InfoEntries.size()});
            resources.resize(event->InfoEntries.size());

            int i = 0;
            for (auto& [_, entry] : event->InfoEntries) {
                Y_PROTOBUF_SUPPRESS_NODISCARD resources[i].ParseFromString(entry.Payload);
                YDB_LOG_DEBUG("WhiteBoard [",
                    {"i", i},
                    {"]", resources[i].ShortDebugString()});
                i++;
            }
        } else {
            YDB_LOG_ERROR("WhiteBoard",
                {"error", (int) event->Status},
                {"path", event->Path});
        }

        Callback(std::move(resources));

        PassAway();
    }

    void PassAway() {
        if (BoardLookupId) {
            Send(BoardLookupId, new TEvents::TEvPoison);
        }
        IActor::PassAway();
    }

private:
    const TString BoardPath;
    TOnResourcesSnapshotCallback Callback;
    TActorId BoardLookupId;
};

} // namespace

NActors::IActor* CreateTakeResourcesSnapshotActor(
    const TString& boardPath, TOnResourcesSnapshotCallback&& callback)
{
    return new TTakeResourcesSnapshotActor(boardPath, std::move(callback));
}

} // namespace NKikimr::NKqp::NRm
