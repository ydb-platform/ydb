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

#include <ydb/library/yql/utils/yql_panic.h>


namespace NKikimr::NKqp::NRm {

namespace {

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)

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
                LOG_C("Unexpected event type: " << ev->GetTypeRewrite()
                    << ", event: " << ev->GetTypeName());
        }
    }

    void HandleWait(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BoardLookupId = {};

        TEvStateStorage::TEvBoardInfo* event = ev->Get();
        TVector<NKikimrKqp::TKqpNodeResources> resources;

        if (event->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            LOG_I("WhiteBoard entries: " << event->InfoEntries.size());
            resources.resize(event->InfoEntries.size());

            int i = 0;
            for (auto& [_, entry] : event->InfoEntries) {
                Y_PROTOBUF_SUPPRESS_NODISCARD resources[i].ParseFromString(entry.Payload);
                LOG_D("WhiteBoard [" << i << "]: " << resources[i].ShortDebugString());
                i++;
            }
        } else {
            LOG_E("WhiteBoard error: " << (int) event->Status << ", path: " << event->Path);
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
