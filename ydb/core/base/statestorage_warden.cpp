#include "statestorage_impl.h"
#include "tabletid.h"
#include "appdata.h"
#include "tablet.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/map.h>
#include <util/generic/hash_set.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)

namespace NKikimr {

class TStateStorageWarden : public TActorBootstrapped<TStateStorageWarden> {
    const ui32 GroupId; // for convenience

    TIntrusivePtr<TStateStorageInfo> StateStorageInfo;
    TIntrusivePtr<TStateStorageInfo> BoardInfo;
    TIntrusivePtr<TStateStorageInfo> SchemeBoardInfo;

    template<typename TCreateFunc>
    bool UpdateReplicaInfo(TIntrusivePtr<TStateStorageInfo> &updated, TIntrusivePtr<TStateStorageInfo> &current, TCreateFunc createFunc) {
        // updates current to updated, kills unused replicas, starts new
        // returns true is smth changed
        const ui32 selfNode = SelfId().NodeId();
        TActorSystem *sys = TlsActivationContext->ExecutorThread.ActorSystem;
        const ui32 sysPoolId = AppData()->SystemPoolId;

        bool hasChanges = false;

        const bool checkRings = (updated->Rings.size() == current->Rings.size()) && (updated->NToSelect == current->NToSelect);
        ui32 ringIdx = 0;
        ui32 index = 0;
        for (const ui32 ringSz = updated->Rings.size(); ringIdx < ringSz; ++ringIdx) {
            const auto &replicas = updated->Rings[ringIdx].Replicas;
            const bool checkReplicas = checkRings && (replicas.size() == current->Rings[ringIdx].Replicas.size());

            ui32 replicaIdx = 0;
            for (const ui32 replicaSz = replicas.size(); replicaIdx < replicaSz; ++replicaIdx, ++index) {
                // keep previous
                if (checkReplicas && replicas[replicaIdx] == current->Rings[ringIdx].Replicas[replicaIdx])
                    continue;

                hasChanges = true;

                // should kill?
                if (ringIdx < current->Rings.size() && replicaIdx < current->Rings[ringIdx].Replicas.size()) {
                    const TActorId outdated = current->Rings[ringSz].Replicas[replicaIdx];
                    if (outdated.NodeId() == selfNode) {
                        sys->RegisterLocalService(outdated, TActorId());
                        Send(outdated, new TEvents::TEvPoison());
                    }
                }

                // must start?
                if (replicas[replicaIdx].NodeId() == selfNode) {
                    const TActorId replicaActorId = Register(createFunc(updated.Get(), index), TMailboxType::ReadAsFilled, sysPoolId);
                    sys->RegisterLocalService(replicas[replicaIdx], replicaActorId);
                }
            }
        }

        if (hasChanges)
            current = updated;

        return hasChanges;
    }

    void UpdateConfig(const NKikimrConfig::TDomainsConfig::TStateStorage &config) {
        TIntrusivePtr<TStateStorageInfo> stateStorageInfo;
        TIntrusivePtr<TStateStorageInfo> boardInfo;
        TIntrusivePtr<TStateStorageInfo> schemeBoardInfo;

        BuildStateStorageInfos(config, stateStorageInfo, boardInfo, schemeBoardInfo);

        bool hasChanges = false;

        hasChanges |= UpdateReplicaInfo(stateStorageInfo, StateStorageInfo, CreateStateStorageReplica);
        hasChanges |= UpdateReplicaInfo(boardInfo, BoardInfo, CreateStateStorageBoardReplica);
        hasChanges |= UpdateReplicaInfo(schemeBoardInfo, SchemeBoardInfo, CreateSchemeBoardReplica);

        // Update proxy config
        if (hasChanges) {
            Send(MakeStateStorageProxyID(GroupId), new TEvStateStorage::TEvUpdateGroupConfig(stateStorageInfo, boardInfo, schemeBoardInfo));
        }
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev) {
        const auto &record = ev->Get()->Record;

        if (!record.GetConfig().HasDomainsConfig())
            return;

        for (const NKikimrConfig::TDomainsConfig::TStateStorage &config : record.GetConfig().GetDomainsConfig().GetStateStorage()) {
            if (config.GetSSId() == GroupId) {
                UpdateConfig(config);
                break;
            }
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY;
    }

    TStateStorageWarden(const TIntrusivePtr<TStateStorageInfo> &info, const TIntrusivePtr<TStateStorageInfo> &board, const TIntrusivePtr<TStateStorageInfo> &schemeBoard)
        : GroupId(info->StateStorageGroup)
        , StateStorageInfo(info)
        , BoardInfo(board)
        , SchemeBoardInfo(schemeBoard)
    {
        Y_VERIFY(GroupId == board->StateStorageGroup);
        Y_VERIFY(GroupId == schemeBoard->StateStorageGroup);
    }

    void Bootstrap()
    {
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(NKikimrConsole::TConfigItem::DomainsConfigItem));
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        }
    }
};

IActor* CreateStateStorageWarden(const TIntrusivePtr<TStateStorageInfo> &info, const TIntrusivePtr<TStateStorageInfo> &board, const TIntrusivePtr<TStateStorageInfo> &schemeBoard) {
    return new TStateStorageWarden(info, board, schemeBoard);
}

}
