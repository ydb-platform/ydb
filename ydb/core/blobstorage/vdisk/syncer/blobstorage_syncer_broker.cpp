#include "blobstorage_syncer_broker.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_log.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

namespace NKikimr {

    class TSyncBroker : public TActorBootstrapped<TSyncBroker> {
        TControlWrapper MaxInProgressSyncCount;

        std::unordered_map<TActorId, std::unordered_set<TActorId>> Active;

        struct TWaitSync {
            TActorId VDiskActorId;
            std::unordered_set<TActorId> ActorIds;
        };
        std::list<TWaitSync> WaitQueue; // TODO: better search

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNC_BROKER;
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvQuerySyncToken, Handle)
            hFunc(TEvReleaseSyncToken, Handle)
            hFunc(TEvents::TEvWakeup, Handle)
        )

        explicit TSyncBroker(const TControlWrapper& maxInProgressSyncCount)
            : MaxInProgressSyncCount(maxInProgressSyncCount)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc, TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
        }

        void Handle(TEvQuerySyncToken::TPtr& ev) {
            const auto vDiskActorId = ev->Get()->VDiskActorId;
            const auto actorId = ev->Sender;

            if (const auto it = Active.find(vDiskActorId); it != Active.end()) {
                it->second.insert(actorId);
                Send(actorId, new TEvSyncToken);

                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_SYNCER,
                    "TEvQuerySyncToken, VDisk actor id: " << vDiskActorId <<
                    ", actor id: " << actorId <<
                    ", token sent, active: " << Active.size() <<
                    ", waiting: " << WaitQueue.size());
                return;
            }

            const auto limit = (ui64)MaxInProgressSyncCount;

            if (!limit || Active.size() < limit) {
                Active[vDiskActorId].insert(actorId);
                Send(actorId, new TEvSyncToken);

                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_SYNCER,
                    "TEvQuerySyncToken, VDisk actor id: " << vDiskActorId <<
                    ", actor id: " << actorId <<
                    ", token sent, active: " << Active.size() <<
                    ", waiting: " << WaitQueue.size());
                return;
            }

            auto pred = [&vDiskActorId](const auto& item) {
                return item.VDiskActorId == vDiskActorId;
            };

            if (const auto it = std::find_if(WaitQueue.begin(), WaitQueue.end(), pred); it != WaitQueue.end()) {
                it->ActorIds.insert(actorId);

                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_SYNCER,
                    "TEvQuerySyncToken, VDisk actor id: " << vDiskActorId <<
                    ", actor id: " << actorId <<
                    ", enqueued, active: " << Active.size() <<
                    ", waiting: " << WaitQueue.size());
                return;
            }
            
            TWaitSync sync{vDiskActorId, {actorId}};
            WaitQueue.emplace_back(std::move(sync));

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_SYNCER,
                "TEvQuerySyncToken, VDisk actor id: " << vDiskActorId <<
                ", actor id: " << actorId <<
                ", enqueued, active: " << Active.size() <<
                ", waiting: " << WaitQueue.size());
        }

        void ProcessQueue() {
            const auto limit = (ui64)MaxInProgressSyncCount;

            while (!WaitQueue.empty() && (!limit || Active.size() < limit)) {
                const auto& waitSync = WaitQueue.front();
                for (const auto& actorId : waitSync.ActorIds) {
                    Send(actorId, new TEvSyncToken);
                }
                Active[waitSync.VDiskActorId] = std::move(waitSync.ActorIds);
                WaitQueue.pop_front();
            }
        }

        void Handle(TEvReleaseSyncToken::TPtr& ev) {
            const auto vDiskActorId = ev->Get()->VDiskActorId;
            const auto actorId = ev->Sender;

            if (const auto it = Active.find(vDiskActorId); it != Active.end()) {
                it->second.erase(actorId);
                if (it->second.empty()) {
                    Active.erase(vDiskActorId);
                    ProcessQueue();
                }

                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_SYNCER,
                    "TEvReleaseSyncToken, VDisk actor id: " << vDiskActorId <<
                    ", actor id: " << actorId <<
                    ", token released, active: " << Active.size() <<
                    ", waiting: " << WaitQueue.size());
                return;
            }

            auto pred = [&vDiskActorId](const auto& item) {
                return item.VDiskActorId == vDiskActorId;
            };

            if (const auto it = std::find_if(WaitQueue.begin(), WaitQueue.end(), pred); it != WaitQueue.end()) {
                it->ActorIds.erase(actorId);
                if (it->ActorIds.empty()) {
                    WaitQueue.erase(it);
                }

                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_SYNCER,
                    "TEvReleaseSyncToken, VDisk actor id: " << vDiskActorId <<
                    ", actor id: " << actorId <<
                    ", removed from queue, active: " << Active.size() <<
                    ", waiting: " << WaitQueue.size());
            }
        }

        void Handle(TEvents::TEvWakeup::TPtr&) {
            ProcessQueue();
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
        }
    };

    IActor *CreateSyncBrokerActor(const TControlWrapper& maxInProgressSyncCount) {
        return new TSyncBroker(maxInProgressSyncCount);
    }

} // NKikimr
