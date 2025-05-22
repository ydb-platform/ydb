#pragma once

#include "defs.h"
#include "blob_recovery.h"

namespace NKikimr {

    class TBlobRecoveryActor : public TActorBootstrapped<TBlobRecoveryActor> {
        const TIntrusivePtr<TVDiskContext> VCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const ::NMonitoring::TDynamicCounterPtr Counters;
        const TString LogPrefix;

    public:
        TBlobRecoveryActor(TIntrusivePtr<TVDiskContext> vctx, TIntrusivePtr<TBlobStorageGroupInfo> info,
                ::NMonitoring::TDynamicCounterPtr counters)
            : VCtx(std::move(vctx))
            , Info(std::move(info))
            , Counters(counters)
            , LogPrefix(VCtx->VDiskLogPrefix)
        {}

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_BLOB_RECOVERY_ACTOR;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // actor state function

        void Bootstrap();
        void PassAway() override;

        STRICT_STFUNC(StateFunc,
            hFunc(TEvVGenerationChange, Handle);
            hFunc(TEvProxyQueueState, Handle);
            hFunc(TEvRecoverBlob, Handle);
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // BS_QUEUE management

        struct TQueueInfo {
            const TActorId QueueActorId;
            bool IsConnected = false;
        };
        std::unordered_map<TVDiskIdShort, TQueueInfo, THash<TVDiskIdShort>> Queues;
        bool IsConnected = false;

        void StartQueues();
        void StopQueues();
        void Handle(TEvVGenerationChange::TPtr ev);
        void Handle(TEvProxyQueueState::TPtr ev);
        void EvaluateConnectionQuorum();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // request processing

        struct TInFlightContext;
        using TInFlightContextPtr = std::shared_ptr<TInFlightContext>;
        using TInFlight = std::multimap<TInstant, TInFlightContextPtr>; // deadline -> context map
        struct TInFlightContext {
            TInFlight::iterator Iterator; // pointing to this record in InFlight map
            const ui64 RequestId; // identifier of a request (for debugging purposes)
            const TActorId Sender; // sender of an TEvRecoverBlob
            const ui64 Cookie; // cookie with the original request
            std::unique_ptr<TEvRecoverBlobResult> Result; // pending response message
            ui32 NumUnrespondedBlobs = 0; // number of blobs with UNKNOWN status

            TInFlightContext(ui64 requestId, TEventHandle<TEvRecoverBlob>& ev)
                : RequestId(requestId)
                , Sender(ev.Sender)
                , Cookie(ev.Cookie)
                , Result(std::make_unique<TEvRecoverBlobResult>())
            {}

            void SendResult(const TActorIdentity& self) {
                self.Send(Sender, Result.release(), 0, Cookie);
            }
        };
        TInFlight InFlight;
        ui64 NextRequestId = 1;
        bool WakeupScheduled = false;

        void Handle(TEvRecoverBlob::TPtr ev);
        void HandleWakeup();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDisk query processing

        struct TQuery {
            std::unique_ptr<TEvBlobStorage::TEvVGet> VGet; // currently filled in
            std::deque<std::unique_ptr<TEvBlobStorage::TEvVGet>> Pending; // already filled in and ready
            ui32 WorstReplySize = 0;
        };
        std::map<TVDiskID, TQuery> Queries;

        // a map to fill upon receiving VGet result
        struct TPerBlobInfo {
            std::weak_ptr<TInFlightContext> Context;
            TEvRecoverBlobResult::TItem *Item; // item to update
            ui32 BlobReplyCounter = 0; // number of unreplied queries for this blob
        };
        std::unordered_multimap<TLogoBlobID, TPerBlobInfo, THash<TLogoBlobID>> VGetResultMap;
        std::set<std::tuple<TVDiskIdShort, TLogoBlobID>> GetsInFlight;

        void AddBlobQuery(const TLogoBlobID& id, NMatrix::TVectorType needed, const std::shared_ptr<TInFlightContext>& context, TEvRecoverBlobResult::TItem *item);
        void AddExtremeQuery(const TVDiskID& vdiskId, const TLogoBlobID& id, TInstant deadline, ui32 idxInSubgroup);
        void SendPendingQueries();
        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev);
        NKikimrProto::EReplyStatus ProcessItemData(TEvRecoverBlobResult::TItem& item);
    };


} // NKikimr
