#include "blobstorage_hullcompactbroker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <util/generic/hash_set.h>
#include <util/generic/queue.h>

namespace NKikimr {

    struct TCompactionRequest{
        TVDiskIdShort VDiskId;
        TActorId ActorId;
        double Priority;
        TMonotonic RequestTime;

        TCompactionRequest(const TVDiskIdShort vDiskId, const TActorId actorId, const double priority)
            : VDiskId(vDiskId)
            , ActorId(actorId)
            , Priority(priority)
            , RequestTime(TMonotonic::Now())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionRequest VDiskId# " << VDiskId << " ActorId# " << ActorId << " Priority# " << Priority << " RequestTime# " << RequestTime << " WaitTimeMs# " << (TMonotonic::Now() - RequestTime).MilliSeconds() << "}";
            return str.Str();
        }
    };

    struct TCompactionInfo{
        TString PDiskId;
        TVDiskIdShort VDiskId;
        TActorId ActorId;
        TCompactionTokenId Token;
        TMonotonic StartTime;

        TCompactionInfo(const TString& pdiskId, const TVDiskIdShort& vdiskId, const TActorId actorId, const TCompactionTokenId token)
            : PDiskId(pdiskId)
            , VDiskId(vdiskId)
            , ActorId(actorId)
            , Token(token)
            , StartTime(TMonotonic::Now())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionInfo PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " ActorId# " << ActorId << " Token# " << Token << " StartTime# " << StartTime << " WorkingTimeMs# " << (TMonotonic::Now() - StartTime).MilliSeconds() << "}";
            return str.Str();
        }
    };

    struct TCompactionQueue {
        ui64 MaxActiveCompactions = 1;

        struct TCompactionRequests : public THashMap<TVDiskIdShort, TCompactionRequest> {
            TString ToString() const {
                TStringStream str;
                str << "{Size# [" << size();
                for (const auto& [vdiskId, request] : *this) {
                    str << " {VDiskId# " << vdiskId << " Request# " << request.ToString() << "},";
                }
                str << "]}";
                return str.Str();
            }
        };

        struct TCompactionsInfo : public THashMap<TVDiskIdShort, TCompactionInfo> {
            TString ToString() const {
                TStringStream str;
                str << "{Size# [" << size();
                for (const auto& [vdiskId, info] : *this) {
                    str << " {VDiskId# " << vdiskId << " Info# " << info.ToString() << "},";
                }
                str << "]}";
                return str.Str();
            }
        };

        using TCompactionsToken = THashMap<TCompactionTokenId, TVDiskIdShort>;

        TCompactionRequests PendingCompactions;
        TCompactionsToken CompactionsToken;
        TCompactionsInfo CompactionsInfo;

        TCompactionQueue(ui64 maxActiveCompactions = 1)
            : MaxActiveCompactions(maxActiveCompactions)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionQueue MaxActiveCompactions# " << MaxActiveCompactions << " PendingCompactions# " << PendingCompactions.ToString() << " ActiveCompactions# " << CompactionsInfo.ToString() << "}";
            return str.Str();
        }

        void RequestCompactionToken(const TVDiskIdShort& vdiskId, const TActorId& actorId, double priority) {
            if (CompactionsInfo.find(vdiskId) != CompactionsInfo.end()) {
                Y_ABORT_UNLESS(CompactionsInfo.at(vdiskId).ActorId != actorId);
                Y_ABORT_UNLESS(CompactionsToken.find(CompactionsInfo.at(vdiskId).Token) != CompactionsToken.end());
                Y_ABORT_UNLESS(PendingCompactions.find(vdiskId) == PendingCompactions.end());
                CompactionsToken.erase(CompactionsInfo.at(vdiskId).Token);
                CompactionsInfo.erase(vdiskId);
            }

            PendingCompactions.emplace(vdiskId, TCompactionRequest(vdiskId, actorId, priority));
        }

        void UpdateRequestCompactionToken(const TVDiskIdShort& vdiskId, const TActorId& actorId, double priority) {
            if (CompactionsInfo.find(vdiskId) != CompactionsInfo.end()) {
                Y_ABORT_UNLESS(CompactionsInfo.at(vdiskId).ActorId == actorId);
                Y_ABORT_UNLESS(PendingCompactions.find(vdiskId) == PendingCompactions.end());
                return;
            }
            Y_ABORT_UNLESS(PendingCompactions.find(vdiskId) != PendingCompactions.end());
            Y_ABORT_UNLESS(PendingCompactions.at(vdiskId).ActorId == actorId);
            PendingCompactions.at(vdiskId).Priority = priority;
        }

        void ReleaseCompactionToken(const TVDiskIdShort& vdiskId, const TActorId& actorId, TCompactionTokenId token) {
            Y_ABORT_UNLESS(CompactionsToken.find(token) != CompactionsToken.end());
            Y_ABORT_UNLESS(CompactionsToken[token] == vdiskId);
            Y_ABORT_UNLESS(CompactionsInfo.find(vdiskId) != CompactionsInfo.end());
            Y_ABORT_UNLESS(CompactionsInfo.at(vdiskId).ActorId == actorId);

            CompactionsToken.erase(token);
            CompactionsInfo.erase(vdiskId);
            PendingCompactions.erase(vdiskId);
        }

        TMaybe<TCompactionInfo> StartNewCompaction(const TString& pdiskId, TCompactionTokenId token) {
            if (CompactionsInfo.size() >= MaxActiveCompactions) {
                return Nothing();
            }
            if (PendingCompactions.empty()) {
                return Nothing();
            }

            auto it = std::max_element(PendingCompactions.begin(), PendingCompactions.end(),
                    [](const auto& lhs, const auto& rhs) {
                        return lhs.second.Priority < rhs.second.Priority;
                    });
            Y_ABORT_UNLESS(it != PendingCompactions.end());
            const TVDiskIdShort vdiskId = it->first;
            const TCompactionRequest& request = it->second;
            CompactionsInfo.emplace(vdiskId, TCompactionInfo(pdiskId, request.VDiskId, request.ActorId, token));
            CompactionsToken[token] = vdiskId;
            PendingCompactions.erase(it);
            return CompactionsInfo.at(vdiskId);
        }
    };

    struct TPDiskCompactions {
        THashMap<TString, TCompactionQueue> CompactionsPerPDisk;

        TString ToString() const {
            TStringStream str;
            str << "{TPDiskCompactions [";
            for (const auto& [pdiskId, compactionQueue] : CompactionsPerPDisk) {
                str << " {PDiskId# " << pdiskId << " Compactions# " << compactionQueue.ToString() << "},";
            }
            str << "{TPDiskCompactions ]";
            return str.Str();
        }

        void RequestCompactionToken(const TString& pdiskId, const TVDiskIdShort& vdiskId, const TActorId& actorId, double priority) {
            return CompactionsPerPDisk[pdiskId].RequestCompactionToken(vdiskId, actorId, priority);
        }

        void UpdateRequestCompactionToken(const TString& pdiskId, const TVDiskIdShort& vdiskId, const TActorId& actorId, double priority) {
            return CompactionsPerPDisk[pdiskId].UpdateRequestCompactionToken(vdiskId, actorId, priority);
        }

        void ReleaseCompactionToken(const TString& pdiskId, const TVDiskIdShort& vdiskId, const TActorId& actorId, TCompactionTokenId token) {
            return CompactionsPerPDisk[pdiskId].ReleaseCompactionToken(vdiskId, actorId, token);
        }

        TMaybe<TCompactionInfo> StartNewCompaction(TCompactionTokenId token) {
            for (auto& [pdiskId, compactionQueue] : CompactionsPerPDisk) {
                auto compactionInfo = compactionQueue.StartNewCompaction(pdiskId, token);
                if (compactionInfo) {
                    return compactionInfo;
                }
            }
            return Nothing();
        }
    };

    class TCompBroker : public TActorBootstrapped<TCompBroker> {
        TPDiskCompactions CompactionsPerPDisk;
        ui64 Token = 0;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_COMP_BROKER_ACTOR;
        }

        TCompBroker() : TActorBootstrapped<TThis>() {}

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            ctx.Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup);
        }

        void Handle(TEvCompactionTokenRequest::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvCompactionTokenRequest: " << ev->Get()->ToString());

            CompactionsPerPDisk.RequestCompactionToken(ev->Get()->PDiskId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Ratio);
            TryToStartNewCompactions(ctx);
        }

        void Handle(TEvUpdateCompactionTokenRequest::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvUpdateCompactionTokenRequest: " << ev->Get()->ToString());

            CompactionsPerPDisk.UpdateRequestCompactionToken(ev->Get()->PDiskId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Ratio);
            TryToStartNewCompactions(ctx);
        }

        void Handle(TEvReleaseCompactionToken::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvReleaseCompactionToken: " << ev->Get()->ToString());

            CompactionsPerPDisk.ReleaseCompactionToken(ev->Get()->PDiskId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Token);
            TryToStartNewCompactions(ctx);
        }

        void HandleWakeup(const TActorContext& ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvWakeup");

            TryToStartNewCompactions(ctx);
            ctx.Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvCompactionTokenRequest, Handle)
            HFunc(TEvUpdateCompactionTokenRequest, Handle)
            HFunc(TEvReleaseCompactionToken, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )

        void TryToStartNewCompactions(const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_COMP_BROKER, "Compactions queue state: " << CompactionsPerPDisk.ToString());

            while (auto compactionInfo = CompactionsPerPDisk.StartNewCompaction(Token)) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_COMP_BROKER, "Start new compaction: " << compactionInfo->ToString());
                Send(compactionInfo->ActorId, new TEvCompactionTokenResult(compactionInfo->Token));
                Token++;
            }
        }
    };

    IActor *CreateCompBrokerActor() {
        return new TCompBroker();
    }

} // NKikimr
