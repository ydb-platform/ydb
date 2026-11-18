#include "blobstorage_hullcompactbroker.h"
#include <memory>
#include <cmath>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/base/counters.h>
#include <util/generic/hash_set.h>
#include <util/generic/queue.h>
#include <util/datetime/base.h>
#include <util/string/join.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
    struct TCompBrokerMon : public TThrRefBase {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Group;
        
        NMonitoring::TDynamicCounters::TCounterPtr CompBrokerPendingCompactions;
        NMonitoring::TDynamicCounters::TCounterPtr CompBrokerActiveCompactions;
        
        NMonitoring::TDynamicCounters::TCounterPtr CompBrokerTokenRequests;
        NMonitoring::TDynamicCounters::TCounterPtr CompBrokerTokenGrants;
        NMonitoring::TDynamicCounters::TCounterPtr CompBrokerTokenReleases;
        
        TCompBrokerMon(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
            : Group(GetServiceCounters(counters, "storage_utils"))
        {
            CompBrokerPendingCompactions = Group->GetCounter("CompBrokerPendingCompactions", false);
            CompBrokerActiveCompactions = Group->GetCounter("CompBrokerActiveCompactions", false);
            
            CompBrokerTokenRequests = Group->GetCounter("CompBrokerTokenRequests", true);
            CompBrokerTokenGrants = Group->GetCounter("CompBrokerTokenGrants", true);
            CompBrokerTokenReleases = Group->GetCounter("CompBrokerTokenReleases", true);
            
        }
    };

    struct TCompactionKey {
        TGroupId GroupId;
        TVDiskIdShort VDiskId;
        TActorId ActorId;

        TCompactionKey(const TGroupId& groupId, const TVDiskIdShort& vDiskId, const TActorId& actorId)
            : GroupId(groupId)
            , VDiskId(vDiskId)
            , ActorId(actorId)
        {}

        bool operator==(const TCompactionKey& other) const {
            return GroupId == other.GroupId && VDiskId == other.VDiskId && ActorId == other.ActorId;
        }

        TString ToString() const {
            TStringStream str;
            str << "{GroupId# " << GroupId << " VDiskId# " << VDiskId.ToString() << " ActorId# " << ActorId << "}";
            return str.Str();
        }

        ui64 Hash() const {
            size_t h = THash<NKikimr::TGroupId>{}(GroupId);
            h = CombineHashes(h, VDiskId.Hash());
            h = CombineHashes(h, THash<TActorId>{}(ActorId));
            return h;
        }
    };
} // namespace NKikimr

template <>
struct THash<NKikimr::TCompactionKey> {
    ui64 operator()(const NKikimr::TCompactionKey& key) const {
        return key.Hash();
    }
};

namespace NKikimr {

    struct TCompactionRequest{
        TCompactionKey Key;
        double Priority;
        TInstant RequestTime;
        ui64 RequestOrder;

        TCompactionRequest(const TGroupId& groupId, const TVDiskIdShort& vDiskId, const TActorId actorId, const double priority, ui64 requestOrder)
            : Key(groupId, vDiskId, actorId)
            , Priority(priority)
            , RequestTime(TInstant::Now())
            , RequestOrder(requestOrder)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionRequest " << Key.ToString()
                << " Priority# " << Priority
                << " RequestTime# " << RequestTime.ToStringUpToSeconds()
                << " WaitTimeMs# " << (TInstant::Now() - RequestTime).MilliSeconds()
                << " RequestOrder# " << RequestOrder
                << "}";
            return str.Str();
        }
    };

    struct TCompactionInfo{
        TPDiskId PDiskId;
        TGroupId GroupId;
        TVDiskIdShort VDiskId;
        TActorId ActorId;
        TCompactionTokenId Token;
        TInstant StartTime;

        TCompactionInfo(TPDiskId pdiskId, const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId actorId, const TCompactionTokenId token)
            : PDiskId(pdiskId)
            , GroupId(groupId)
            , VDiskId(vdiskId)
            , ActorId(actorId)
            , Token(token)
            , StartTime(TInstant::Now())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionInfo PDiskId# " << PDiskId
                << " GroupId# " << GroupId
                << " VDiskId# " << VDiskId.ToString()
                << " ActorId# " << ActorId
                << " Token# " << Token
                << " StartTime# " << StartTime.ToStringUpToSeconds()
                << " WorkingTimeMs# " << (TInstant::Now() - StartTime).MilliSeconds()
                << "}";
            return str.Str();
        }
    };

    struct TCompactionQueue {
        i64 MaxActiveCompactions;
        ui64 NextRequestOrder = 0;

        struct TCompactionRequests : public THashMap<TCompactionKey, TCompactionRequest> {
            TString ToString() const {
                TStringStream str;
                str << "{TCompactionRequests Size# " << size() << ", Compactions# [";
                TVector<TString> items;
                for (const auto& [key, request] : *this) {
                    TStringStream item;
                    item << " {Key# " << key.ToString() << " Request# " << request.ToString() << "}";
                    items.push_back(item.Str());
                }
                str << JoinSeq(", ", items) << "]}";
                return str.Str();
            }
        };

        struct TCompactionsInfo : public THashMap<TCompactionKey, TCompactionInfo> {
            TString ToString() const {
                TStringStream str;
                str << "{TCompactionsInfo Size# " << size() << ", Compactions# [";
                TVector<TString> items;
                for (const auto& [key, info] : *this) {
                    TStringStream item;
                    item << " {Key# " << key.ToString() << " Info# " << info.ToString() << "}";
                    items.push_back(item.Str());
                }
                str << JoinSeq(", ", items) << "]}";
                return str.Str();
            }
        };

        using TCompactionsToken = THashMap<TCompactionTokenId, TCompactionKey>;

        TCompactionRequests PendingCompactions;
        TCompactionsToken CompactionsToken;
        TCompactionsInfo ActiveCompactionsInfo;

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionQueue MaxActiveCompactions# " << MaxActiveCompactions
                << " PendingCompactions# " << PendingCompactions.ToString()
                << " ActiveCompactions# " << ActiveCompactionsInfo.ToString()
                << "}";
            return str.Str();
        }

        void RequestCompactionToken(const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId& actorId, double priority) {
            TCompactionKey key(groupId, vdiskId, actorId);

            PendingCompactions.insert_or_assign(key, TCompactionRequest(groupId, vdiskId, actorId, priority, NextRequestOrder++));
        }

        void ReleaseCompactionToken(const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId& actorId, TCompactionTokenId token) {
            TCompactionKey key(groupId, vdiskId, actorId);
            
            auto tokenIt = CompactionsToken.find(token);
            Y_VERIFY_S(tokenIt != CompactionsToken.end(), "ReleaseCompactionToken: token " << token << " not found");
            Y_VERIFY_S(tokenIt->second == key, "ReleaseCompactionToken: token " << token << " belongs to " << tokenIt->second.ToString() 
                << ", not " << key.ToString());
            
            auto infoIt = ActiveCompactionsInfo.find(key);
            Y_VERIFY_S(infoIt != ActiveCompactionsInfo.end(), "ReleaseCompactionToken: no active compaction for " << key.ToString());

            CompactionsToken.erase(token);
            ActiveCompactionsInfo.erase(key);
            PendingCompactions.erase(key);
        }

        void RemoveCompaction(const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId& actorId) {
            TCompactionKey key(groupId, vdiskId, actorId);
            auto compactionInfo = ActiveCompactionsInfo.find(key);

            if (compactionInfo != ActiveCompactionsInfo.end()) {
                CompactionsToken.erase(compactionInfo->second.Token);
                ActiveCompactionsInfo.erase(key);
            }

            PendingCompactions.erase(key);
        }

        TMaybe<TCompactionInfo> StartNewCompaction(i64 maxCompactions, TPDiskId pdiskId, TCompactionTokenId token) {
            MaxActiveCompactions = maxCompactions;
            if (ActiveCompactionsInfo.size() >= (ui64)maxCompactions) {
                return Nothing();
            }
            if (PendingCompactions.empty()) {
                return Nothing();
            }

            auto it = std::max_element(PendingCompactions.begin(), PendingCompactions.end(),
                    [](const auto& lhs, const auto& rhs) {
                        constexpr double epsilon = 1e-9;
                        if (std::abs(lhs.second.Priority - rhs.second.Priority) > epsilon) {
                            return lhs.second.Priority < rhs.second.Priority;
                        }
                        return lhs.second.RequestOrder > rhs.second.RequestOrder;
                    });
            Y_VERIFY(it != PendingCompactions.end());
            const TCompactionKey& key = it->first;
            const TCompactionRequest& request = it->second;
            ActiveCompactionsInfo.emplace(key, TCompactionInfo(pdiskId, request.Key.GroupId, request.Key.VDiskId, request.Key.ActorId, token));
            CompactionsToken.emplace(token, request.Key);
            PendingCompactions.erase(it);
            return ActiveCompactionsInfo.at(key);
        }
    };

    struct TPDiskCompactions {
        THashMap<TPDiskId, TCompactionQueue> CompactionsPerPDisk;

        TString ToString() const {
            TStringStream str;
            str << "{TPDiskCompactions [";
            TVector<TString> items;
            for (const auto& [pdiskId, compactionQueue] : CompactionsPerPDisk) {
                TStringStream item;
                item << " {PDiskId# " << pdiskId << " Compactions# " << compactionQueue.ToString() << "}";
                items.push_back(item.Str());
            }
            str << JoinSeq(", ", items) << "]}";
            return str.Str();
        }

        void RequestCompactionToken(TPDiskId pdiskId, const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId& actorId, double priority) {
            return CompactionsPerPDisk[pdiskId].RequestCompactionToken(groupId, vdiskId, actorId, priority);
        }

        void ReleaseCompactionToken(TPDiskId pdiskId, const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId& actorId, TCompactionTokenId token) {
            return CompactionsPerPDisk[pdiskId].ReleaseCompactionToken(groupId, vdiskId, actorId, token);
        }

        void RemoveCompaction(TPDiskId pdiskId, const TGroupId& groupId, const TVDiskIdShort& vdiskId, const TActorId& actorId) {
            return CompactionsPerPDisk[pdiskId].RemoveCompaction(groupId, vdiskId, actorId);
        }

        TMaybe<TCompactionInfo> StartNewCompaction(i64 maxCompactionsPerPDisk, TCompactionTokenId token) {
            for (auto& [pdiskId, compactionQueue] : CompactionsPerPDisk) {
                auto compactionInfo = compactionQueue.StartNewCompaction(maxCompactionsPerPDisk, pdiskId, token);
                if (compactionInfo) {
                    return compactionInfo;
                }
            }
            return Nothing();
        }

        ui64 GetTotalPending() const {
            ui64 total = 0;
            for (const auto& [_, queue] : CompactionsPerPDisk) {
                total += queue.PendingCompactions.size();
            }
            return total;
        }

        ui64 GetTotalActive() const {
            ui64 total = 0;
            for (const auto& [_, queue] : CompactionsPerPDisk) {
                total += queue.ActiveCompactionsInfo.size();
            }
            return total;
        }
    };

    ui64 LongWorkingThresholdSec = 60*60*6; // 6h
    ui64 LongWaitingThresholdSec = 60*60*24; // 24h

    class TCompBroker : public TActorBootstrapped<TCompBroker> {
        TPDiskCompactions CompactionsPerPDisk;
        ui64 Token = 1;
        TMemorizableControlWrapper MaxActiveCompactionsPerPDisk;
        TIntrusivePtr<TCompBrokerMon> Mon;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_COMP_BROKER_ACTOR;
        }

        TCompBroker(
            const TControlWrapper& maxActiveCompactionsPerPDisk,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
            : TActorBootstrapped<TThis>()
            , MaxActiveCompactionsPerPDisk(maxActiveCompactionsPerPDisk)
            , Mon(new TCompBrokerMon(counters))
        {}

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup);
        }

        void Handle(TEvCompactionTokenRequest::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvCompactionTokenRequest: " << ev->Get()->ToString());

            Mon->CompBrokerTokenRequests->Inc();
            CompactionsPerPDisk.RequestCompactionToken(ev->Get()->PDiskId, ev->Get()->GroupId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Ratio);
            TryToStartNewCompactions(ctx);
        }

        void Handle(TEvReleaseCompactionToken::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvReleaseCompactionToken: " << ev->Get()->ToString());

            Mon->CompBrokerTokenReleases->Inc();
            if (ev->Get()->Force) {
                CompactionsPerPDisk.RemoveCompaction(ev->Get()->PDiskId, ev->Get()->GroupId, ev->Get()->VDiskId, ev->Sender);
            } else {
                CompactionsPerPDisk.ReleaseCompactionToken(ev->Get()->PDiskId, ev->Get()->GroupId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Token);
            }
            TryToStartNewCompactions(ctx);
        }

        void HandleWakeup(const TActorContext& ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvWakeup");

            TryToStartNewCompactions(ctx);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup);
        }

        void TryToStartNewCompactions(const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_COMP_BROKER, "Compactions queue state: " << CompactionsPerPDisk.ToString());

            auto maxCompactions = MaxActiveCompactionsPerPDisk.Update(ctx.Now());
            while (auto compactionInfo = CompactionsPerPDisk.StartNewCompaction(maxCompactions, Token)) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_COMP_BROKER, "Start new compaction: " << compactionInfo->ToString());
                Mon->CompBrokerTokenGrants->Inc();
                Send(compactionInfo->ActorId, new TEvCompactionTokenResult(compactionInfo->Token, compactionInfo->GroupId, compactionInfo->VDiskId));
                Token++;
            }
            
            UpdateMetrics(ctx);
        }

        void CollectCurrentCompactionsStats(const TActorContext& ctx) {
            TInstant now = TInstant::Now();
            
            TVector<TString> longWaitingCompactions;
            TVector<TString> longWorkingCompactions;
            
            for (const auto& [pdiskId, queue] : CompactionsPerPDisk.CompactionsPerPDisk) {
                for (const auto& [key, request] : queue.PendingCompactions) {
                    double waitTimeSeconds = (now - request.RequestTime).SecondsFloat();

                    if (waitTimeSeconds >= LongWaitingThresholdSec) {
                        TStringStream ss;
                        ss << "{PDiskId# " << pdiskId
                           << " VDiskId# " << request.Key.VDiskId
                           << " ActorId# " << request.Key.ActorId
                           << " WaitTimeSec# " << static_cast<i64>(waitTimeSeconds)
                           << " Priority# " << request.Priority << "}";
                        longWaitingCompactions.push_back(ss.Str());
                    }
                }
                
                for (const auto& [key, info] : queue.ActiveCompactionsInfo) {
                    double workTimeSeconds = (now - info.StartTime).SecondsFloat();

                    if (workTimeSeconds >= LongWorkingThresholdSec) {
                        TStringStream ss;
                        ss << "{PDiskId# " << pdiskId
                           << " VDiskId# " << info.VDiskId
                           << " ActorId# " << info.ActorId
                           << " Token# " << info.Token
                           << " WorkTimeSec# " << static_cast<i64>(workTimeSeconds)
                           << " StartTime# " << info.StartTime.ToStringUpToSeconds() << "}";
                        longWorkingCompactions.push_back(ss.Str());
                    }
                }
            }
            
            if (!longWaitingCompactions.empty()) {
                LOG_WARN_S(ctx, NKikimrServices::BS_COMP_BROKER, 
                    "Long waiting compactions detected: Count# " << longWaitingCompactions.size() 
                    << " Compactions# [" << JoinSeq(", ", longWaitingCompactions) << "]");
            }
            
            if (!longWorkingCompactions.empty()) {
                LOG_WARN_S(ctx, NKikimrServices::BS_COMP_BROKER,
                    "Long working compactions detected: Count# " << longWorkingCompactions.size() 
                    << " Compactions# [" << JoinSeq(", ", longWorkingCompactions) << "]");
            }
        }

        void UpdateMetrics(const TActorContext& ctx) {
            CollectCurrentCompactionsStats(ctx);
            Mon->CompBrokerPendingCompactions->Set(CompactionsPerPDisk.GetTotalPending());
            Mon->CompBrokerActiveCompactions->Set(CompactionsPerPDisk.GetTotalActive());
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvCompactionTokenRequest, Handle)
            HFunc(TEvReleaseCompactionToken, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )
    };

    IActor *CreateCompBrokerActor(
        const TControlWrapper& maxActiveCompactionsPerPDisk,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
        return new TCompBroker(maxActiveCompactionsPerPDisk, counters);
    }

} // NKikimr
