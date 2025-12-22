#include "blobstorage_hullcompactbroker.h"
#include <memory>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/base/counters.h>
#include <util/generic/hash_set.h>
#include <util/generic/queue.h>
#include <util/datetime/base.h>
#include <util/string/join.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>

namespace NKikimr {
    struct TCompBrokerMon : public TThrRefBase {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Group;
        
        NMonitoring::TDynamicCounters::TCounterPtr PendingCompactions;
        NMonitoring::TDynamicCounters::TCounterPtr ActiveCompactions;
        
        NMonitoring::TDynamicCounters::TCounterPtr TokenRequests;
        NMonitoring::TDynamicCounters::TCounterPtr TokenGrants;
        NMonitoring::TDynamicCounters::TCounterPtr TokenReleases;
        NMonitoring::TDynamicCounters::TCounterPtr TokenUpdates;
        
        NMonitoring::THistogramPtr WaitTimeSeconds;
        NMonitoring::THistogramPtr WorkTimeSeconds;
        
        TCompBrokerMon(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
            : Group(GetServiceCounters(counters, "utils|comp_broker"))
        {
            PendingCompactions = Group->GetCounter("PendingCompactions", false);
            ActiveCompactions = Group->GetCounter("ActiveCompactions", false);
            
            TokenRequests = Group->GetCounter("TokenRequests", true);
            TokenGrants = Group->GetCounter("TokenGrants", true);
            TokenReleases = Group->GetCounter("TokenReleases", true);
            TokenUpdates = Group->GetCounter("TokenUpdates", true);
            
            NMonitoring::TBucketBounds waitBounds{
                10,      // 10 seconds
                30,      // 30 seconds
                60,      // 1 minute
                300,     // 5 minutes
                600,     // 10 minutes
                1800,    // 30 minutes
                3600,    // 1 hour
                7200,    // 2 hours
                21600    // 6 hours
            };
            WaitTimeSeconds = Group->GetHistogram("WaitTimeSeconds", 
                NMonitoring::ExplicitHistogram(waitBounds));
            
            NMonitoring::TBucketBounds workBounds{
                60,      // 1 minute
                300,     // 5 minutes
                600,     // 10 minutes
                1800,    // 30 minutes
                3600,    // 1 hour
                7200,    // 2 hours
                21600,   // 6 hours
                43200,   // 12 hours
                86400    // 24 hours
            };
            WorkTimeSeconds = Group->GetHistogram("WorkTimeSeconds", 
                NMonitoring::ExplicitHistogram(workBounds));
        }
    };

    struct TCompactionKey {
        TString VDiskId;
        TActorId ActorId;

        TCompactionKey(const TString& vDiskId, const TActorId& actorId)
            : VDiskId(vDiskId)
            , ActorId(actorId)
        {}

        bool operator==(const TCompactionKey& other) const {
            return VDiskId == other.VDiskId && ActorId == other.ActorId;
        }

        TString ToString() const {
            TStringStream str;
            str << "{VDiskId# " << VDiskId << " ActorId# " << ActorId << "}";
            return str.Str();
        }
    };

    struct TCompactionRequest{
        TCompactionKey Key;
        double Priority;
        TInstant RequestTime;

        TCompactionRequest(const TString& vDiskId, const TActorId actorId, const double priority)
            : Key(vDiskId, actorId)
            , Priority(priority)
            , RequestTime(TInstant::Now())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionRequest " << Key.ToString() << " Priority# " << Priority << " RequestTime# " << RequestTime.ToStringUpToSeconds() << " WaitTimeMs# " << (TInstant::Now() - RequestTime).MilliSeconds() << "}";
            return str.Str();
        }
    };

    struct TCompactionInfo{
        TString PDiskId;
        TString VDiskId;
        TActorId ActorId;
        TCompactionTokenId Token;
        TInstant StartTime;

        TCompactionInfo(const TString& pdiskId, const TString& vdiskId, const TActorId actorId, const TCompactionTokenId token)
            : PDiskId(pdiskId)
            , VDiskId(vdiskId)
            , ActorId(actorId)
            , Token(token)
            , StartTime(TInstant::Now())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionInfo PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " ActorId# " << ActorId << " Token# " << Token << " StartTime# " << StartTime.ToStringUpToSeconds() << " WorkingTimeMs# " << (TInstant::Now() - StartTime).MilliSeconds() << "}";
            return str.Str();
        }
    };

    struct TCompactionQueue {
        i64 MaxActiveCompactions;

        struct TCompactionRequests : public THashMap<TString, TCompactionRequest> {
            TString ToString() const {
                TStringStream str;
                str << "{TCompactionRequests Size# " << size() << ", Compactions# [";
                TVector<TString> items;
                for (const auto& [key, request] : *this) {
                    TStringStream item;
                    item << " {Key# " << key << " Request# " << request.ToString() << "}";
                    items.push_back(item.Str());
                }
                str << JoinSeq(", ", items) << "]}";
                return str.Str();
            }
        };

        struct TCompactionsInfo : public THashMap<TString, TCompactionInfo> {
            TString ToString() const {
                TStringStream str;
                str << "{TCompactionsInfo Size# " << size() << ", Compactions# [";
                TVector<TString> items;
                for (const auto& [key, info] : *this) {
                    TStringStream item;
                    item << " {Key# " << key << " Info# " << info.ToString() << "}";
                    items.push_back(item.Str());
                }
                str << JoinSeq(", ", items) << "]}";
                return str.Str();
            }
        };

        using TCompactionsToken = THashMap<TCompactionTokenId, TCompactionKey>;

        TCompactionRequests PendingCompactions;
        TCompactionsToken CompactionsToken;
        TCompactionsInfo CompactionsInfo;

        TString ToString() const {
            TStringStream str;
            str << "{TCompactionQueue MaxActiveCompactions# " << MaxActiveCompactions << " PendingCompactions# " << PendingCompactions.ToString() << " ActiveCompactions# " << CompactionsInfo.ToString() << "}";
            return str.Str();
        }

        void RequestCompactionToken(const TString& vdiskId, const TActorId& actorId, double priority) {
            TCompactionKey key(vdiskId, actorId);

            auto infoIt = CompactionsInfo.find(key.ToString());
            Y_VERIFY_S(infoIt == CompactionsInfo.end(), 
                "UpdateRequestCompactionToken: trying to request token during active compaction for " << key.ToString());

            PendingCompactions.emplace(key.ToString(), TCompactionRequest(vdiskId, actorId, priority));
        }

        void UpdateRequestCompactionToken(const TString& vdiskId, const TActorId& actorId, double priority) {
            TCompactionKey key(vdiskId, actorId);

            auto infoIt = CompactionsInfo.find(key.ToString());
            if (infoIt != CompactionsInfo.end()) {
                // compaction is already active, nothing to update
                return;
            }

            auto pendingIt = PendingCompactions.find(key.ToString());
            Y_VERIFY_S(pendingIt != PendingCompactions.end(), 
                "UpdateRequestCompactionToken: no pending request for " << key.ToString());
            pendingIt->second.Priority = priority;
        }

        void ReleaseCompactionToken(const TString& vdiskId, const TActorId& actorId, TCompactionTokenId token) {
            TCompactionKey key(vdiskId, actorId);
            
            auto tokenIt = CompactionsToken.find(token);
            Y_VERIFY_S(tokenIt != CompactionsToken.end(), "ReleaseCompactionToken: token " << token << " not found");
            Y_VERIFY_S(tokenIt->second == key, "ReleaseCompactionToken: token " << token << " belongs to " << tokenIt->second.ToString() 
                << ", not " << key.ToString());
            
            auto infoIt = CompactionsInfo.find(key.ToString());
            Y_VERIFY_S(infoIt != CompactionsInfo.end(), "ReleaseCompactionToken: no active compaction for " << key.ToString());

            CompactionsToken.erase(token);
            CompactionsInfo.erase(key.ToString());
            PendingCompactions.erase(key.ToString());
        }

        TMaybe<TCompactionInfo> StartNewCompaction(i64 maxCompactions, const TString& pdiskId, TCompactionTokenId token) {
            MaxActiveCompactions = maxCompactions;
            if (CompactionsInfo.size() >= (ui64)maxCompactions) {
                return Nothing();
            }
            if (PendingCompactions.empty()) {
                return Nothing();
            }

            auto it = std::max_element(PendingCompactions.begin(), PendingCompactions.end(),
                    [](const auto& lhs, const auto& rhs) {
                        return lhs.second.Priority < rhs.second.Priority;
                    });
            Y_VERIFY(it != PendingCompactions.end());
            const TString& key = it->first;
            const TCompactionRequest& request = it->second;
            CompactionsInfo.emplace(key, TCompactionInfo(pdiskId, request.Key.VDiskId, request.Key.ActorId, token));
            CompactionsToken.emplace(token, request.Key);
            PendingCompactions.erase(it);
            return CompactionsInfo.at(key);
        }
    };

    struct TPDiskCompactions {
        THashMap<TString, TCompactionQueue> CompactionsPerPDisk;

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

        void RequestCompactionToken(const TString& pdiskId, const TString& vdiskId, const TActorId& actorId, double priority) {
            return CompactionsPerPDisk[pdiskId].RequestCompactionToken(vdiskId, actorId, priority);
        }

        void UpdateRequestCompactionToken(const TString& pdiskId, const TString& vdiskId, const TActorId& actorId, double priority) {
            return CompactionsPerPDisk[pdiskId].UpdateRequestCompactionToken(vdiskId, actorId, priority);
        }

        void ReleaseCompactionToken(const TString& pdiskId, const TString& vdiskId, const TActorId& actorId, TCompactionTokenId token) {
            return CompactionsPerPDisk[pdiskId].ReleaseCompactionToken(vdiskId, actorId, token);
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
                total += queue.CompactionsInfo.size();
            }
            return total;
        }
    };

    class TCompBroker : public TActorBootstrapped<TCompBroker> {
        TPDiskCompactions CompactionsPerPDisk;
        ui64 Token = 1;
        TMemorizableControlWrapper MaxActiveCompactionsPerPDisk;
        TMemorizableControlWrapper LongWaitingThresholdSec;
        TMemorizableControlWrapper LongWorkingThresholdSec;
        TIntrusivePtr<TCompBrokerMon> Mon;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_COMP_BROKER_ACTOR;
        }

        TCompBroker(
            const TControlWrapper& maxActiveCompactionsPerPDisk,
            const TControlWrapper& longWaitingThresholdSec,
            const TControlWrapper& longWorkingThresholdSec,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
            : TActorBootstrapped<TThis>()
            , MaxActiveCompactionsPerPDisk(maxActiveCompactionsPerPDisk)
            , LongWaitingThresholdSec(longWaitingThresholdSec)
            , LongWorkingThresholdSec(longWorkingThresholdSec)
            , Mon(new TCompBrokerMon(counters))
        {}

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup);
        }

        void Handle(TEvCompactionTokenRequest::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvCompactionTokenRequest: " << ev->Get()->ToString());

            Mon->TokenRequests->Inc();
            CompactionsPerPDisk.RequestCompactionToken(ev->Get()->PDiskId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Ratio);
            TryToStartNewCompactions(ctx);
        }

        void Handle(TEvUpdateCompactionTokenRequest::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvUpdateCompactionTokenRequest: " << ev->Get()->ToString());

            Mon->TokenUpdates->Inc();
            CompactionsPerPDisk.UpdateRequestCompactionToken(ev->Get()->PDiskId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Ratio);
            TryToStartNewCompactions(ctx);
        }

        void Handle(TEvReleaseCompactionToken::TPtr& ev, const TActorContext &ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvReleaseCompactionToken: " << ev->Get()->ToString());

            Mon->TokenReleases->Inc();
            CompactionsPerPDisk.ReleaseCompactionToken(ev->Get()->PDiskId, ev->Get()->VDiskId, ev->Sender, ev->Get()->Token);
            TryToStartNewCompactions(ctx);
        }

        void HandleWakeup(const TActorContext& ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::BS_COMP_BROKER, "Handle TEvWakeup");

            TryToStartNewCompactions(ctx);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup);
        }

        void TryToStartNewCompactions(const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::BS_COMP_BROKER, "Compactions queue state: " << CompactionsPerPDisk.ToString());

            while (auto compactionInfo = CompactionsPerPDisk.StartNewCompaction(MaxActiveCompactionsPerPDisk.Update(ctx.Now()), Token)) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_COMP_BROKER, "Start new compaction: " << compactionInfo->ToString());
                Mon->TokenGrants->Inc();
                Send(compactionInfo->ActorId, new TEvCompactionTokenResult(compactionInfo->Token, compactionInfo->VDiskId));
                Token++;
            }
            
            UpdateMetrics(ctx);
        }

        void CollectCurrentCompactionsStats(const TActorContext& ctx) {
            TInstant now = TInstant::Now();
            i64 longWaitingThreshold = LongWaitingThresholdSec.Update(ctx.Now());
            i64 longWorkingThreshold = LongWorkingThresholdSec.Update(ctx.Now());
            
            TVector<TString> longWaitingCompactions;
            TVector<TString> longWorkingCompactions;
            
            for (const auto& [pdiskId, queue] : CompactionsPerPDisk.CompactionsPerPDisk) {
                for (const auto& [key, request] : queue.PendingCompactions) {
                    double waitTimeSeconds = (now - request.RequestTime).SecondsFloat();
                    Mon->WaitTimeSeconds->Collect(waitTimeSeconds);
                    
                    if (longWaitingThreshold > 0 && waitTimeSeconds >= longWaitingThreshold) {
                        TStringStream ss;
                        ss << "{PDiskId# " << pdiskId 
                           << " VDiskId# " << request.Key.VDiskId 
                           << " ActorId# " << request.Key.ActorId
                           << " WaitTimeSec# " << static_cast<i64>(waitTimeSeconds)
                           << " Priority# " << request.Priority << "}";
                        longWaitingCompactions.push_back(ss.Str());
                    }
                }
                
                for (const auto& [key, info] : queue.CompactionsInfo) {
                    double workTimeSeconds = (now - info.StartTime).SecondsFloat();
                    Mon->WorkTimeSeconds->Collect(workTimeSeconds);
                    
                    if (longWorkingThreshold > 0 && workTimeSeconds >= longWorkingThreshold) {
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
            Mon->PendingCompactions->Set(CompactionsPerPDisk.GetTotalPending());
            Mon->ActiveCompactions->Set(CompactionsPerPDisk.GetTotalActive());
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvCompactionTokenRequest, Handle)
            HFunc(TEvUpdateCompactionTokenRequest, Handle)
            HFunc(TEvReleaseCompactionToken, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )
    };

    IActor *CreateCompBrokerActor(
        const TControlWrapper& maxActiveCompactionsPerPDisk,
        const TControlWrapper& longWaitingThresholdSec,
        const TControlWrapper& longWorkingThresholdSec,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
        return new TCompBroker(maxActiveCompactionsPerPDisk, longWaitingThresholdSec, longWorkingThresholdSec, counters);
    }

} // NKikimr
