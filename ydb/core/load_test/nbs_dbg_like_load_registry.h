#pragma once

#include <ydb/core/protos/test_shard_control.pb.h>
#include <ydb/core/protos/load_test.pb.h>
#include <google/protobuf/util/message_differencer.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/guid.h>
#include <util/generic/vector.h>

namespace NKikimr::NNbsDbgLike {

// Actor-owned: admission precedes asynchronous placement and startup. Completed
// records are never evicted early, including failed admissions after insertion.
class TRunRegistry {
public:
    using TControl = NKikimrClient::TNbsDbgLikeLoadControl;
    using TResult = NKikimrClient::TNbsDbgLikeLoadResult;
    struct TRun {
        TControl Input;
        TResult Result;
        ui64 Tag = 0;
        ui32 PendingCapabilities = 0;
        // One deadline for the whole startup sequence: placement lookup,
        // capability checks, DBG readiness and configuration acknowledgement.
        TInstant StartupDeadline;
        TVector<NKikimrClient::TNbsDbgLikeLoadTablet> Placements;
    };
    const TString Incarnation = CreateGuidAsString();
    THashMap<TString, TRun> Runs;

    void Prune(TInstant now) {
        for (auto it = Runs.begin(); it != Runs.end();) {
            if (it->second.Result.HasFinishedAtMs()
                && now >= TInstant::MilliSeconds(it->second.Result.GetFinishedAtMs()) + TDuration::Hours(24)) {
                Runs.erase(it++);
            } else {
                ++it;
            }
        }
    }

    static bool SameInput(const TControl& a, const TControl& b) {
        const auto& left = a.GetLoad().GetNbsDbgLikeLoad();
        const auto& right = b.GetLoad().GetNbsDbgLikeLoad();
        if (left.TargetsSize() != right.TargetsSize()) { return false; }
        for (ui32 i = 0; i < left.TargetsSize(); ++i) {
            // Absence requests placement lookup; explicit zero means local.
            if (left.GetTargets(i).HasNodeId() != right.GetTargets(i).HasNodeId()) { return false; }
        }
        auto normalizedA = a;
        auto normalizedB = b;
        normalizedA.ClearRpcTimeoutMs();
        normalizedB.ClearRpcTimeoutMs();
        return google::protobuf::util::MessageDifferencer::Equivalent(normalizedA, normalizedB);
    }

    static bool Active(const TRun& run) {
        return !run.Result.HasFinishedAtMs();
    }

    TRun* Admit(const TControl& request, TInstant now, TString& error) {
        Prune(now);
        if (auto it = Runs.find(request.GetRequestId()); it != Runs.end()) {
            if (!SameInput(it->second.Input, request)) {
                error = "request ID conflicts with original logical inputs";
                return nullptr;
            }
            return &it->second;
        }
        if (Runs.size() >= 1024) {
            error = "coordinator capacity exhausted (1024 retained/active runs)";
            return nullptr;
        }
        auto& run = Runs[request.GetRequestId()];
        run.Input = request;
        run.StartupDeadline = now + TDuration::Seconds(request.GetStartupTimeoutSeconds());
        run.Result.SetState(TResult::IN_PROGRESS);
        run.Result.SetLatencyUnit("microseconds");
        run.Result.SetStartedAtMs(now.MilliSeconds());
        *run.Result.MutableEffectiveConfig() = request.GetLoad();
        return &run;
    }
};

} // namespace NKikimr::NNbsDbgLike
