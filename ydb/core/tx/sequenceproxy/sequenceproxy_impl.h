#pragma once
#include "defs.h"
#include "sequenceproxy.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <ydb/core/base/counters.h>
#include <ydb/library/services/services.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/monotonic_provider.h>

namespace NKikimr {
namespace NSequenceProxy {

    struct TSequenceProxyCounters : TAtomicRefCount<TSequenceProxyCounters> {
        ::NMonitoring::TDynamicCounters::TCounterPtr RequestCount;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResponseCount;
        ::NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;

        ::NMonitoring::THistogramPtr SequenceShardAllocateCount;
        ::NMonitoring::THistogramPtr NextValLatency;

        TSequenceProxyCounters();
    };

    class TSequenceProxy : public TActorBootstrapped<TSequenceProxy> {
    public:
        TSequenceProxy(const TSequenceProxySettings& settings)
            : Settings(settings)
        {
            Y_UNUSED(Settings); // TODO
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::SEQUENCE_PROXY_SERVICE;
        }

        void Bootstrap();

    private:
        class TAllocateActor;

        using TSequenceInfo = NSchemeCache::TSchemeCacheNavigate::TSequenceInfo;

        struct TEvPrivate {
            enum EEv {
                EvBegin = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
                EvAllocateResult,
            };

            struct TEvAllocateResult : public TEventLocal<TEvAllocateResult, EvAllocateResult> {
                Ydb::StatusIds::StatusCode Status;
                NYql::TIssues Issues;
                ui64 TabletId;
                TPathId PathId;
                i64 Start, Increment;
                ui64 Count;
            };
        };

    private:
        struct TResolveResult {
            TPathId PathId;
            TIntrusiveConstPtr<TSequenceInfo> SequenceInfo;
            TIntrusivePtr<TSecurityObject> SecurityObject;
        };

    private:
        struct TNextValRequestInfo {
            TActorId Sender;
            ui64 Cookie;
            TIntrusivePtr<NACLib::TUserToken> UserToken;
            TMonotonic StartAt;
        };

        struct TCachedAllocation {
            i64 Start, Increment;
            ui64 Count;
        };

        struct TPendingAllocation {
            ui64 Cache;
            THolder<TEvPrivate::TEvAllocateResult> Result;
        };

        // When requests attach to a specific path id are tracked here
        // This is also where we track all the cached info about a given
        // sequence.
        struct TSequenceByPathId {
            TIntrusiveConstPtr<TSequenceInfo> SequenceInfo;
            TIntrusivePtr<TSecurityObject> SecurityObject;
            TList<TCachedAllocation> CachedAllocations;
            TList<TNextValRequestInfo> PendingNextValResolve;
            TList<TNextValRequestInfo> PendingNextVal;
            ui64 LastKnownTabletId = 0;
            ui64 DefaultCacheSize = 0;
            bool ResolveInProgress = false;
            bool AllocateInProgress = false;
            ui64 TotalCached = 0;
            ui64 TotalRequested = 0;
            ui64 TotalAllocating = 0;
        };

        // When requests come using sequence name they end up here first
        struct TSequenceByName {
            TPathId PathId;
            TList<TNextValRequestInfo> NewNextValResolve;
            TList<TNextValRequestInfo> PendingNextValResolve;
            bool ResolveInProgress = false;
        };

        // Every database may have its own set of cached objects
        struct TDatabaseState {
            THashMap<TPathId, TSequenceByPathId> SequenceByPathId;
            THashMap<TString, TSequenceByName> SequenceByName;
        };

        struct TResolveInFlight {
            TString Database;
            std::variant<TString, TPathId> Path;
        };

        struct TAllocateInFlight {
            TString Database;
            TPathId PathId;
        };

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvents::TEvPoison, HandlePoison);
                hFunc(TEvSequenceProxy::TEvNextVal, Handle);
                hFunc(TEvPrivate::TEvAllocateResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            }
        }

        void HandlePoison();
        void Handle(TEvSequenceProxy::TEvNextVal::TPtr& ev);
        void Handle(TEvPrivate::TEvAllocateResult::TPtr& ev);
        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

        void Reply(const TNextValRequestInfo& request, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);
        void Reply(const TNextValRequestInfo& request, const TPathId& pathId, i64 value);
        ui64 StartResolve(const TString& database, const std::variant<TString, TPathId>& path, bool syncVersion);
        ui64 StartAllocate(ui64 tabletId, const TString& database, const TPathId& pathId, ui64 cache);
        void MaybeStartResolve(const TString& database, const TString& path, TSequenceByName& info);
        void DoNextVal(TNextValRequestInfo&& request, const TString& database, const TString& path);
        void DoNextVal(TNextValRequestInfo&& request, const TString& database, const TPathId& pathId, bool needRefresh = true);
        void OnResolveError(const TString& database, const TString& path, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);
        void OnResolveError(const TString& database, const TPathId& pathId, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);
        void OnResolveResult(const TString& database, const TString& path, TResolveResult&& result);
        void OnResolveResult(const TString& database, const TPathId& pathId, TResolveResult&& result);
        void OnResolved(const TString& database, const TPathId& pathId, TSequenceByPathId& info, TList<TNextValRequestInfo>& resolved);
        void OnChanged(const TString& database, const TPathId& pathId, TSequenceByPathId& info);
        bool DoMaybeReplyUnauthorized(const TNextValRequestInfo& request, const TPathId& pathId, TSequenceByPathId& info);
        bool DoReplyFromCache(const TNextValRequestInfo& request, const TPathId& pathId, TSequenceByPathId& info);

    private:
        const TSequenceProxySettings Settings;
        TString LogPrefix;
        THashMap<TString, TDatabaseState> Databases;
        THashMap<ui64, TResolveInFlight> ResolveInFlight;
        THashMap<ui64, TAllocateInFlight> AllocateInFlight;
        ui64 LastCookie = 0;
        TIntrusivePtr<TSequenceProxyCounters> Counters;
    };

} // namespace NSequenceProxy
} // namespace NKikimr
