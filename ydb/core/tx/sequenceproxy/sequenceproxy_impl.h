#pragma once
#include "defs.h"
#include "sequenceproxy.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr {
namespace NSequenceProxy {

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
        class TResolveActor;
        class TAllocateActor;

        using TSequenceInfo = NSchemeCache::TSchemeCacheNavigate::TSequenceInfo;

        struct TEvPrivate {
            enum EEv {
                EvResolveResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
                EvAllocateResult,
            };

            struct TEvResolveResult : public TEventLocal<TEvResolveResult, EvResolveResult> {
                Ydb::StatusIds::StatusCode Status;
                NYql::TIssues Issues;
                TPathId PathId;
                TIntrusiveConstPtr<TSequenceInfo> SequenceInfo;
                TIntrusivePtr<TSecurityObject> SecurityObject;
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
        struct TNextValRequestInfo {
            TActorId Sender;
            ui64 Cookie;
            TIntrusivePtr<NACLib::TUserToken> UserToken;
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
                hFunc(TEvPrivate::TEvResolveResult, Handle);
                hFunc(TEvPrivate::TEvAllocateResult, Handle);
            }
        }

        void HandlePoison();
        void Handle(TEvSequenceProxy::TEvNextVal::TPtr& ev);
        void Handle(TEvPrivate::TEvResolveResult::TPtr& ev);
        void Handle(TEvPrivate::TEvAllocateResult::TPtr& ev);

        ui64 StartResolve(const TString& database, const std::variant<TString, TPathId>& path, bool syncVersion);
        ui64 StartAllocate(ui64 tabletId, const TString& database, const TPathId& pathId, ui64 cache);
        void DoNextVal(TNextValRequestInfo&& request, const TString& database, const TString& path);
        void DoNextVal(TNextValRequestInfo&& request, const TString& database, const TPathId& pathId, bool needRefresh = true);
        void OnResolveResult(const TString& database, const TString& path, TEvPrivate::TEvResolveResult* msg);
        void OnResolveResult(const TString& database, const TPathId& pathId, TEvPrivate::TEvResolveResult* msg);
        void OnResolved(const TString& database, const TPathId& pathId, TSequenceByPathId& info);
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
    };

} // namespace NSequenceProxy
} // namespace NKikimr
