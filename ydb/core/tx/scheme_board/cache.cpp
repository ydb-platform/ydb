#include "cache.h"
#include "double_indexed.h"
#include "events.h"
#include "events_internal.h"
#include "helpers.h"
#include "monitorable_actor.h"
#include "subscriber.h"

#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/schemeshard/schemeshard_types.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/writer/json.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

namespace NKikimr {
namespace NSchemeBoard {

#define SBC_LOG_T(stream) SB_LOG_T(TX_PROXY_SCHEME_CACHE, stream)
#define SBC_LOG_D(stream) SB_LOG_D(TX_PROXY_SCHEME_CACHE, stream)
#define SBC_LOG_N(stream) SB_LOG_N(TX_PROXY_SCHEME_CACHE, stream)
#define SBC_LOG_W(stream) SB_LOG_W(TX_PROXY_SCHEME_CACHE, stream)

using TEvNavigate = TEvTxProxySchemeCache::TEvNavigateKeySet;
using TEvNavigateResult = TEvTxProxySchemeCache::TEvNavigateKeySetResult;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;
using TNavigateContext = NSchemeCache::TSchemeCacheNavigateContext;
using TNavigateContextPtr = TIntrusivePtr<TNavigateContext>;

using TEvResolve = TEvTxProxySchemeCache::TEvResolveKeySet;
using TEvResolveResult = TEvTxProxySchemeCache::TEvResolveKeySetResult;
using TResolve = NSchemeCache::TSchemeCacheRequest;
using TResolveContext = NSchemeCache::TSchemeCacheRequestContext;
using TResolveContextPtr = TIntrusivePtr<TResolveContext>;

using TVariantContextPtr = std::variant<TNavigateContextPtr, TResolveContextPtr>;

namespace {

    void SetError(TNavigateContext* context, TNavigate::TEntry& entry, TNavigate::EStatus status) {
        ++context->Request->ErrorCount;
        entry.Status = status;
    }

    void SetError(TResolveContext* context, TResolve::TEntry& entry, TResolve::EStatus entryStatus, TKeyDesc::EStatus keyDescStatus) {
        entry.Status = entryStatus;
        entry.KeyDescription->Status = keyDescStatus;
        ++context->Request->ErrorCount;
    }

    void SetRootUnknown(TNavigateContext* context, TNavigate::TEntry& entry) {
        SetError(context, entry, TNavigate::EStatus::RootUnknown);
    }

    void SetRootUnknown(TResolveContext* context, TResolve::TEntry&) {
        ++context->Request->ErrorCount;
    }

    void SetPathNotExist(TNavigateContext* context, TNavigate::TEntry& entry) {
        SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
    }

    void SetPathNotExist(TResolveContext* context, TResolve::TEntry& entry) {
        SetError(context, entry, TResolve::EStatus::PathErrorNotExist, TKeyDesc::EStatus::NotExists);
    }

    void SetLookupError(TNavigateContext* context, TNavigate::TEntry& entry) {
        SetError(context, entry, TNavigate::EStatus::LookupError);
    }

    void SetLookupError(TResolveContext* context, TResolve::TEntry& entry) {
        SetError(context, entry, TResolve::EStatus::LookupError, TKeyDesc::EStatus::NotExists);
    }

    template <typename TRequest, typename TEvRequest, typename TDerived>
    class TDbResolver: public TActorBootstrapped<TDerived> {
        void Handle() {
            TlsActivationContext->Send(new IEventHandle(Cache, Sender, new TEvRequest(Request.Release())));
            this->PassAway();
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::SCHEME_BOARD_DB_RESOLVER;
        }

        TDbResolver(const TActorId& cache, const TActorId& sender, THolder<TRequest> request, ui64 domainOwnerId)
            : Cache(cache)
            , Sender(sender)
            , Request(std::move(request))
            , DomainOwnerId(domainOwnerId)
        {
        }

        void Bootstrap() {
            TNavigate::TEntry entry;
            entry.Path = SplitPath(Request->DatabaseName);
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RedirectRequired = false;

            auto request = MakeHolder<TNavigate>();
            request->ResultSet.emplace_back(std::move(entry));
            request->DomainOwnerId = DomainOwnerId;

            this->Send(Cache, new TEvNavigate(request.Release()));
            this->Become(&TDerived::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            }
        }

        using TBase = TDbResolver<TRequest, TEvRequest, TDerived>;

    private:
        const TActorId Cache;
        const TActorId Sender;
        THolder<TRequest> Request;
        const ui64 DomainOwnerId;

    }; // TDbResolver

    class TDbResolverNavigate: public TDbResolver<TNavigate, TEvNavigate, TDbResolverNavigate> {
    public:
        using TBase::TBase;
    };

    class TDbResolverResolve: public TDbResolver<TResolve, TEvResolve, TDbResolverResolve> {
    public:
        using TBase::TBase;
    };

    IActor* CreateDbResolver(const TActorId& cache, const TActorId& sender, THolder<TNavigate> request, ui64 domainOwnerId) {
        return new TDbResolverNavigate(cache, sender, std::move(request), domainOwnerId);
    }

    IActor* CreateDbResolver(const TActorId& cache, const TActorId& sender, THolder<TResolve> request, ui64 domainOwnerId) {
        return new TDbResolverResolve(cache, sender, std::move(request), domainOwnerId);
    }

    template <typename TContextPtr, typename TEvResult, typename TDerived>
    class TAccessChecker: public TActorBootstrapped<TDerived> {
        static bool IsDomain(TNavigate::TEntry& entry) {
            switch (entry.Kind) {
            case NSchemeCache::TSchemeCacheNavigate::KindSubdomain:
            case NSchemeCache::TSchemeCacheNavigate::KindExtSubdomain:
                return true;
            default:
                return false;
            }
        }

        static bool IsDomain(TResolve::TEntry&) {
            return false;
        }

        static TIntrusivePtr<TSecurityObject> GetSecurityObject(const TNavigate::TEntry& entry) {
            return entry.SecurityObject;
        }

        static TIntrusivePtr<TSecurityObject> GetSecurityObject(const TResolve::TEntry& entry) {
            return entry.KeyDescription->SecurityObject;
        }

        static ui32 GetAccess(const TNavigate::TEntry& entry) {
            return entry.Access;
        }

        static ui32 GetAccess(const TResolve::TEntry& entry) {
            return entry.Access;
        }

        static ui32 GetAccessForEnhancedError() {
            return NACLib::EAccessRights::DescribeSchema;
        }

        static void SetErrorAndClear(TNavigateContext* context, TNavigate::TEntry& entry, const bool isDescribeDenied) {
            if (isDescribeDenied) {
                SetError(context, entry, TNavigate::EStatus::AccessDenied);
            } else {
                SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
            }

            switch (entry.RequestType) {
            case TNavigate::TEntry::ERequestType::ByPath:
                entry.TableId = TTableId();
                break;
            case TNavigate::TEntry::ERequestType::ByTableId:
                entry.Path.clear();
                break;
            }

            entry.Self.Drop();
            entry.SecurityObject.Drop();
            entry.DomainInfo.Drop();
            entry.Kind = TNavigate::KindUnknown;
            entry.Attributes.clear();
            entry.ListNodeEntry.Drop();
            entry.DomainDescription.Drop();
            entry.Columns.clear();
            entry.NotNullColumns.clear();
            entry.Indexes.clear();
            entry.CdcStreams.clear();
            entry.RTMRVolumeInfo.Drop();
            entry.KesusInfo.Drop();
            entry.SolomonVolumeInfo.Drop();
            entry.PQGroupInfo.Drop();
            entry.OlapStoreInfo.Drop();
            entry.ColumnTableInfo.Drop();
            entry.CdcStreamInfo.Drop();
            entry.SequenceInfo.Drop();
            entry.ReplicationInfo.Drop();
            entry.BlobDepotInfo.Drop();
            entry.BlockStoreVolumeInfo.Drop();
            entry.FileStoreInfo.Drop();
        }

        static void SetErrorAndClear(TResolveContext* context, TResolve::TEntry& entry, const bool isDescribeDenied) {
            if (isDescribeDenied) {
                SetError(context, entry, TResolve::EStatus::AccessDenied, TKeyDesc::EStatus::NotExists);
            } else {
                SetError(context, entry, TResolve::EStatus::PathErrorNotExist, TKeyDesc::EStatus::NotExists);
            }

            entry.Kind = TResolve::KindUnknown;
            entry.DomainInfo.Drop();
            TKeyDesc& keyDesc = *entry.KeyDescription;
            keyDesc.ColumnInfos.clear();
            keyDesc.Partitioning = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
            keyDesc.SecurityObject.Drop();
        }

        void SendResult() {
            SBC_LOG_D("Send result"
                << ": self# " << this->SelfId()
                << ", recipient# " << Context->Sender
                << ", result# " << Context->Request->ToString(*AppData()->TypeRegistry));

            this->Send(Context->Sender, new TEvResult(Context->Request.Release()), 0, Context->Cookie);
            this->PassAway();
        }

    public:
        explicit TAccessChecker(TContextPtr context)
            : Context(context)
        {
            Y_ABORT_UNLESS(!Context->WaitCounter);
        }

        void Bootstrap(const TActorContext&) {
            for (auto& entry : Context->Request->ResultSet) {
                if (!IsDomain(entry) && Context->ResolvedDomainInfo && entry.DomainInfo) {
                    // ^ It is allowed to describe subdomains by specifying root as DatabaseName

                    if (Context->ResolvedDomainInfo->DomainKey != entry.DomainInfo->DomainKey) {
                        SBC_LOG_W("Path does not belong to the specified domain"
                            << ": self# " << this->SelfId()
                            << ", domain# " << Context->ResolvedDomainInfo->DomainKey
                            << ", path's domain# " << entry.DomainInfo->DomainKey);

                        SetErrorAndClear(Context.Get(), entry, false);
                    }
                }

                if (Context->Request->UserToken) {
                    auto securityObject = GetSecurityObject(entry);
                    if (securityObject == nullptr) {
                        continue;
                    }

                    const ui32 access = GetAccess(entry);
                    if (!securityObject->CheckAccess(access, *Context->Request->UserToken)) {
                        SBC_LOG_W("Access denied"
                            << ": self# " << this->SelfId()
                            << ", for# " << Context->Request->UserToken->GetUserSID()
                            << ", access# " << NACLib::AccessRightsToString(access));

                        SetErrorAndClear(
                            Context.Get(),
                            entry,
                            securityObject->CheckAccess(GetAccessForEnhancedError(), *Context->Request->UserToken));
                    }
                }
            }

            SendResult();
        }

        using TBase = TAccessChecker<TContextPtr, TEvResult, TDerived>;

    private:
        TContextPtr Context;

    }; // TAccessChecker

    class TAccessCheckerNavigate: public TAccessChecker<TNavigateContextPtr, TEvNavigateResult, TAccessCheckerNavigate> {
    public:
        using TBase::TBase;
    };

    class TAccessCheckerResolve: public TAccessChecker<TResolveContextPtr, TEvResolveResult, TAccessCheckerResolve> {
    public:
        using TBase::TBase;
    };

    IActor* CreateAccessChecker(TNavigateContextPtr context) {
        return new TAccessCheckerNavigate(context);
    }

    IActor* CreateAccessChecker(TResolveContextPtr context) {
        return new TAccessCheckerResolve(context);
    }

    class TWatchCache: public TMonitorableActor<TWatchCache> {
        using TBase = TMonitorableActor<TWatchCache>;

    private:
        struct TWatcher {
            TActorId Actor;
            ui64 Key;

            TWatcher(TActorId actor, ui64 key)
                : Actor(actor)
                , Key(key)
            { }
        };

        struct TWatcherCompare {
            using is_transparent = void;

            inline bool operator()(const TWatcher& a, const TWatcher& b) const {
                if (a.Actor < b.Actor) {
                    return true;
                }
                if (a.Actor == b.Actor) {
                    return a.Key < b.Key;
                }
                return false;
            }

            inline bool operator()(const TWatcher& a, const TActorId& b) const {
                return a.Actor < b;
            }

            inline bool operator()(const TActorId& a, const TWatcher& b) const {
                return a < b.Actor;
            }
        };

        struct TEntry {
            TString Path;
            TPathId PathId;
            TActorId Subscriber;
            TSet<TWatcher, TWatcherCompare> Watchers;
            NSchemeCache::TDescribeResult::TPtr Result;
            bool Deleted = false;
            bool Unavailable = false;
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::SCHEME_BOARD_WATCH_CACHE_ACTOR;
        }

        void Bootstrap() {
            TBase::Bootstrap();
            Become(&TThis::StateWork);
        }

        void PassAway() override {
            for (auto& pr : EntriesByPathId) {
                Send(pr.second.Subscriber, new TEvents::TEvPoison);
            }
            TBase::PassAway();
        }

    private:
        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvWatchPathId, Handle);
                hFunc(TEvTxProxySchemeCache::TEvWatchRemove, Handle);
                hFunc(TSchemeBoardEvents::TEvNotifyUpdate, Handle);
                hFunc(TSchemeBoardEvents::TEvNotifyDelete, Handle);

                sFunc(TEvents::TEvPoison, PassAway);
            }
        }

        void Handle(TEvTxProxySchemeCache::TEvWatchPathId::TPtr& ev) {
            const auto* msg = ev->Get();

            auto* entry = EnsureEntry(msg->PathId);

            TWatcher watcher(ev->Sender, msg->Key);
            if (entry->Watchers.insert(watcher).second) {
                EntriesByWatcher.emplace(watcher, entry);
            }

            Notify(entry, watcher);
        }

        void Handle(TEvTxProxySchemeCache::TEvWatchRemove::TPtr& ev) {
            const auto* msg = ev->Get();

            // When key == 0 remove all watches for the sender
            auto range = (msg->Key == 0)
                ? EntriesByWatcher.equal_range(ev->Sender)
                : EntriesByWatcher.equal_range(TWatcher(ev->Sender, msg->Key));

            for (auto it = range.first; it != range.second; ++it) {
                auto watcher = it->first;
                auto* entry = it->second;
                entry->Watchers.erase(watcher);
                // TODO: probably want to cleanup entries that have no watchers in the future
            }
            EntriesByWatcher.erase(range.first, range.second);
        }

        void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
            auto* msg = ev->Get();

            auto* entry = FindEntry(ev->Sender);
            if (!entry) {
                return;
            }

            if (msg->DescribeSchemeResult.HasStatus()) {
                entry->Path = msg->DescribeSchemeResult.GetPath();
                entry->Result = NSchemeCache::TDescribeResult::Create(std::move(msg->DescribeSchemeResult));
                entry->Deleted = false;
                entry->Unavailable = false;
            } else {
                entry->Result = nullptr;
                entry->Deleted = false;
                entry->Unavailable = true;
            }

            Notify(entry);
        }

        void Handle(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev) {
            auto* msg = ev->Get();

            auto* entry = FindEntry(ev->Sender);
            if (!entry) {
                return;
            }

            if (msg->Strong) {
                entry->Result = nullptr;
                entry->Deleted = true;
                entry->Unavailable = false;
            } else {
                entry->Result = nullptr;
                entry->Deleted = false;
                entry->Unavailable = true;
            }

            Notify(entry);
        }

    private:
        TEntry* FindEntry(const TActorId& subscriber) {
            auto it = EntriesBySubscriber.find(subscriber);
            if (it == EntriesBySubscriber.end()) {
                return nullptr;
            }
            return it->second;
        }

        TEntry* EnsureEntry(const TPathId& pathId) {
            auto it = EntriesByPathId.find(pathId);
            if (it != EntriesByPathId.end()) {
                return &it->second;
            }
            auto* entry = &EntriesByPathId[pathId];
            entry->PathId = pathId;
            entry->Subscriber = CreateSubscriber(pathId);
            EntriesBySubscriber[entry->Subscriber] = entry;
            return entry;
        }

        void Notify(TEntry* entry) {
            for (const auto& watcher : entry->Watchers) {
                Notify(entry, watcher);
            }
        }

        void Notify(TEntry* entry, const TWatcher& watcher) {
            if (entry->Unavailable) {
                Send(watcher.Actor, new TEvTxProxySchemeCache::TEvWatchNotifyUnavailable(
                    watcher.Key,
                    entry->Path,
                    entry->PathId));
            } else if (entry->Deleted) {
                Send(watcher.Actor, new TEvTxProxySchemeCache::TEvWatchNotifyDeleted(
                    watcher.Key,
                    entry->Path,
                    entry->PathId));
            } else if (entry->Result) {
                Send(watcher.Actor, new TEvTxProxySchemeCache::TEvWatchNotifyUpdated(
                    watcher.Key,
                    entry->Path,
                    entry->PathId,
                    entry->Result));
            }
        }

    private:
        template <typename TPath>
        TActorId CreateSubscriber(const TPath& path, const ui64 tabletId, const ui64 domainOwnerId) const {
            SBC_LOG_T("Create subscriber"
                << ": self# " << SelfId()
                << ", path# " << path
                << ", tabletId# " << tabletId
                << ", domainOwnerId# " << domainOwnerId);

            return Register(CreateSchemeBoardSubscriber(SelfId(), path, domainOwnerId));
        }

        TActorId CreateSubscriber(const TPathId& pathId) const {
            return CreateSubscriber(pathId, pathId.OwnerId, pathId.OwnerId);
        }

    private:
        THashMap<TPathId, TEntry> EntriesByPathId;
        THashMap<TActorId, TEntry*> EntriesBySubscriber;
        TMultiMap<TWatcher, TEntry*, TWatcherCompare> EntriesByWatcher;

    }; // TWatchCache

} // anonymous

class TSchemeCache: public TMonitorableActor<TSchemeCache> {
    class TCounters {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using THistogramPtr = NMonitoring::THistogramPtr;

        TCounterPtr InFlight;
        TCounterPtr Requests;
        TCounterPtr Hits;
        TCounterPtr Misses;
        TCounterPtr Syncs;
        THistogramPtr Latency;

        TCounterPtr PerEntryHits;
        TCounterPtr PerEntryMisses;
        TCounterPtr PerEntrySyncs;

    public:
        explicit TCounters(::NMonitoring::TDynamicCounterPtr counters)
            : InFlight(counters->GetCounter("InFlight", false))
            , Requests(counters->GetCounter("Requests", true))
            , Hits(counters->GetCounter("Hits", true))
            , Misses(counters->GetCounter("Misses", true))
            , Syncs(counters->GetCounter("Syncs", true))
            , Latency(counters->GetHistogram("LatencyMs", NMonitoring::ExponentialHistogram(10, 4, 1)))
            , PerEntryHits(counters->GetCounter("PerEntry/Hits", true))
            , PerEntryMisses(counters->GetCounter("PerEntry/Misses", true))
            , PerEntrySyncs(counters->GetCounter("PerEntry/Syncs", true))
        {
        }

        void StartRequest(ui64 entryCount, ui64 waitCount, ui64 syncCount) {
            *InFlight += 1;
            *Requests += 1;

            if (!waitCount || waitCount == syncCount) {
                *Hits += 1;
            } else {
                *Misses += 1;
            }
            if (syncCount) {
                *Syncs += 1;
            }

            *PerEntryHits += (entryCount - (waitCount - syncCount));
            *PerEntryMisses += (waitCount - syncCount);
            *PerEntrySyncs += syncCount;
        }

        void FinishRequest(const TDuration& latency) {
            *InFlight -= 1;
            Latency->Collect(latency.MilliSeconds());
        }
    };

    struct TSubscriber {
        enum class EType: ui8 {
            // from low to high priority
            Empty,
            ByPathId,
            ByPath,
        };

        TActorId Subscriber;
        ui64 DomainOwnerId;
        EType Type;
        mutable ui64 SyncCookie;

        TSubscriber()
            : DomainOwnerId(0)
            , Type(EType::Empty)
            , SyncCookie(0)
        {
        }

        explicit TSubscriber(const TActorId& subscriber, const ui64 domainOwnerId, const TPathId&)
            : Subscriber(subscriber)
            , DomainOwnerId(domainOwnerId)
            , Type(EType::ByPathId)
            , SyncCookie(0)
        {
        }

        explicit TSubscriber(const TActorId& subscriber, const ui64 domainOwnerId, const TString&)
            : Subscriber(subscriber)
            , DomainOwnerId(domainOwnerId)
            , Type(EType::ByPath)
            , SyncCookie(0)
        {
        }

        TString ToString() const {
            return TStringBuilder() << "{"
                << " Subscriber: " << Subscriber
                << " DomainOwnerId: " << DomainOwnerId
                << " Type: " << static_cast<ui32>(Type)
                << " SyncCookie: " << SyncCookie
            << " }";
        }

        bool operator<(const TSubscriber& x) const {
            return Type < x.Type;
        }

        explicit operator bool() const {
            return bool(Subscriber);
        }
    };

    struct TResponseProps {
        ui64 Cookie;
        bool IsSync;
        bool Partial;

        TResponseProps()
            : Cookie(0)
            , IsSync(false)
            , Partial(false)
        {
        }

        explicit TResponseProps(ui64 cookie, bool isSync, bool partial)
            : Cookie(cookie)
            , IsSync(isSync)
            , Partial(partial)
        {
        }

        static TResponseProps FromEvent(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
            return TResponseProps(ev->Cookie, false, false);
        }

        static TResponseProps FromEvent(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev) {
            return TResponseProps(ev->Cookie, false, false);
        }

        static TResponseProps FromEvent(NInternalEvents::TEvSyncResponse::TPtr& ev) {
            return TResponseProps(ev->Cookie, true, ev->Get()->Partial);
        }

        TString ToString() const {
            return TStringBuilder() << "{"
                << " Cookie: " << Cookie
                << " IsSync: " << (IsSync ? "true" : "false")
                << " Partial: " << Partial
            << " }";
        }
    };

    class TCacheItem {
        struct TRequest {
            size_t EntryIndex;
            ui64 Cookie;
            bool IsSync;
        };

        void Clear() {
            Status.Clear();
            Kind = TNavigate::KindUnknown;
            TableKind = TResolve::KindUnknown;
            Created = false;
            CreateStep = 0;

            // pathid is never changed (yet) so must be kept
            AbandonedSchemeShardsIds.clear();

            SecurityObject.Drop();
            DomainInfo.Drop();
            Attributes.clear();

            ListNodeEntry.Drop();

            IsPrivatePath = false;

            // virtual must be kept

            Columns.clear();
            KeyColumnTypes.clear();
            NotNullColumns.clear();
            Indexes.clear();
            CdcStreams.clear();
            Partitioning = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();

            Self.Drop();

            DomainDescription.Drop();
            RtmrVolumeInfo.Drop();
            KesusInfo.Drop();
            SolomonVolumeInfo.Drop();
            PQGroupInfo.Drop();
            OlapStoreInfo.Drop();
            ColumnTableInfo.Drop();
            CdcStreamInfo.Drop();
            SequenceInfo.Drop();
            ReplicationInfo.Drop();
            BlobDepotInfo.Drop();
            ExternalTableInfo.Drop();
            ExternalDataSourceInfo.Drop();
            BlockStoreVolumeInfo.Drop();
            FileStoreInfo.Drop();
            ViewInfo.Drop();
            ResourcePoolInfo.Drop();
        }

        void FillTableInfo(const NKikimrSchemeOp::TPathDescription& pathDesc) {
            const auto& tableDesc = pathDesc.GetTable();

            for (const auto& columnDesc : tableDesc.GetColumns()) {
                auto& column = Columns[columnDesc.GetId()];
                column.Id = columnDesc.GetId();
                column.Name = columnDesc.GetName();
                auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(columnDesc.GetTypeId(),
                    columnDesc.HasTypeInfo() ? &columnDesc.GetTypeInfo() : nullptr);
                column.PType = typeInfoMod.TypeInfo;
                column.PTypeMod = typeInfoMod.TypeMod;
                column.IsBuildInProgress = columnDesc.GetIsBuildInProgress();

                if (columnDesc.HasDefaultFromSequence()) {
                    column.SetDefaultFromSequence();
                    column.DefaultFromSequence = columnDesc.GetDefaultFromSequence();
                } else if (columnDesc.HasDefaultFromLiteral()) {
                    column.SetDefaultFromLiteral();
                    column.DefaultFromLiteral = columnDesc.GetDefaultFromLiteral();
                }

                if (columnDesc.GetNotNull()) {
                    column.IsNotNullColumn = true;
                    NotNullColumns.insert(columnDesc.GetName());
                }
            }

            KeyColumnTypes.resize(tableDesc.KeyColumnIdsSize());
            for (ui32 i : xrange(tableDesc.KeyColumnIdsSize())) {
                auto* column = Columns.FindPtr(tableDesc.GetKeyColumnIds(i));
                Y_ABORT_UNLESS(column != nullptr);
                column->KeyOrder = i;
                KeyColumnTypes[i] = column->PType;
            }

            Indexes.reserve(tableDesc.TableIndexesSize());
            for (const auto& index : tableDesc.GetTableIndexes()) {
                Indexes.push_back(index);
            }

            CdcStreams.reserve(tableDesc.CdcStreamsSize());
            for (const auto& index : tableDesc.GetCdcStreams()) {
                CdcStreams.push_back(index);
            }

            if (pathDesc.TablePartitionsSize()) {
                auto partitioning = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
                partitioning->resize(pathDesc.TablePartitionsSize());
                for (ui32 i : xrange(pathDesc.TablePartitionsSize())) {
                    const auto& src = pathDesc.GetTablePartitions(i);
                    auto& partition = (*partitioning)[i];
                    partition.Range = TKeyDesc::TPartitionRangeInfo();
                    partition.Range->EndKeyPrefix.Parse(src.GetEndOfRangeKeyPrefix());
                    partition.Range->IsInclusive = src.HasIsInclusive() && src.GetIsInclusive();
                    partition.Range->IsPoint = src.HasIsPoint() && src.GetIsPoint();
                    partition.ShardId = src.GetDatashardId();
                }

                Partitioning = std::move(partitioning);
            }

            if (pathDesc.HasDomainDescription()) {
                DomainInfo = new NSchemeCache::TDomainInfo(pathDesc.GetDomainDescription());
            }
        }

        void FillTableInfoFromOlapStore(const NKikimrSchemeOp::TPathDescription& pathDesc) {
            if (pathDesc.HasDomainDescription()) {
                DomainInfo = new NSchemeCache::TDomainInfo(pathDesc.GetDomainDescription());
            }
        }

        void FillTableInfoFromColumnTable(const NKikimrSchemeOp::TPathDescription& pathDesc) {
            const auto& desc = pathDesc.GetColumnTableDescription();

            THashMap<TString, ui32> nameToId;
            const auto& schemaDesc = desc.GetSchema();
            for (const auto& columnDesc : schemaDesc.GetColumns()) {
                auto& column = Columns[columnDesc.GetId()];
                column.Id = columnDesc.GetId();
                column.Name = columnDesc.GetName();
                auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(columnDesc.GetTypeId(),
                    columnDesc.HasTypeInfo() ? &columnDesc.GetTypeInfo() : nullptr);
                column.PType = typeInfoMod.TypeInfo;
                column.PTypeMod = typeInfoMod.TypeMod;
                nameToId[column.Name] = column.Id;
                if (columnDesc.GetNotNull()) {
                    NotNullColumns.insert(columnDesc.GetName());
                }
            }

            SchemaVersion = schemaDesc.GetVersion();
            KeyColumnTypes.resize(schemaDesc.KeyColumnNamesSize());
            for (ui32 i : xrange(schemaDesc.KeyColumnNamesSize())) {
                auto* pcolid = nameToId.FindPtr(schemaDesc.GetKeyColumnNames(i));
                Y_ABORT_UNLESS(pcolid);
                auto* column = Columns.FindPtr(*pcolid);
                Y_ABORT_UNLESS(column != nullptr);
                column->KeyOrder = i;
                KeyColumnTypes[i] = column->PType;
            }

            if (pathDesc.HasDomainDescription()) {
                DomainInfo = new NSchemeCache::TDomainInfo(pathDesc.GetDomainDescription());
            }
        }

        static TResolve::EKind PathSubTypeToTableKind(NKikimrSchemeOp::EPathSubType subType) {
            switch (subType) {
            case NKikimrSchemeOp::EPathSubTypeSyncIndexImplTable:
                return TResolve::KindSyncIndexTable;
            case NKikimrSchemeOp::EPathSubTypeAsyncIndexImplTable:
                return TResolve::KindAsyncIndexTable;
            case NKikimrSchemeOp::EPathSubTypeVectorKmeansTreeIndexImplTable:
                return TResolve::KindVectorIndexTable;
            default:
                return TResolve::KindRegularTable;
            }
        }

        static bool CalcPathIsPrivate(NKikimrSchemeOp::EPathType type, NKikimrSchemeOp::EPathSubType subType) {
            switch (type) {
            case NKikimrSchemeOp::EPathTypeTable:
                switch (subType) {
                case NKikimrSchemeOp::EPathSubTypeSyncIndexImplTable:
                case NKikimrSchemeOp::EPathSubTypeAsyncIndexImplTable:
                case NKikimrSchemeOp::EPathSubTypeVectorKmeansTreeIndexImplTable:
                    return true;
                default:
                    return false;
                }
            case NKikimrSchemeOp::EPathTypePersQueueGroup:
                switch (subType) {
                case NKikimrSchemeOp::EPathSubTypeStreamImpl:
                    return true;
                default:
                    return false;
                }
            case NKikimrSchemeOp::EPathTypeTableIndex:
                return true;
            default:
                return false;
            }
        }

        static bool IsLikeDirectory(TNavigate::EKind kind) {
            switch (kind) {
            case TNavigate::KindSubdomain:
            case TNavigate::KindPath:
            case TNavigate::KindIndex:
            case TNavigate::KindCdcStream:
                return true;
            default:
                return false;
            }
        }

        template <typename TPtr, typename TDesc>
        static void FillInfo(TNavigate::EKind kind, TPtr& ptr, TDesc&& desc) {
            ptr = new typename TPtr::TValueType();
            ptr->Kind = kind;
            ptr->Description = std::move(desc);
        }

        std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> FillRangePartitioning(const TTableRange& range) const {
            Y_ABORT_UNLESS(Partitioning);
            Y_ABORT_UNLESS(!Partitioning->empty());

            if (range.IsFullRange(KeyColumnTypes.size())) {
                return Partitioning;
            }

            auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();

            // Temporary fix: for an empty range we need to return some datashard
            // so that it can handle readset logic (send empty result to other tx participants etc.)
            if (range.IsEmptyRange(KeyColumnTypes)) {
                partitions->push_back(*Partitioning->begin());
                return partitions;
            }

            TVector<TKeyDesc::TPartitionInfo>::const_iterator low = LowerBound(
                Partitioning->begin(), Partitioning->end(), true,
                [&](const TKeyDesc::TPartitionInfo& left, bool) {
                    const int compares = CompareBorders<true, false>(
                        left.Range->EndKeyPrefix.GetCells(), range.From,
                        left.Range->IsInclusive || left.Range->IsPoint,
                        range.InclusiveFrom || range.Point, KeyColumnTypes
                    );

                    return (compares < 0);
                }
            );

            Y_ABORT_UNLESS(low != Partitioning->end(), "last key must be (inf)");

            do {
                partitions->push_back(*low);

                if (range.Point) {
                    return partitions;
                }

                const int prevComp = CompareBorders<true, true>(
                    low->Range->EndKeyPrefix.GetCells(), range.To,
                    low->Range->IsPoint || low->Range->IsInclusive,
                    range.InclusiveTo, KeyColumnTypes
                );

                if (prevComp >= 0) {
                    return partitions;
                }

            } while (++low != Partitioning->end());

            return partitions;
        }

        bool IsSysTable() const {
            return Kind == TNavigate::KindTable && PathId.OwnerId == TSysTables::SysSchemeShard;
        }

        void SetPathId(const TPathId pathId) {
            if (PathId) {
                Y_ABORT_UNLESS(PathId == pathId);
            }

            PathId = pathId;
        }

        void SendSyncRequest() const {
            Y_ABORT_UNLESS(Subscriber, "it hangs if no subscriber");
            Owner->Send(Subscriber.Subscriber, new NInternalEvents::TEvSyncRequest(), 0, ++Subscriber.SyncCookie);
        }

        void ResendSyncRequests(THashMap<TVariantContextPtr, TVector<TRequest>>& inFlight) const {
            for (auto& [_, requests] : inFlight) {
                for (auto& request : requests) {
                    if (!request.IsSync) {
                        continue;
                    }

                    SendSyncRequest();
                    request.Cookie = Subscriber.SyncCookie;
                }
            }
        }

        template <typename TContextPtr>
        void ProcessInFlightNoCheck(TContextPtr context, const TVector<TRequest>& requests) const {
            // trigger LookupError
            TResponseProps props(Max<ui64>(), true, true);

            for (const auto& request : requests) {
                auto& entry = context->Request->ResultSet[request.EntryIndex];
                FillEntry(context.Get(), entry, props);

                Y_ABORT_UNLESS(context->WaitCounter > 0);
                --context->WaitCounter;
            }

            if (!context->WaitCounter) {
                Owner->Complete(context);
            }
        }

    public:
        explicit TCacheItem(TSchemeCache* owner, const TSubscriber& subscriber, bool isVirtual)
            : Owner(owner)
            , Subscriber(subscriber)
            , Filled(false)
            , Kind(TNavigate::EKind::KindUnknown)
            , TableKind(TResolve::EKind::KindUnknown)
            , Created(false)
            , CreateStep(0)
            , IsPrivatePath(false)
            , IsVirtual(isVirtual)
            , SchemaVersion(0)
            , Partitioning(std::make_shared<TVector<TKeyDesc::TPartitionInfo>>())
        {
        }

        TCacheItem(const TCacheItem& other) = delete;

        explicit TCacheItem(TCacheItem&& other)
            : Owner(other.Owner)
            , Subscriber(other.Subscriber)
            , Filled(other.Filled)
            , InFlight(std::move(other.InFlight))
            , Kind(other.Kind)
            , TableKind(other.TableKind)
            , Created(other.Created)
            , CreateStep(other.CreateStep)
            , PathId(other.PathId)
            , IsPrivatePath(other.IsPrivatePath)
            , IsVirtual(other.IsVirtual)
            , SchemaVersion(other.SchemaVersion)
            , Partitioning(std::make_shared<TVector<TKeyDesc::TPartitionInfo>>())
        {
            if (other.Subscriber) {
                other.Subscriber = TSubscriber();
            }
        }

        TString ToString() const {
            return TStringBuilder() << "{"
                << " Subscriber: " << Subscriber.ToString()
                << " Filled: " << Filled
                << " Status: " << (Status ? NKikimrScheme::EStatus_Name(*Status) : "undefined")
                << " Kind: " << static_cast<ui32>(Kind)
                << " TableKind: " << static_cast<ui32>(TableKind)
                << " Created: " << Created
                << " CreateStep: " << CreateStep
                << " PathId: " << PathId
                << " DomainId: " << GetDomainId()
                << " IsPrivatePath: " << IsPrivatePath
                << " IsVirtual: " << IsVirtual
                << " SchemaVersion: " << SchemaVersion
            << " }";
        }

        template <typename T>
        static TString ProtoJsonString(const T& message) {
            using namespace google::protobuf::util;

            JsonPrintOptions opts;
            opts.preserve_proto_field_names = true;

            TString jsonString;
            MessageToJsonString(message, &jsonString, opts);

            return jsonString;
        }

        TString ToJsonString() const {
            NJsonWriter::TBuf json;
            auto root = json.BeginObject();

            root.WriteKey("Subscriber").BeginObject()
                .WriteKey("ActorId").WriteString(::ToString(Subscriber.Subscriber))
                .WriteKey("DomainOwnerId").WriteULongLong(Subscriber.DomainOwnerId)
                .WriteKey("Type").WriteInt(static_cast<int>(Subscriber.Type))
                .WriteKey("SyncCookie").WriteULongLong(Subscriber.SyncCookie)
            .EndObject();

            root.WriteKey("Filled").WriteBool(Filled);
            root.WriteKey("Path").WriteString(Path);
            root.WriteKey("PathId").WriteString(::ToString(PathId));
            root.WriteKey("Kind").WriteInt(static_cast<int>(Kind));
            root.WriteKey("TableKind").WriteInt(static_cast<int>(TableKind));
            root.WriteKey("Created").WriteBool(Created);
            root.WriteKey("CreateStep").WriteULongLong(CreateStep);
            root.WriteKey("IsPrivatePath").WriteBool(IsPrivatePath);
            root.WriteKey("IsVirtual").WriteBool(IsVirtual);
            root.WriteKey("SchemaVersion").WriteULongLong(SchemaVersion);

            if (Status) {
                root.WriteKey("Status").WriteString(NKikimrScheme::EStatus_Name(*Status));
            } else {
                root.WriteKey("Status").WriteNull();
            }

            if (Attributes) {
                auto attrs = root.WriteKey("Attributes").BeginObject();

                for (const auto& [key, value] : Attributes) {
                    attrs.WriteKey(key).WriteString(value);
                }

                attrs.EndObject();
            }

            if (AbandonedSchemeShardsIds) {
                auto abandoned = root.WriteKey("AbandonedSchemeShardsIds").BeginList();

                for (const auto ssId : AbandonedSchemeShardsIds) {
                    abandoned.WriteULongLong(ssId);
                }

                abandoned.EndList();
            }

            if (DomainInfo) {
                root.WriteKey("DomainInfo").BeginObject()
                    .WriteKey("DomainKey").WriteString(::ToString(DomainInfo->DomainKey))
                    .WriteKey("ResourcesDomainKey").WriteString(::ToString(DomainInfo->ResourcesDomainKey))
                    .WriteKey("Params").UnsafeWriteValue(ProtoJsonString(DomainInfo->Params))
                .EndObject();
            }

            if (Self) {
                root.WriteKey("Self").UnsafeWriteValue(ProtoJsonString(Self->Info));
            }

            if (ListNodeEntry) {
                auto children = root.WriteKey("Children").BeginList();

                for (const auto& child : ListNodeEntry->Children) {
                    children.BeginObject()
                        .WriteKey("Name").WriteString(child.Name)
                        .WriteKey("PathId").WriteString(::ToString(child.PathId))
                        .WriteKey("SchemaVersion").WriteULongLong(child.SchemaVersion)
                        .WriteKey("Kind").WriteInt(static_cast<int>(child.Kind))
                    .EndObject();
                }

                children.EndList();
            }

            if (Columns) {
                auto columns = root.WriteKey("Columns").BeginList();

                for (const auto& [_, column] : Columns) {
                    columns.BeginObject()
                        .WriteKey("Id").WriteULongLong(column.Id)
                        .WriteKey("Name").WriteString(column.Name)
                        .WriteKey("Type").WriteULongLong(column.PType.GetTypeId()) // TODO: support pg types
                        .WriteKey("KeyOrder").WriteInt(column.KeyOrder)
                    .EndObject();
                }

                columns.EndList();
            }

            #define DESCRIPTION_PART(name) \
                if (name) { \
                    root.WriteKey(#name).UnsafeWriteValue(ProtoJsonString(name->Description)); \
                }

            DESCRIPTION_PART(DomainDescription);
            DESCRIPTION_PART(RtmrVolumeInfo);
            DESCRIPTION_PART(KesusInfo);
            DESCRIPTION_PART(SolomonVolumeInfo);
            DESCRIPTION_PART(PQGroupInfo);
            DESCRIPTION_PART(OlapStoreInfo);
            DESCRIPTION_PART(ColumnTableInfo);
            DESCRIPTION_PART(CdcStreamInfo);
            DESCRIPTION_PART(SequenceInfo);
            DESCRIPTION_PART(ReplicationInfo);
            DESCRIPTION_PART(BlobDepotInfo);
            DESCRIPTION_PART(ExternalTableInfo);
            DESCRIPTION_PART(ExternalDataSourceInfo);
            DESCRIPTION_PART(BlockStoreVolumeInfo);
            DESCRIPTION_PART(FileStoreInfo);
            DESCRIPTION_PART(ViewInfo);
            DESCRIPTION_PART(ResourcePoolInfo);

            #undef DESCRIPTION_PART

            root.EndObject();
            return json.Str();
        }

        void OnEvict(bool respondInFlight = true) {
            if (Subscriber) {
                Owner->Send(Subscriber.Subscriber, new TEvents::TEvPoisonPill());
                Subscriber = TSubscriber();
            }

            if (!respondInFlight) {
                return;
            }

            for (const auto& [contextVariant, requests] : InFlight) {
                if (auto* context = std::get_if<TNavigateContextPtr>(&contextVariant)) {
                    ProcessInFlightNoCheck(*context, requests);
                } else if (auto* context = std::get_if<TResolveContextPtr>(&contextVariant)) {
                    ProcessInFlightNoCheck(*context, requests);
                } else {
                    Y_ABORT("unknown context type");
                }
            }
        }

        TCacheItem& Merge(TCacheItem&& other) noexcept {
            if (Subscriber < other.Subscriber) {
                SwapSubscriber(other.Subscriber);
            } else {
                ResendSyncRequests(other.InFlight);
            }

            InFlight.insert(other.InFlight.begin(), other.InFlight.end());
            other.OnEvict(false);

            return *this;
        }

        void SwapSubscriber(TSubscriber& subscriber) {
            std::swap(Subscriber, subscriber);
            ResendSyncRequests(InFlight);
        }

        void MoveInFlightNavigateByPathRequests(TCacheItem& recipient) {
            EraseNodesIf(InFlight, [&recipient](auto& kv) {
                auto* context = std::get_if<TNavigateContextPtr>(&kv.first);
                if (!context) {
                    return false;
                }

                EraseIf(kv.second, [context, &recipient](const TRequest& request) {
                    const auto& entry = context->Get()->Request->ResultSet[request.EntryIndex];
                    if (entry.RequestType != TNavigate::TEntry::ERequestType::ByPath) {
                        return false;
                    }

                    Y_ABORT_UNLESS(context->Get()->WaitCounter > 0);
                    --context->Get()->WaitCounter;

                    recipient.AddInFlight(*context, request.EntryIndex, request.IsSync);
                    return true;
                });

                return kv.second.empty();
            });
        }

        const TSubscriber& GetSubcriber() const {
            return Subscriber;
        }

        template <typename TContextPtr>
        void AddInFlight(TContextPtr context, const size_t entryIndex, const bool isSync) const {
            if (IsVirtual) {
                return;
            }

            if (isSync) {
                SendSyncRequest();
            }

            auto it = InFlight.find(context);
            if (it == InFlight.end()) {
                it = InFlight.emplace(context, TVector<TRequest>()).first;
            }

            it->second.push_back({entryIndex, Subscriber.SyncCookie, isSync});
            ++context->WaitCounter;
        }

        template <typename TContext>
        void ProcessInFlight(TContext* context, TVector<TRequest>& requests, const TResponseProps& response) const {
            EraseIf(requests, [this, context, response](const TRequest& request) {
                if (request.IsSync != response.IsSync) {
                    return false;
                }

                if (request.IsSync && request.Cookie > response.Cookie) {
                    return false;
                }

                auto& entry = context->Request->ResultSet[request.EntryIndex];
                if (!entry.SyncVersion || (entry.SyncVersion && response.IsSync)) {
                    FillEntry(context, entry, response);
                }

                Y_ABORT_UNLESS(context->WaitCounter > 0);
                --context->WaitCounter;

                return true;
            });
        }

        TVector<TVariantContextPtr> ProcessInFlight(const TResponseProps& response) const {
            TVector<TVariantContextPtr> processed(Reserve(InFlight.size()));

            EraseNodesIf(InFlight, [this, &processed, response](auto& kv) {
                processed.push_back(kv.first);

                if (auto* context = std::get_if<TNavigateContextPtr>(&kv.first)) {
                    ProcessInFlight(context->Get(), kv.second, response);
                } else if (auto* context = std::get_if<TResolveContextPtr>(&kv.first)) {
                    ProcessInFlight(context->Get(), kv.second, response);
                } else {
                    Y_ABORT("unknown context type");
                }

                return kv.second.empty();
            });

            return processed;
        }

        void FillAsSysPath() {
            Clear();
            Filled = true;

            Status = NKikimrScheme::StatusSuccess;
            Kind = TNavigate::KindPath;
            Created = true;
            PathId = TPathId(TSysTables::SysSchemeShard, 0);
            Path = "/sys";

            IsVirtual = true;
        }

        void FillAsSysLocks(const bool v2) {
            Clear();
            Filled = true;

            Status = NKikimrScheme::StatusSuccess;
            Kind = TNavigate::KindTable;
            Created = true;
            PathId = TPathId(TSysTables::SysSchemeShard, v2 ? TSysTables::SysTableLocks2 : TSysTables::SysTableLocks);
            Path = v2 ? "/sys/locks2" : "/sys/locks";

            TVector<NScheme::TTypeInfo> keyColumnTypes;
            TSysTables::TLocksTable::GetInfo(Columns, keyColumnTypes, v2);
            for (auto type : keyColumnTypes) {
                KeyColumnTypes.push_back(type);
            }

            IsPrivatePath = true;
            IsVirtual = true;
        }

        void Fill(TSchemeBoardEvents::TEvNotifyUpdate& notify) {
            Clear();
            Filled = true;

            Y_ABORT_UNLESS(notify.PathId);
            SetPathId(notify.PathId);

            if (!notify.DescribeSchemeResult.HasStatus()) {
                return;
            } else {
                Status = notify.DescribeSchemeResult.GetStatus();
                if (Status != NKikimrScheme::StatusSuccess) {
                    return;
                }
            }

            Y_ABORT_UNLESS(notify.DescribeSchemeResult.HasPathDescription());
            auto& pathDesc = *notify.DescribeSchemeResult.MutablePathDescription();

            Y_ABORT_UNLESS(notify.DescribeSchemeResult.HasPath());
            Path = notify.DescribeSchemeResult.GetPath();

            const auto& abandoned = pathDesc.GetAbandonedTenantsSchemeShards();
            AbandonedSchemeShardsIds = TSet<ui64>(abandoned.begin(), abandoned.end());

            Y_ABORT_UNLESS(pathDesc.HasSelf());
            const auto& entryDesc = pathDesc.GetSelf();
            Self = new TNavigate::TDirEntryInfo();
            Self->Info.CopyFrom(entryDesc);

            Created = entryDesc.HasCreateFinished() && entryDesc.GetCreateFinished();
            CreateStep = entryDesc.GetCreateStep();
            SecurityObject = new TSecurityObject(entryDesc.GetOwner(), entryDesc.GetEffectiveACL(), false);
            DomainInfo = new NSchemeCache::TDomainInfo(pathDesc.GetDomainDescription());

            for (const auto& attr : pathDesc.GetUserAttributes()) {
                Attributes[attr.GetKey()] = attr.GetValue();
            }

            auto tableSchemaVersion = [](const auto& entryDesc) {
                return entryDesc.HasVersion() ? entryDesc.GetVersion().GetTableSchemaVersion() : 0;
            };
            auto tableIndexVersion = [](const auto& entryDesc) {
                return entryDesc.HasVersion() ? entryDesc.GetVersion().GetTableIndexVersion() : 0;
            };

            switch (entryDesc.GetPathType()) {
            case NKikimrSchemeOp::EPathTypeSubDomain:
                Kind = TNavigate::KindSubdomain;
                FillInfo(Kind, DomainDescription, std::move(*pathDesc.MutableDomainDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeExtSubDomain:
                Kind = TNavigate::KindExtSubdomain;
                FillInfo(Kind, DomainDescription, std::move(*pathDesc.MutableDomainDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeDir:
                Kind = TNavigate::KindPath;
                if (entryDesc.GetPathId() == entryDesc.GetParentPathId()) {
                    FillInfo(Kind, DomainDescription, std::move(*pathDesc.MutableDomainDescription()));
                }
                break;
            case NKikimrSchemeOp::EPathTypeTable:
                Kind = TNavigate::KindTable;
                TableKind = PathSubTypeToTableKind(entryDesc.GetPathSubType());
                IsPrivatePath = CalcPathIsPrivate(entryDesc.GetPathType(), entryDesc.GetPathSubType());
                if (Created) {
                    FillTableInfo(pathDesc);
                    SchemaVersion = tableSchemaVersion(entryDesc);
                }
                break;
            case NKikimrSchemeOp::EPathTypeColumnStore:
                Kind = TNavigate::KindOlapStore;
                if (Created) {
                    FillTableInfoFromOlapStore(pathDesc);
                }
                FillInfo(Kind, OlapStoreInfo, std::move(*pathDesc.MutableColumnStoreDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeColumnTable:
                Kind = TNavigate::KindColumnTable;
                if (Created) {
                    FillTableInfoFromColumnTable(pathDesc);
                }
                FillInfo(Kind, ColumnTableInfo, std::move(*pathDesc.MutableColumnTableDescription()));
                if (ColumnTableInfo->Description.HasColumnStorePathId()) {
                    auto& p = ColumnTableInfo->Description.GetColumnStorePathId();
                    ColumnTableInfo->OlapStoreId = TTableId(p.GetOwnerId(), p.GetLocalId());
                }
                break;
            case NKikimrSchemeOp::EPathTypeTableIndex:
                Kind = TNavigate::KindIndex;
                IsPrivatePath = CalcPathIsPrivate(entryDesc.GetPathType(), entryDesc.GetPathSubType());
                SchemaVersion = tableIndexVersion(entryDesc);
                break;
            case NKikimrSchemeOp::EPathTypeRtmrVolume:
                Kind = TNavigate::KindRtmr;
                FillInfo(Kind, RtmrVolumeInfo, std::move(*pathDesc.MutableRtmrVolumeDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeKesus:
                Kind = TNavigate::KindKesus;
                FillInfo(Kind, KesusInfo, std::move(*pathDesc.MutableKesus()));
                break;
            case NKikimrSchemeOp::EPathTypeSolomonVolume:
                Kind = TNavigate::KindSolomon;
                FillInfo(Kind, SolomonVolumeInfo, std::move(*pathDesc.MutableSolomonDescription()));
                break;
            case NKikimrSchemeOp::EPathTypePersQueueGroup:
                Kind = TNavigate::KindTopic;
                IsPrivatePath = CalcPathIsPrivate(entryDesc.GetPathType(), entryDesc.GetPathSubType());
                if (Created) {
                    NPQ::Migrate(*pathDesc.MutablePersQueueGroup()->MutablePQTabletConfig());
                    FillInfo(Kind, PQGroupInfo, std::move(*pathDesc.MutablePersQueueGroup()));
                }
                break;
            case NKikimrSchemeOp::EPathTypeCdcStream:
                Kind = TNavigate::KindCdcStream;
                IsPrivatePath = CalcPathIsPrivate(entryDesc.GetPathType(), entryDesc.GetPathSubType());
                if (Created) {
                    FillInfo(Kind, CdcStreamInfo, std::move(*pathDesc.MutableCdcStreamDescription()));
                    if (CdcStreamInfo->Description.HasPathId()) {
                        const auto& pathId = CdcStreamInfo->Description.GetPathId();
                        CdcStreamInfo->PathId = TPathId(pathId.GetOwnerId(), pathId.GetLocalId());
                    }
                }
                break;
            case NKikimrSchemeOp::EPathTypeSequence:
                Kind = TNavigate::KindSequence;
                FillInfo(Kind, SequenceInfo, std::move(*pathDesc.MutableSequenceDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeReplication:
                Kind = TNavigate::KindReplication;
                FillInfo(Kind, ReplicationInfo, std::move(*pathDesc.MutableReplicationDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeBlobDepot:
                Kind = TNavigate::KindBlobDepot;
                FillInfo(Kind, BlobDepotInfo, std::move(*pathDesc.MutableBlobDepotDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeExternalTable:
                Kind = TNavigate::KindExternalTable;
                FillInfo(Kind, ExternalTableInfo, std::move(*pathDesc.MutableExternalTableDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeExternalDataSource:
                Kind = TNavigate::KindExternalDataSource;
                FillInfo(Kind, ExternalDataSourceInfo, std::move(*pathDesc.MutableExternalDataSourceDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeBlockStoreVolume:
                Kind = TNavigate::KindBlockStoreVolume;
                FillInfo(Kind, BlockStoreVolumeInfo, std::move(*pathDesc.MutableBlockStoreVolumeDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeFileStore:
                Kind = TNavigate::KindFileStore;
                FillInfo(Kind, FileStoreInfo, std::move(*pathDesc.MutableFileStoreDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeView:
                Kind = TNavigate::KindView;
                FillInfo(Kind, ViewInfo, std::move(*pathDesc.MutableViewDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeResourcePool:
                Kind = TNavigate::KindResourcePool;
                FillInfo(Kind, ResourcePoolInfo, std::move(*pathDesc.MutableResourcePoolDescription()));
                break;
            case NKikimrSchemeOp::EPathTypeInvalid:
                Y_DEBUG_ABORT("Invalid path type");
                break;
            }

            if (IsLikeDirectory(Kind)) {
                ListNodeEntry = new TNavigate::TListNodeEntry();
                ListNodeEntry->Kind = TNavigate::KindPath;
                ListNodeEntry->Children.reserve(pathDesc.ChildrenSize());

                for (const auto& child : pathDesc.GetChildren()) {
                    const auto& name = child.GetName();
                    const auto pathId = TPathId(child.GetSchemeshardId(), child.GetPathId());

                    switch (child.GetPathType()) {
                    case NKikimrSchemeOp::EPathTypeSubDomain:
                    case NKikimrSchemeOp::EPathTypeDir:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindPath);
                        break;
                    case NKikimrSchemeOp::EPathTypeExtSubDomain:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindExtSubdomain);
                        break;
                    case NKikimrSchemeOp::EPathTypeTable:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindTable, tableSchemaVersion(child));
                        break;
                    case NKikimrSchemeOp::EPathTypeColumnStore:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindOlapStore);
                        break;
                    case NKikimrSchemeOp::EPathTypeColumnTable:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindColumnTable);
                        break;
                    case NKikimrSchemeOp::EPathTypeRtmrVolume:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindRtmr);
                        break;
                    case NKikimrSchemeOp::EPathTypeKesus:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindKesus);
                        break;
                    case NKikimrSchemeOp::EPathTypeSolomonVolume:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindSolomon);
                        break;
                    case NKikimrSchemeOp::EPathTypePersQueueGroup:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindTopic);
                        break;
                    case NKikimrSchemeOp::EPathTypeCdcStream:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindCdcStream);
                        break;
                    case NKikimrSchemeOp::EPathTypeSequence:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindSequence);
                        break;
                    case NKikimrSchemeOp::EPathTypeReplication:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindReplication);
                        break;
                    case NKikimrSchemeOp::EPathTypeBlobDepot:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindBlobDepot);
                        break;
                    case NKikimrSchemeOp::EPathTypeExternalTable:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindExternalTable);
                        break;
                    case NKikimrSchemeOp::EPathTypeExternalDataSource:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindExternalDataSource);
                        break;
                    case NKikimrSchemeOp::EPathTypeBlockStoreVolume:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindBlockStoreVolume);
                        break;
                    case NKikimrSchemeOp::EPathTypeFileStore:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindFileStore);
                        break;
                    case NKikimrSchemeOp::EPathTypeView:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindView);
                        break;
                    case NKikimrSchemeOp::EPathTypeResourcePool:
                        ListNodeEntry->Children.emplace_back(name, pathId, TNavigate::KindResourcePool);
                        break;
                    case NKikimrSchemeOp::EPathTypeTableIndex:
                    case NKikimrSchemeOp::EPathTypeInvalid:
                        Y_DEBUG_ABORT("Invalid path type");
                        break;
                    }
                }
            }
        }

        void Fill(TSchemeBoardEvents::TEvNotifyDelete& notify) {
            Clear();
            Filled = true;

            if (notify.PathId) {
                SetPathId(notify.PathId);
            }

            Y_DEBUG_ABORT_UNLESS(Subscriber.DomainOwnerId);
            if (notify.Strong) {
                Status = NKikimrScheme::StatusPathDoesNotExist;
            }
        }

        void Fill(NInternalEvents::TEvSyncResponse&) {
        }

        bool IsFilled() const {
            return Filled;
        }

        TMaybe<NKikimrScheme::EStatus> GetStatus() const {
            return Status;
        }

        TPathId GetPathId() const {
            return IsFilled() ? PathId : TPathId();
        }

        TDomainId GetDomainId() const {
            return (IsFilled() && DomainInfo) ? DomainInfo->DomainKey : TDomainId();
        }

        auto GetDomainInfo() const {
            return DomainInfo;
        }

        const TSet<ui64>& GetAbandonedSchemeShardIds() const {
            return AbandonedSchemeShardsIds;
        }

        void FillSystemViewEntry(TNavigateContext* context, TNavigate::TEntry& entry,
            NSysView::ISystemViewResolver::ETarget target) const
        {
            auto sysViewInfo = entry.TableId.SysViewInfo;

            if (sysViewInfo == NSysView::SysPathName) {
                if (entry.Operation == TNavigate::OpTable) {
                    return SetError(context, entry, TNavigate::EStatus::PathNotTable);
                }

                auto listNodeEntry = MakeIntrusive<TNavigate::TListNodeEntry>();

                auto names = Owner->SystemViewResolver->GetSystemViewNames(target);
                std::sort(names.begin(), names.end());

                listNodeEntry->Kind = TNavigate::KindPath;
                listNodeEntry->Children.reserve(names.size());
                for (const auto& name : names) {
                    listNodeEntry->Children.emplace_back(name, TPathId(), TNavigate::KindTable);
                }

                entry.Kind = TNavigate::KindPath;
                entry.ListNodeEntry = listNodeEntry;
                entry.TableId = TTableId(PathId.OwnerId, InvalidLocalPathId, sysViewInfo);

            } else {
                auto schema = Owner->SystemViewResolver->GetSystemViewSchema(sysViewInfo, target);
                if (!schema) {
                    return SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
                }

                entry.Kind = TNavigate::KindTable;
                if (target == NSysView::ISystemViewResolver::ETarget::OlapStore ||
                    target == NSysView::ISystemViewResolver::ETarget::ColumnTable)
                {
                    // OLAP sys views are represented by OLAP tables
                    entry.Kind =TNavigate::KindColumnTable;
                }

                entry.Columns = std::move(schema->Columns);

                if (entry.RequestType == TNavigate::TEntry::ERequestType::ByPath) {
                    entry.TableId = TTableId(PathId.OwnerId, PathId.LocalPathId, sysViewInfo);
                } else {
                    entry.Path = SplitPath(Path);
                    entry.Path.emplace_back(NKikimr::NSysView::SysPathName);
                    entry.Path.emplace_back(sysViewInfo);
                }
            }

            entry.Status = TNavigate::EStatus::Ok;

            entry.SecurityObject = SecurityObject;
            entry.DomainInfo = DomainInfo;
        }

        void FillEntry(TNavigateContext* context, TNavigate::TEntry& entry, const TResponseProps& props = TResponseProps()) const {
            SBC_LOG_D("FillEntry for TNavigate"
                << ": self# " << Owner->SelfId()
                << ", cacheItem# " << ToString()
                << ", entry# " << entry.ToString()
                << ", props# " << props.ToString());

            if (props.IsSync && props.Partial) {
                return SetError(context, entry, TNavigate::EStatus::LookupError);
            }

            if (Status && Status == NKikimrScheme::StatusPathDoesNotExist) {
                return SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
            }

            if (!Status || Status != NKikimrScheme::StatusSuccess) {
                return SetError(context, entry, TNavigate::EStatus::LookupError);
            }

            if (!entry.TableId.SysViewInfo.empty()) {
                if (Kind == TNavigate::KindPath) {
                    auto split = SplitPath(Path);
                    if (split.size() == 1) {
                        return FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::Domain);
                    }
                } else if (Kind == TNavigate::KindSubdomain || Kind == TNavigate::KindExtSubdomain) {
                    return FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::SubDomain);
                } else if (Kind == TNavigate::KindOlapStore) {
                    FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::OlapStore);
                    entry.OlapStoreInfo = OlapStoreInfo;
                    return;
                } else if (Kind == TNavigate::KindColumnTable) {
                    FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::ColumnTable);
                    entry.OlapStoreInfo = OlapStoreInfo;
                    entry.ColumnTableInfo = ColumnTableInfo;
                    return;
                }

                return SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
            }

            const bool isTable = Kind == TNavigate::KindTable
                || Kind == TNavigate::KindColumnTable
                || Kind == TNavigate::KindExternalTable
                || Kind == TNavigate::KindExternalDataSource
                || Kind == TNavigate::KindView;
            const bool isTopic = Kind == TNavigate::KindTopic
                || Kind == TNavigate::KindCdcStream;

            if (entry.Operation == TNavigate::OpTable && !isTable) {
                return SetError(context, entry, TNavigate::EStatus::PathNotTable);
            }

            if (!Created && (isTable || isTopic)) {
                return SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
            }

            if (!entry.ShowPrivatePath && IsPrivatePath) {
                return SetError(context, entry, TNavigate::EStatus::PathErrorUnknown);
            }

            // common
            entry.SecurityObject = SecurityObject;
            entry.DomainInfo = DomainInfo;
            entry.Attributes = Attributes;

            entry.Status = TNavigate::EStatus::Ok;

            if (Kind == TNavigate::KindExtSubdomain && entry.RedirectRequired) {
                SetError(context, entry, TNavigate::EStatus::RedirectLookupError);
            }

            entry.Kind = Kind;
            entry.CreateStep = CreateStep;

            if (entry.RequestType == TNavigate::TEntry::ERequestType::ByPath) {
                if (Kind == TNavigate::KindTable || Kind == TNavigate::KindColumnTable) {
                    entry.TableId = TTableId(PathId.OwnerId, PathId.LocalPathId, SchemaVersion);
                } else {
                    entry.TableId = TTableId(PathId.OwnerId, PathId.LocalPathId);
                }
            } else {
                entry.Path = SplitPath(Path);
                // update schema version
                entry.TableId.SchemaVersion = SchemaVersion;

            }

            if (entry.Operation == TNavigate::OpList) {
                entry.ListNodeEntry = ListNodeEntry;
            }

            // specific (it's safe to fill them all)
            entry.Self = Self;
            entry.Columns = Columns;
            entry.NotNullColumns = NotNullColumns;
            entry.Indexes = Indexes;
            entry.CdcStreams = CdcStreams;
            entry.DomainDescription = DomainDescription;
            entry.RTMRVolumeInfo = RtmrVolumeInfo;
            entry.KesusInfo = KesusInfo;
            entry.SolomonVolumeInfo = SolomonVolumeInfo;
            entry.PQGroupInfo = PQGroupInfo;
            entry.OlapStoreInfo = OlapStoreInfo;
            entry.ColumnTableInfo = ColumnTableInfo;
            entry.CdcStreamInfo = CdcStreamInfo;
            entry.SequenceInfo = SequenceInfo;
            entry.ReplicationInfo = ReplicationInfo;
            entry.BlobDepotInfo = BlobDepotInfo;
            entry.ExternalTableInfo = ExternalTableInfo;
            entry.ExternalDataSourceInfo = ExternalDataSourceInfo;
            entry.BlockStoreVolumeInfo = BlockStoreVolumeInfo;
            entry.FileStoreInfo = FileStoreInfo;
            entry.ViewInfo = ViewInfo;
            entry.ResourcePoolInfo = ResourcePoolInfo;
        }

        bool CheckColumns(TResolveContext* context, TResolve::TEntry& entry,
            const TVector<NScheme::TTypeInfo>& keyColumnTypes,
            const THashMap<ui32, TSysTables::TTableColumnInfo>& columns) const
        {
            TKeyDesc& keyDesc = *entry.KeyDescription;

            // check key types
            if (keyDesc.KeyColumnTypes.size() > keyColumnTypes.size()) {
                SetError(context, entry, TResolve::EStatus::TypeCheckError, TKeyDesc::EStatus::TypeCheckFailed);
                return false;
            }

            for (ui32 i : xrange(keyDesc.KeyColumnTypes.size())) {
                if (keyDesc.KeyColumnTypes[i] != keyColumnTypes[i]) {
                    SetError(context, entry, TResolve::EStatus::TypeCheckError, TKeyDesc::EStatus::TypeCheckFailed);
                    return false;
                }
            }

            // check operations
            keyDesc.ColumnInfos.clear();
            keyDesc.ColumnInfos.reserve(keyDesc.Columns.size());
            for (auto& columnOp : keyDesc.Columns) {
                if (IsSystemColumn(columnOp.Column)) {
                    continue;
                }

                if (IsSysTable() && columnOp.Operation == TKeyDesc::EColumnOperation::InplaceUpdate) {
                    SetError(context, entry, TResolve::EStatus::TypeCheckError, TKeyDesc::EStatus::OperationNotSupported);
                    return false;
                }

                const auto* column = columns.FindPtr(columnOp.Column);

                if (!column) {
                    entry.Status = TResolve::EStatus::TypeCheckError;
                    keyDesc.Status = TKeyDesc::EStatus::TypeCheckFailed;
                    keyDesc.ColumnInfos.push_back({
                        columnOp.Column, NScheme::TTypeInfo(), 0, TKeyDesc::EStatus::NotExists
                    });
                    continue;
                }

                if (column->PType != columnOp.ExpectedType) {
                    entry.Status = TResolve::EStatus::TypeCheckError;
                    keyDesc.Status = TKeyDesc::EStatus::TypeCheckFailed;
                    keyDesc.ColumnInfos.push_back({
                        columnOp.Column, column->PType, 0, TKeyDesc::EStatus::TypeCheckFailed
                    });
                    continue;
                }

                if (columnOp.Operation == TKeyDesc::EColumnOperation::InplaceUpdate) {
                    entry.Status = TResolve::EStatus::TypeCheckError;
                    keyDesc.Status = TKeyDesc::EStatus::OperationNotSupported;
                    keyDesc.ColumnInfos.push_back({
                        columnOp.Column, column->PType, 0, TKeyDesc::EStatus::OperationNotSupported
                    });
                    continue;
                }
            }

            return true;
        }

        void FillSystemViewEntry(TResolveContext* context, TResolve::TEntry& entry,
            NSysView::ISystemViewResolver::ETarget target) const
        {
            TKeyDesc& keyDesc = *entry.KeyDescription;
            auto sysViewInfo = keyDesc.TableId.SysViewInfo;

            auto schema = Owner->SystemViewResolver->GetSystemViewSchema(sysViewInfo, target);
            if (!schema) {
                return SetError(context, entry, TResolve::EStatus::PathErrorNotExist, TKeyDesc::EStatus::NotExists);
            }

            if (!CheckColumns(context, entry, schema->KeyColumnTypes, schema->Columns)) {
                return;
            }

            if (keyDesc.Status != TKeyDesc::EStatus::Unknown) {
                ++context->Request->ErrorCount;
                return;
            }

            entry.DomainInfo = DomainInfo;
            keyDesc.SecurityObject = SecurityObject;

            entry.Status = TResolve::EStatus::OkData;
            keyDesc.Status = TKeyDesc::EStatus::Ok;
        }

        void FillEntry(TResolveContext* context, TResolve::TEntry& entry, const TResponseProps& props = TResponseProps()) const {
            SBC_LOG_D("FillEntry for TResolve"
                << ": self# " << Owner->SelfId()
                << ", cacheItem# " << ToString()
                << ", entry# " << entry.ToString()
                << ", props# " << props.ToString());

            TKeyDesc& keyDesc = *entry.KeyDescription;

            if (props.IsSync && props.Partial) {
                return SetError(context, entry, TResolve::EStatus::LookupError, TKeyDesc::EStatus::NotExists);
            }

            if (Status && Status == NKikimrScheme::StatusPathDoesNotExist) {
                return SetError(context, entry, TResolve::EStatus::PathErrorNotExist, TKeyDesc::EStatus::NotExists);
            }

            if (!Status || Status != NKikimrScheme::StatusSuccess) {
                return SetError(context, entry, TResolve::EStatus::LookupError, TKeyDesc::EStatus::NotExists);
            }

            if (!keyDesc.TableId.SysViewInfo.empty()) {
                if (Kind == TNavigate::KindPath) {
                    auto split = SplitPath(Path);
                    if (split.size() == 1) {
                        return FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::Domain);
                    }
                } else if (Kind == TNavigate::KindSubdomain || Kind == TNavigate::KindExtSubdomain) {
                    return FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::SubDomain);
                } else if (Kind == TNavigate::KindOlapStore) {
                    FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::OlapStore);
                    // Add all shards of the OLAP store
                    auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
                    for (ui64 columnShard : OlapStoreInfo->Description.GetColumnShards()) {
                        partitions->push_back(TKeyDesc::TPartitionInfo(columnShard));
                        partitions->back().Range = TKeyDesc::TPartitionRangeInfo();
                    }
                    keyDesc.Partitioning = std::move(partitions);
                    return;
                } else if (Kind == TNavigate::KindColumnTable) {
                    FillSystemViewEntry(context, entry, NSysView::ISystemViewResolver::ETarget::ColumnTable);
                    // Add all shards of the OLAP table
                    auto shardingInfo = NSharding::IShardingBase::BuildFromProto(ColumnTableInfo->Description.GetSharding());
                    if (shardingInfo.IsFail()) {
                        return SetError(context, entry, TResolve::EStatus::PathErrorUnknown, TKeyDesc::EStatus::NotExists);
                    }
                    auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
                    for (ui64 columnShard : (*shardingInfo)->GetActiveReadShardIds()) {
                        partitions->push_back(TKeyDesc::TPartitionInfo(columnShard));
                        partitions->back().Range = TKeyDesc::TPartitionRangeInfo();
                    }
                    keyDesc.Partitioning = std::move(partitions);
                    return;
                }

                return SetError(context, entry, TResolve::EStatus::PathErrorNotExist, TKeyDesc::EStatus::NotExists);
            }

            if (!Created) {
                return SetError(context, entry, TResolve::EStatus::NotMaterialized, TKeyDesc::EStatus::NotExists);
            }

            entry.Kind = TableKind;
            entry.DomainInfo = DomainInfo;

            if (Self) {
                entry.GeneralVersion = Self->Info.GetVersion().GetGeneralVersion();
            }

            if (!CheckColumns(context, entry, KeyColumnTypes, Columns)) {
                return;
            }

            // fill partition info
            if (IsSysTable()) {
                if (keyDesc.Range.Point) {
                    ui64 shard = 0;
                    bool ok = TSysTables::TLocksTable::ExtractKey(
                        keyDesc.Range.From, TSysTables::TLocksTable::EColumns::DataShard, shard
                    );
                    if (ok) {
                        auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
                        partitions->push_back(TKeyDesc::TPartitionInfo(shard));
                        keyDesc.Partitioning = std::move(partitions);
                    } else {
                        keyDesc.Status = TKeyDesc::EStatus::OperationNotSupported;
                        ++context->Request->ErrorCount;
                    }
                }
            } else if (ColumnTableInfo) {
                // TODO: return proper partitioning info (KIKIMR-11069)
                auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
                for (ui64 columnShard : ColumnTableInfo->Description.GetSharding().GetColumnShards()) {
                    partitions->push_back(TKeyDesc::TPartitionInfo(columnShard));
                    partitions->back().Range = TKeyDesc::TPartitionRangeInfo();
                }
                keyDesc.Partitioning = std::move(partitions);
            } else {
                if (Partitioning) {
                    keyDesc.Partitioning = FillRangePartitioning(keyDesc.Range);
                }
            }

            if (keyDesc.GetPartitions().empty()) {
                entry.Status = TResolve::EStatus::TypeCheckError;
                keyDesc.Status = TKeyDesc::EStatus::OperationNotSupported;
            }

            // fill ACL info
            keyDesc.SecurityObject = SecurityObject;

            if (keyDesc.Status != TKeyDesc::EStatus::Unknown) {
                ++context->Request->ErrorCount;
                return;
            }

            entry.Status = TResolve::EStatus::OkData;
            keyDesc.Status = TKeyDesc::EStatus::Ok;
        }

    private:
        // internal
        TSchemeCache* Owner;
        TSubscriber Subscriber;
        bool Filled;

        mutable THashMap<TVariantContextPtr, TVector<TRequest>> InFlight;

        // common
        TMaybe<NKikimrScheme::EStatus> Status;
        TNavigate::EKind Kind;
        TResolve::EKind TableKind;
        bool Created;
        ui64 CreateStep;
        TPathId PathId;
        TString Path;
        TSet<ui64> AbandonedSchemeShardsIds;
        TIntrusivePtr<TSecurityObject> SecurityObject;
        NSchemeCache::TDomainInfo::TPtr DomainInfo;
        THashMap<TString, TString> Attributes;
        bool IsPrivatePath;
        bool IsVirtual;

        // Used for Table and Index
        ui64 SchemaVersion;

        // domain & path specific
        TIntrusivePtr<TNavigate::TListNodeEntry> ListNodeEntry;
        TIntrusivePtr<TNavigate::TDomainDescription> DomainDescription;

        // table specific
        THashMap<ui32, TSysTables::TTableColumnInfo> Columns;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
        THashSet<TString> NotNullColumns;
        TVector<NKikimrSchemeOp::TIndexDescription> Indexes;
        TVector<NKikimrSchemeOp::TCdcStreamDescription> CdcStreams;
        std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;

        TIntrusivePtr<TNavigate::TDirEntryInfo> Self;

        // RTMR specific
        TIntrusivePtr<TNavigate::TRtmrVolumeInfo> RtmrVolumeInfo;

        // Kesus specific
        TIntrusivePtr<TNavigate::TKesusInfo> KesusInfo;

        // Solomon specific
        TIntrusivePtr<TNavigate::TSolomonVolumeInfo> SolomonVolumeInfo;

        // PQ specific
        TIntrusivePtr<TNavigate::TPQGroupInfo> PQGroupInfo;

        // OlapStore specific
        TIntrusivePtr<TNavigate::TOlapStoreInfo> OlapStoreInfo;
        TIntrusivePtr<TNavigate::TColumnTableInfo> ColumnTableInfo;

        // CDC specific
        TIntrusivePtr<TNavigate::TCdcStreamInfo> CdcStreamInfo;

        // Sequence specific
        TIntrusivePtr<TNavigate::TSequenceInfo> SequenceInfo;

        // Replication specific
        TIntrusivePtr<TNavigate::TReplicationInfo> ReplicationInfo;

        // BlobDepot specific
        TIntrusivePtr<TNavigate::TBlobDepotInfo> BlobDepotInfo;

        // ExternalTable specific
        TIntrusivePtr<TNavigate::TExternalTableInfo> ExternalTableInfo;

        // ExternalDataSource specific
        TIntrusivePtr<TNavigate::TExternalDataSourceInfo> ExternalDataSourceInfo;

        // NBS specific
        TIntrusivePtr<TNavigate::TBlockStoreVolumeInfo> BlockStoreVolumeInfo;
        TIntrusivePtr<TNavigate::TFileStoreInfo> FileStoreInfo;

        // View specific
        TIntrusivePtr<TNavigate::TViewInfo> ViewInfo;

        // ResourcePool specific
        TIntrusivePtr<TNavigate::TResourcePoolInfo> ResourcePoolInfo;

    }; // TCacheItem

    struct TMerger {
        TCacheItem& operator()(TCacheItem& dst, TCacheItem&& src) {
            return dst.Merge(std::move(src));
        }
    };

    struct TEvicter {
        void operator()(TCacheItem& val) {
            return val.OnEvict();
        }
    };

    enum class EPathType {
        RegularPath,
        SysPath,
        SysLocksV1,
        SysLocksV2,
    };

    static EPathType PathType(const TStringBuf path) {
        if (path == "/sys"sv) {
            return EPathType::SysPath;
        } else if (path == "/sys/locks"sv) {
            return EPathType::SysLocksV1;
        } else if (path == "/sys/locks2"sv) {
            return EPathType::SysLocksV2;
        }

        return EPathType::RegularPath;
    }

    static EPathType PathType(const TPathId& pathId) {
        if (pathId == TPathId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks)) {
            return EPathType::SysLocksV1;
        } else if (pathId == TPathId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks2)) {
            return EPathType::SysLocksV2;
        }

        return EPathType::RegularPath;
    }

    static TInstant Now() {
        return TlsActivationContext->Now();
    }

    template <typename TPath>
    TSubscriber CreateSubscriber(const TPath& path, const ui64 domainOwnerId) const {
        SBC_LOG_T("Create subscriber"
            << ": self# " << SelfId()
            << ", path# " << path
            << ", domainOwnerId# " << domainOwnerId);

        return TSubscriber(
            Register(CreateSchemeBoardSubscriber(SelfId(), path, domainOwnerId)), domainOwnerId, path
        );
    }

    template <typename TContextPtr, typename TEntry, typename TPathExtractor, typename TTabletIdExtractor>
    void HandleEntry(TContextPtr context, TEntry& entry, size_t index,
        TPathExtractor pathExtractor, TTabletIdExtractor tabletIdExtractor)
    {
        auto path = pathExtractor(entry);
        TCacheItem* cacheItem = Cache.FindPtr(path);

        if (!cacheItem) {
            const EPathType pathType = PathType(path);
            switch (pathType) {
            case EPathType::RegularPath:
            {
                const ui64 tabletId = tabletIdExtractor(entry);
                if (tabletId == ui64(NSchemeShard::InvalidTabletId) || (tabletId >> 56) != 1) {
                    return SetRootUnknown(context.Get(), entry);
                }

                ui64 domainOwnerId = context->Request->DomainOwnerId;
                if (!domainOwnerId && context->Request->DatabaseName) {
                    TCacheItem* dbItem = Cache.FindPtr(CanonizePath(context->Request->DatabaseName));
                    if (!dbItem) {
                        return SetRootUnknown(context.Get(), entry);
                    }

                    auto status = dbItem->GetStatus();
                    if (status && status == NKikimrScheme::StatusPathDoesNotExist) {
                        return SetPathNotExist(context.Get(), entry);
                    }

                    if (!status || status != NKikimrScheme::StatusSuccess) {
                        return SetLookupError(context.Get(), entry);
                    }

                    Y_ABORT_UNLESS(dbItem->GetDomainInfo());
                    domainOwnerId = dbItem->GetDomainInfo()->ExtractSchemeShard();
                }

                if (!domainOwnerId) {
                    domainOwnerId = tabletId;
                }

                cacheItem = &Cache.Upsert(path, TCacheItem(this, CreateSubscriber(path, domainOwnerId), false));
                break;
            }
            case EPathType::SysPath:
                cacheItem = &Cache.Upsert(path, TCacheItem(this, TSubscriber(), true));
                cacheItem->FillAsSysPath();
                break;
            case EPathType::SysLocksV1:
            case EPathType::SysLocksV2:
                cacheItem = &Cache.Upsert(path, TCacheItem(this, TSubscriber(), true));
                cacheItem->FillAsSysLocks(pathType == EPathType::SysLocksV2);
                break;
            }
        }

        Cache.Promote(path);

        if (cacheItem->IsFilled() && !entry.SyncVersion) {
            cacheItem->FillEntry(context.Get(), entry);
        }

        if (entry.SyncVersion) {
            cacheItem->AddInFlight(context, index, true);
        }

        if (!cacheItem->IsFilled()) {
            cacheItem->AddInFlight(context, index, false);
        }
    }

    TCacheItem* SwapSubscriberAndUpsert(TCacheItem* byPath, const TPathId& notifyPathId, const TString& notifyPath) {
        TSubscriber subscriber = CreateSubscriber(byPath->GetPathId(), byPath->GetSubcriber().DomainOwnerId);
        byPath->SwapSubscriber(subscriber);

        TCacheItem newItem(this, subscriber, false);
        byPath->MoveInFlightNavigateByPathRequests(newItem);

        Cache.DeleteIndex(notifyPath);
        return &Cache.Upsert(notifyPath, notifyPathId, std::move(newItem));
    }

    TCacheItem* ResolveCacheItemCommon(const TPathId& notifyPathId, const TString& notifyPath) {
        if (!notifyPathId) {
            return Cache.FindPtr(notifyPath);
        }

        if (!notifyPath) {
            return Cache.FindPtr(notifyPathId);
        }

        TCacheItem* byPath = Cache.FindPtr(notifyPath);
        TCacheItem* byPathId = Cache.FindPtr(notifyPathId);

        if (byPath == byPathId) {
            return byPathId;
        }

        if (!byPath) {
            TSubscriber subscriber = CreateSubscriber(notifyPath, byPathId->GetSubcriber().DomainOwnerId);
            return &Cache.Upsert(notifyPath, notifyPathId, TCacheItem(this, subscriber, false));
        }

        TCacheItem* byPathByPathId = Cache.FindPtr(byPath->GetPathId());

        if (!byPathByPathId) {
            return &Cache.Upsert(notifyPath, notifyPathId, TCacheItem(this, TSubscriber(), false));
        }

        Y_ABORT_UNLESS(byPath == byPathByPathId);

        if (!byPath->IsFilled() || byPath->GetPathId().OwnerId == notifyPathId.OwnerId) {
            if (byPath->GetPathId() < notifyPathId) {
                return SwapSubscriberAndUpsert(byPath, notifyPathId, notifyPath);
            }

            return byPathId;
        }

        return nullptr;
    }

    template <typename TProtoNotify>
    TCacheItem* ResolveCacheItem(const TProtoNotify& notify,
                                 const TDomainId& notifyDomainId = {}, const TSet<ui64>& abandonedSchemeShardIds = {})
    {
        TCacheItem* byPath = Cache.FindPtr(notify.Path);
        TCacheItem* byPathId = Cache.FindPtr(notify.PathId);

        SBC_LOG_D("ResolveCacheItem"
            << ": self# " << SelfId()
            << ", notify# " << notify.ToString()
            << ", by path# " << (byPath ? byPath->ToString() : "nullptr")
            << ", by pathId# " << (byPathId ? byPathId->ToString() : "nullptr"));

        TCacheItem* commonResolve = ResolveCacheItemCommon(notify.PathId, notify.Path);
        if (commonResolve) {
            return commonResolve;
        }

        if (!byPath) {
            return nullptr;
        }

        Y_ABORT_UNLESS(byPath);
        Y_ABORT_UNLESS(byPathId != byPath);

        if (!byPath->GetDomainId() && notifyDomainId) {
            return SwapSubscriberAndUpsert(byPath, notify.PathId, notify.Path);
        }

        SBC_LOG_D("ResolveCacheItemForNotify: subdomain case"
                  << ": self# " << SelfId()
                  << ", path# " << notify.Path
                  << ", pathId# " << notify.PathId
                  << ", byPath# " << (byPath ? byPath->ToString() : "nullptr")
                  << ", byPathId# " << (byPathId ? byPathId->ToString() : "nullptr"));

        if (byPath->GetDomainId() != notifyDomainId && notifyDomainId) {
            SBC_LOG_D("ResolveCacheItemForNotify: recreation domain case"
                << ": self# " << SelfId()
                << ", path# " << notify.Path
                << ", pathId# " << notify.PathId
                << ", byPath# " << (byPath ? byPath->ToString() : "nullptr")
                << ", byPathId# " << (byPathId ? byPathId->ToString() : "nullptr"));

            if (byPath->GetPathId() < notify.PathId) {
                return SwapSubscriberAndUpsert(byPath, notify.PathId, notify.Path);
            }

            return byPathId;
        }

        if (byPath->GetPathId() == notifyDomainId) { //Update from TSS, GSS->TSS
            if (byPath->GetAbandonedSchemeShardIds().contains(notify.PathId.OwnerId)) {
                SBC_LOG_D("ResolveCacheItemForNotify: this is update from TSS, the update is ignored, present GSS reverted implicitly that TSS"
                    << ": self# " << SelfId()
                    << ", path# " << notify.Path
                    << ", pathId# " << notify.PathId);
                return byPathId;
            }

            SBC_LOG_D("ResolveCacheItemForNotify: this is update from TSS, the update owerrides GSS by path"
                << ": self# " << SelfId()
                << ", path# " << notify.Path
                << ", pathId# " << notify.PathId);
            return SwapSubscriberAndUpsert(byPath, notify.PathId, notify.Path);
        }

        if (byPath->GetDomainId() == notify.PathId) { //Update from GSS, TSS->GSS
            if (abandonedSchemeShardIds.contains(byPath->GetPathId().OwnerId)) { //GSS reverts TSS
                SBC_LOG_D("ResolveCacheItemForNotify: this is update from GSS, the update owerrides TSS by path, GSS implicilty reverts that TSS"
                    << ": self# " << SelfId()
                    << ", path# " << notify.Path
                    << ", pathId# " << notify.PathId);
                return SwapSubscriberAndUpsert(byPath, notify.PathId, notify.Path);
            }

            if (!notifyDomainId) {
                SBC_LOG_D("ResolveCacheItemForNotify: this is update from GSS that removes TSS"
                    << ": self# " << SelfId()
                    << ", path# " << notify.Path
                    << ", pathId# " << notify.PathId);
                return SwapSubscriberAndUpsert(byPath, notify.PathId, notify.Path);
            }

            SBC_LOG_D("ResolveCacheItemForNotify: this is update from GSS, the update us ignored, TSS is prefered"
                << ": self# " << SelfId()
                << ", path# " << notify.Path
                << ", pathId# " << notify.PathId);
            return byPathId;
        }

        if (byPath->GetDomainId() == notifyDomainId && notifyDomainId) {
            SBC_LOG_D("ResolveCacheItemForNotify: recreation migrated path case"
                      << ": self# " << SelfId()
                      << ", path# " << notify.Path
                      << ", pathId# " << notify.PathId
                      << ", byPath# " << (byPath ? byPath->ToString() : "nullptr")
                      << ", byPathId# " << (byPathId ? byPathId->ToString() : "nullptr"));

            if (byPath->GetPathId() < notify.PathId) {
                return SwapSubscriberAndUpsert(byPath, notify.PathId, notify.Path);
            }

            return byPathId;
        }

        if (!notifyDomainId) {
            SBC_LOG_D("ResolveCacheItemForNotify: path has gone, update only by pathId"
                << ": self# " << SelfId()
                << ", path# " << notify.Path
                << ", pathId# " << notify.PathId);
            return byPathId;
        }

        Y_FAIL_S("Unknown update");
    }

    TCacheItem* ResolveCacheItemForNotify(const TSchemeBoardEvents::TEvNotifyDelete& notify) {
        return ResolveCacheItem(notify);
    }

    TCacheItem* ResolveCacheItemForNotify(const NInternalEvents::TEvSyncResponse& notify) {
        return ResolveCacheItem(notify);
    }

    TCacheItem* ResolveCacheItemForNotify(const TSchemeBoardEvents::TEvNotifyUpdate& notify) {
        return ResolveCacheItem(notify, GetDomainId(notify.DescribeSchemeResult), GetAbandonedSchemeShardIds(notify.DescribeSchemeResult));
    }

    template <typename TEvent>
    void HandleNotify(TEvent& ev) {
        const TResponseProps response = TResponseProps::FromEvent(ev);
        auto& notify = *ev->Get();

        SBC_LOG_D("HandleNotify"
            << ": self# " << SelfId()
            << ", notify# " << notify.ToString());

        if (notify.Path && notify.PathId) {
            Y_ABORT_UNLESS(!response.IsSync);
        }

        TCacheItem* cacheItem = ResolveCacheItemForNotify(notify);

        if (!cacheItem) {
            SBC_LOG_W("HandleNotify doesn't find any cacheItem for Fill"
                << ": self# " << SelfId()
                << ", path# " << notify.Path
                << ", pathId# " << notify.PathId
                << ", isSync# " << response.IsSync);
            return;
        }

        cacheItem->Fill(notify);

        if (!response.IsSync && notify.PathId) {
            Y_VERIFY_S(cacheItem->GetPathId() == notify.PathId, "Inconsistent path ids"
                << ": cacheItem# " << cacheItem->ToString()
                << ", notification# " << notify.ToString());
        }

        for (const auto& x : cacheItem->ProcessInFlight(response)) {
            if (auto* context = std::get_if<TNavigateContextPtr>(&x)) {
                if (!context->Get()->WaitCounter) {
                    Complete(*context);
                }
            } else if (auto* context = std::get_if<TResolveContextPtr>(&x)) {
                if (!context->Get()->WaitCounter) {
                    Complete(*context);
                }
            } else {
                Y_ABORT("unknown context type");
            }
        }
    }

    template <typename TEvent>
    bool MaybeRunDbResolver(TEvent& ev) {
        auto& request = ev->Get()->Request;

        if (request->DomainOwnerId) {
            return false;
        }

        if (!request->DatabaseName) {
            return false;
        }

        const auto db = SplitPath(request->DatabaseName);
        if (db.empty()) {
            return false;
        }

        const TString databaseName = CanonizePath(db);

        TCacheItem* dbItem = Cache.FindPtr(databaseName);
        if (dbItem) {
            Cache.Promote(databaseName);
            if (dbItem->IsFilled()) {
                return false;
            }
        }

        auto it = Roots.find(db.front());
        if (it == Roots.end()) {
            return false;
        }

        Register(CreateDbResolver(SelfId(), ev->Sender, THolder(request.Release()), it->second));
        return true;
    }

    template <typename TContextPtr>
    void MaybeComplete(TContextPtr context) {
        Counters.StartRequest(
            context->Request->ResultSet.size(),
            context->WaitCounter,
            Accumulate(context->Request->ResultSet, 0, [](ui64 sum, const auto& entry) {
                return sum + ui64(entry.SyncVersion);
            })
        );

        if (!context->WaitCounter) {
            Complete(context);
        }
    }

    template <typename TContextPtr>
    void Complete(TContextPtr context) {
        Counters.FinishRequest(Now() - context->CreatedAt);
        Register(CreateAccessChecker(context));
    }

    template <typename TContext, typename TEvent>
    TIntrusivePtr<TContext> MakeContext(TEvent& ev) const {
        TIntrusivePtr<TContext> context(new TContext(ev->Sender, ev->Cookie, ev->Get()->Request, Now()));

        if (context->Request->DatabaseName) {
            if (auto* db = Cache.FindPtr(CanonizePath(context->Request->DatabaseName))) {
                context->ResolvedDomainInfo = db->GetDomainInfo();
            }
        }

        return context;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        SBC_LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySet"
            << ": self# " << SelfId()
            << ", request# " << ev->Get()->Request->ToString(*AppData()->TypeRegistry));

        if (MaybeRunDbResolver(ev)) {
            return;
        }

        auto context = MakeContext<TNavigateContext>(ev);

        for (size_t i = 0; i < context->Request->ResultSet.size(); ++i) {
            auto& entry = context->Request->ResultSet[i];

            if (entry.RequestType == TNavigate::TEntry::ERequestType::ByPath) {
                auto pathExtractor = [this](TNavigate::TEntry& entry) {
                    if (AppData()->FeatureFlags.GetEnableSystemViews()
                        && (entry.Operation == TNavigate::OpPath || entry.Operation == TNavigate::OpTable))
                    {
                        NSysView::ISystemViewResolver::TSystemViewPath sysViewPath;
                        if (SystemViewResolver->IsSystemViewPath(entry.Path, sysViewPath)) {
                            entry.TableId.SysViewInfo = sysViewPath.ViewName;
                            return CanonizePath(sysViewPath.Parent);
                        }
                    }

                    TString path = CanonizePath(entry.Path);
                    return path ? path : TString("/");
                };

                auto tabletIdExtractor = [this](const TNavigate::TEntry& entry) {
                    if (entry.Path.empty()) {
                        return ui64(NSchemeShard::InvalidTabletId);
                    }

                    auto it = Roots.find(entry.Path.front());
                    if (it == Roots.end()) {
                        return ui64(NSchemeShard::InvalidTabletId);
                    }

                    return it->second;
                };

                HandleEntry(context, entry, i, pathExtractor, tabletIdExtractor);
            } else {
                auto pathExtractor = [](const TNavigate::TEntry& entry) {
                    return entry.TableId.PathId;
                };

                auto tabletIdExtractor = [](const TNavigate::TEntry& entry) {
                    return entry.TableId.PathId.OwnerId;
                };

                HandleEntry(context, entry, i, pathExtractor, tabletIdExtractor);
            }
        }

        MaybeComplete(context);
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySet::TPtr& ev) {
        SBC_LOG_D("Handle TEvTxProxySchemeCache::TEvResolveKeySet"
            << ": self# " << SelfId()
            << ", request# " << ev->Get()->Request->ToString(*AppData()->TypeRegistry));

        if (MaybeRunDbResolver(ev)) {
            return;
        }

        auto context = MakeContext<TResolveContext>(ev);

        auto pathExtractor = [](const TResolve::TEntry& entry) {
            const TKeyDesc* keyDesc = entry.KeyDescription.Get();
            Y_ABORT_UNLESS(keyDesc != nullptr);
            return TPathId(keyDesc->TableId.PathId);
        };

        auto tabletIdExtractor = [](const TResolve::TEntry& entry) {
            return entry.KeyDescription->TableId.PathId.OwnerId;
        };

        for (size_t i = 0; i < context->Request->ResultSet.size(); ++i) {
            auto& entry = context->Request->ResultSet[i];
            HandleEntry(context, entry, i, pathExtractor, tabletIdExtractor);
        }

        MaybeComplete(context);
    }

    void Handle(TEvTxProxySchemeCache::TEvInvalidateTable::TPtr& ev) {
        SBC_LOG_D("Handle TEvTxProxySchemeCache::TEvInvalidateTable"
            << ": self# " << SelfId());
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvInvalidateTableResult(ev->Get()->Sender), 0, ev->Cookie);
    }

    TActorId EnsureWatchCache() {
        if (!WatchCache) {
            WatchCache = Register(new TWatchCache());
        }
        return WatchCache;
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchPathId::TPtr& ev) {
        auto watchCache = EnsureWatchCache();
        ev->Rewrite(ev->GetTypeRewrite(), watchCache);
        TActivationContext::Send(ev.Release());
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchRemove::TPtr& ev) {
        auto watchCache = EnsureWatchCache();
        ev->Rewrite(ev->GetTypeRewrite(), watchCache);
        TActivationContext::Send(ev.Release());
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(SelfId(), ActorActivityType());
        auto& record = *response->Record.MutableCacheResponse();

        record.SetItemsTotalCount(Cache.Size());
        record.SetItemsByPathCount(Cache.GetPrimaryIndex().size());
        record.SetItemsByPathIdCount(Cache.GetSecondaryIndex().size());

        Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void Handle(TSchemeBoardMonEvents::TEvDescribeRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TCacheItem* desc = nullptr;
        if (record.HasPath()) {
            desc = Cache.FindPtr(record.GetPath());
        } else if (record.HasPathId()) {
            desc = Cache.FindPtr(TPathId(record.GetPathId().GetOwnerId(), record.GetPathId().GetLocalPathId()));
        }

        Send(ev->Sender, new TSchemeBoardMonEvents::TEvDescribeResponse(desc ? desc->ToJsonString() : "{}"), 0, ev->Cookie);
    }

    void ShrinkCache() {
        if (Cache.Shrink()) {
            Send(SelfId(), new TEvents::TEvWakeup());
        } else {
            Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        }
    }

    void PassAway() override {
        auto evicter = [](const auto& index) {
            for (auto it = index.begin(); it != index.end(); ++it) {
                it->second->Value.OnEvict();
            }
        };

        evicter(Cache.GetPrimaryIndex());
        evicter(Cache.GetSecondaryIndex());

        if (WatchCache) {
            Send(WatchCache, new TEvents::TEvPoison);
            WatchCache = { };
        }

        TMonitorableActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PROXY_SCHEME_CACHE;
    }

    TSchemeCache(NSchemeCache::TSchemeCacheConfig* config)
        : Counters(config->Counters)
        , Cache(TDuration::Minutes(2), TThis::Now)
        , SystemViewResolver(NSysView::CreateSystemViewResolver())
    {
        for (const auto& root : config->Roots) {
            Roots.emplace(root.Name, root.RootSchemeShard);
        }
    }

    void Bootstrap() {
        TMonitorableActor::Bootstrap();

        Become(&TThis::StateWork);
        ShrinkCache();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySet, Handle);
            hFunc(TEvTxProxySchemeCache::TEvInvalidateTable, Handle);

            hFunc(TEvTxProxySchemeCache::TEvWatchPathId, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchRemove, Handle);

            hFunc(TSchemeBoardEvents::TEvNotifyUpdate, HandleNotify);
            hFunc(TSchemeBoardEvents::TEvNotifyDelete, HandleNotify);
            hFunc(NInternalEvents::TEvSyncResponse, HandleNotify);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);
            hFunc(TSchemeBoardMonEvents::TEvDescribeRequest, Handle);

            sFunc(TEvents::TEvWakeup, ShrinkCache);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    THashMap<TString, ui64> Roots;
    TCounters Counters;

    TDoubleIndexedCache<TString, TPathId, TCacheItem, TMerger, TEvicter> Cache;
    THolder<NSysView::ISystemViewResolver> SystemViewResolver;

    TActorId WatchCache;

}; // TSchemeCache

} // NSchemeBoard

IActor* CreateSchemeBoardSchemeCache(NSchemeCache::TSchemeCacheConfig* config) {
    return new NSchemeBoard::TSchemeCache(config);
}

} // NKikimr
