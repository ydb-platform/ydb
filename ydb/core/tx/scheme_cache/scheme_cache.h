#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/library/aclib/aclib.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>

namespace NKikimr {

struct TAppData;

namespace NSchemeCache {

struct TSchemeCacheConfig : public TThrRefBase {
    struct TTagEntry {
        ui32 Tag = 0;
        ui64 RootSchemeShard = 0;
        TString Name;

        explicit TTagEntry(ui32 tag, ui64 rootSchemeShard, const TString& name)
            : Tag(tag)
            , RootSchemeShard(rootSchemeShard)
            , Name(name)
        {}
    };

    TSchemeCacheConfig() = default;
    explicit TSchemeCacheConfig(const TAppData* appData, ::NMonitoring::TDynamicCounterPtr counters);

    TVector<TTagEntry> Roots;
    ::NMonitoring::TDynamicCounterPtr Counters;
};

struct TDomainInfo : public TAtomicRefCount<TDomainInfo> {
    using TPtr = TIntrusivePtr<TDomainInfo>;

    explicit TDomainInfo(const TPathId& domainKey, const TPathId& resourcesDomainKey)
        : DomainKey(domainKey)
        , ResourcesDomainKey(resourcesDomainKey)
        , Coordinators(TVector<ui64>{})
    {}

    explicit TDomainInfo(const NKikimrSubDomains::TDomainDescription& descr)
        : DomainKey(GetDomainKey(descr.GetDomainKey()))
        , Params(descr.GetProcessingParams())
        , Coordinators(descr.GetProcessingParams())
    {
        if (descr.HasResourcesDomainKey()) {
            ResourcesDomainKey = GetDomainKey(descr.GetResourcesDomainKey());
        } else {
            ResourcesDomainKey = DomainKey;
        }

        if (descr.HasServerlessComputeResourcesMode()) {
            ServerlessComputeResourcesMode = descr.GetServerlessComputeResourcesMode();
        }
    }

    inline ui64 GetVersion() const {
        return Params.GetVersion();
    }

    inline ui64 ExtractSchemeShard() const {
        if (Params.HasSchemeShard()) {
            return Params.GetSchemeShard();
        } else {
            return DomainKey.OwnerId;
        }
    }

    inline bool IsServerless() const {
        return DomainKey != ResourcesDomainKey;
    }

    TPathId DomainKey;
    TPathId ResourcesDomainKey;
    NKikimrSubDomains::TProcessingParams Params;
    TCoordinators Coordinators;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;

    TString ToString() const;

private:
    inline static TPathId GetDomainKey(const NKikimrSubDomains::TDomainKey& protoKey) {
        return TPathId(protoKey.GetSchemeShard(), protoKey.GetPathId());
    }

}; // TDomainInfo

struct TSchemeCacheNavigate {
    enum class EStatus {
        Unknown = 0,
        RootUnknown = 1,
        PathErrorUnknown = 2,
        PathNotTable = 3,
        PathNotPath = 4,
        TableCreationNotComplete = 5,
        LookupError = 6,
        RedirectLookupError = 7,
        AccessDenied = 8,
        Ok = 128,
    };

    enum EOp {
        OpUnknown = 0,
        OpTable = 1, // would return table info
        OpPath = 2, // would return owning scheme shard id
        OpTopic = 3, // would return topic info
        OpList = 4, // would list children and cache them
    };

    enum EKind {
        KindUnknown = 0,
        KindPath = 2,
        KindTable = 3,
        KindTopic = 4,
        KindRtmr = 5,
        KindKesus = 6,
        KindSolomon = 7,
        KindSubdomain = 8,
        KindExtSubdomain = 9,
        KindIndex = 10,
        KindOlapStore = 11,
        KindColumnTable = 12,
        KindCdcStream = 13,
        KindSequence = 14,
        KindReplication = 15,
        KindBlobDepot = 16,
        KindExternalTable = 17,
        KindExternalDataSource = 18,
        KindBlockStoreVolume = 19,
        KindFileStore = 20,
        KindView = 21,
        KindResourcePool = 22,
    };

    struct TListNodeEntry : public TAtomicRefCount<TListNodeEntry> {
        struct TChild {
            const TString Name;
            const TPathId PathId;
            const ui64 SchemaVersion;
            const EKind Kind;

            explicit TChild(const TString& name, const TPathId& pathId, EKind kind, ui64 schemaVersion = 0)
                : Name(name)
                , PathId(pathId)
                , SchemaVersion(schemaVersion)
                , Kind(kind)
            {}
        };

        EKind Kind = KindUnknown;
        TVector<TChild> Children;
    };

    struct TDirEntryInfo : public TAtomicRefCount<TDirEntryInfo>{
        NKikimrSchemeOp::TDirEntry Info;
    };

    struct TDomainDescription : public TAtomicRefCount<TDomainDescription> {
        EKind Kind = KindUnknown;
        NKikimrSubDomains::TDomainDescription Description;
    };

    struct TPQGroupInfo : public TAtomicRefCount<TPQGroupInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TPersQueueGroupDescription Description;
    };

    struct TRtmrVolumeInfo : public TAtomicRefCount<TRtmrVolumeInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TRtmrVolumeDescription Description;
    };

    struct TKesusInfo : public TAtomicRefCount<TKesusInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TKesusDescription Description;
    };

    struct TSolomonVolumeInfo : public TAtomicRefCount<TSolomonVolumeInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TSolomonVolumeDescription Description;
    };

    struct TOlapStoreInfo : public TAtomicRefCount<TOlapStoreInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TColumnStoreDescription Description;
    };

    struct TColumnTableInfo : public TAtomicRefCount<TColumnTableInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TColumnTableDescription Description;
        TTableId OlapStoreId;
    };

    struct TCdcStreamInfo : public TAtomicRefCount<TCdcStreamInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TCdcStreamDescription Description;
        TPathId PathId;
    };

    struct TSequenceInfo : public TAtomicRefCount<TSequenceInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TSequenceDescription Description;
    };

    struct TReplicationInfo : public TAtomicRefCount<TReplicationInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TReplicationDescription Description;
    };

    struct TBlobDepotInfo : TAtomicRefCount<TBlobDepotInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TBlobDepotDescription Description;
    };

    struct TExternalTableInfo : public TAtomicRefCount<TExternalTableInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TExternalTableDescription Description;
    };

    struct TExternalDataSourceInfo : public TAtomicRefCount<TExternalDataSourceInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TExternalDataSourceDescription Description;
    };

    struct TBlockStoreVolumeInfo : public TAtomicRefCount<TBlockStoreVolumeInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TBlockStoreVolumeDescription Description;
    };

    struct TFileStoreInfo : public TAtomicRefCount<TFileStoreInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TFileStoreDescription Description;
    };

    struct TViewInfo : public TAtomicRefCount<TViewInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TViewDescription Description;
    };

    struct TResourcePoolInfo : public TAtomicRefCount<TResourcePoolInfo> {
        EKind Kind = KindUnknown;
        NKikimrSchemeOp::TResourcePoolDescription Description;
    };

    struct TEntry {
        enum class ERequestType : ui8 {
            ByPath,
            ByTableId
        };

        // in
        TVector<TString> Path;
        ui32 Access = NACLib::DescribeSchema;
        TTableId TableId;
        ERequestType RequestType = ERequestType::ByPath;
        EOp Operation = OpUnknown;
        bool RedirectRequired = true;
        bool ShowPrivatePath = false;
        bool SyncVersion = false;

        // out
        EStatus Status = EStatus::Unknown;
        EKind Kind = KindUnknown;
        ui64 CreateStep = 0;

        // common
        TIntrusivePtr<TDomainInfo> DomainInfo;
        TIntrusivePtr<TSecurityObject> SecurityObject;
        TIntrusiveConstPtr<TDirEntryInfo> Self;
        TIntrusiveConstPtr<TListNodeEntry> ListNodeEntry;

        // table specific
        THashMap<TString, TString> Attributes;
        THashMap<ui32, TSysTables::TTableColumnInfo> Columns;
        THashSet<TString> NotNullColumns;
        TVector<NKikimrSchemeOp::TIndexDescription> Indexes;
        TVector<NKikimrSchemeOp::TCdcStreamDescription> CdcStreams;

        // other
        TIntrusiveConstPtr<TDomainDescription> DomainDescription;
        TIntrusiveConstPtr<TPQGroupInfo> PQGroupInfo;
        TIntrusiveConstPtr<TRtmrVolumeInfo> RTMRVolumeInfo;
        TIntrusiveConstPtr<TKesusInfo> KesusInfo;
        TIntrusiveConstPtr<TSolomonVolumeInfo> SolomonVolumeInfo;
        TIntrusiveConstPtr<TOlapStoreInfo> OlapStoreInfo;
        TIntrusiveConstPtr<TColumnTableInfo> ColumnTableInfo;
        TIntrusiveConstPtr<TCdcStreamInfo> CdcStreamInfo;
        TIntrusiveConstPtr<TSequenceInfo> SequenceInfo;
        TIntrusiveConstPtr<TReplicationInfo> ReplicationInfo;
        TIntrusiveConstPtr<TBlobDepotInfo> BlobDepotInfo;
        TIntrusiveConstPtr<TExternalTableInfo> ExternalTableInfo;
        TIntrusiveConstPtr<TExternalDataSourceInfo> ExternalDataSourceInfo;
        TIntrusiveConstPtr<TBlockStoreVolumeInfo> BlockStoreVolumeInfo;
        TIntrusiveConstPtr<TFileStoreInfo> FileStoreInfo;
        TIntrusiveConstPtr<TViewInfo> ViewInfo;
        TIntrusiveConstPtr<TResourcePoolInfo> ResourcePoolInfo;

        TString ToString() const;
        TString ToString(const NScheme::TTypeRegistry& typeRegistry) const;
    };

    using TResultSet = TVector<TEntry>;

    TResultSet ResultSet;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString DatabaseName;
    ui64 DomainOwnerId = 0;
    ui64 ErrorCount = 0;
    ui64 Cookie = 0;
    const ui64 Instant; // deprecated, used by pq

    TSchemeCacheNavigate()
        : Instant(0)
    {}

    explicit TSchemeCacheNavigate(ui64 instant)
        : Instant(instant)
    {}

    TString ToString(const NScheme::TTypeRegistry& typeRegistry) const;

}; // TSchemeCacheNavigate

struct TSchemeCacheRequest {
    enum class EStatus {
        Unknown = 0,
        TypeCheckError = 1,
        NotMaterialized = 2,
        PathErrorUnknown = 3,
        PathErrorNotExist = 4,
        LookupError = 5,
        AccessDenied = 6,
        OkScheme = 128,
        OkData = 129,
    };

    enum EOp {
        OpUnknown = 0,
        OpRead = 1 << 0,
        OpWrite = 1 << 2,
        OpScheme = 1 << 3,
    };

    enum EKind {
        KindUnknown = 0,
        KindRegularTable = 1,
        KindSyncIndexTable = 2,
        KindAsyncIndexTable = 3,
        KindVectorIndexTable = 4,
    };

    struct TEntry {
        // in
        THolder<TKeyDesc> KeyDescription;
        ui32 Access = 0;
        uintptr_t UserData = 0;
        bool SyncVersion = false;

        // out
        EStatus Status = EStatus::Unknown;
        EKind Kind = EKind::KindUnknown;
        TIntrusivePtr<TDomainInfo> DomainInfo;
        ui64 GeneralVersion = 0;

        explicit TEntry(THolder<TKeyDesc> keyDesc)
            : KeyDescription(std::move(keyDesc))
        {
            Y_DEBUG_ABORT_UNLESS(KeyDescription);
        }

        TEntry(TKeyDesc* keyDesc) = delete;

        TString ToString() const;
        TString ToString(const NScheme::TTypeRegistry& typeRegistry) const;
    };

    using TResultSet = TVector<TEntry>;

    TResultSet ResultSet;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString DatabaseName;
    ui64 DomainOwnerId = 0;
    ui64 ErrorCount = 0;

    TString ToString(const NScheme::TTypeRegistry& typeRegistry) const;

}; // TSchemeCacheRequest

struct TSchemeCacheRequestContext : TAtomicRefCount<TSchemeCacheRequestContext>, TNonCopyable {
    TActorId Sender;
    ui64 Cookie;
    ui64 WaitCounter;
    TAutoPtr<TSchemeCacheRequest> Request;
    const TInstant CreatedAt;
    TIntrusivePtr<TDomainInfo> ResolvedDomainInfo; // resolved from DatabaseName

    TSchemeCacheRequestContext(const TActorId& sender, ui64 cookie, TAutoPtr<TSchemeCacheRequest> request, const TInstant& now = TInstant::Now())
        : Sender(sender)
        , Cookie(cookie)
        , WaitCounter(0)
        , Request(request)
        , CreatedAt(now)
    {}
};

struct TSchemeCacheNavigateContext : TAtomicRefCount<TSchemeCacheNavigateContext>, TNonCopyable {
    TActorId Sender;
    ui64 Cookie;
    ui64 WaitCounter;
    TAutoPtr<TSchemeCacheNavigate> Request;
    const TInstant CreatedAt;
    TIntrusivePtr<TDomainInfo> ResolvedDomainInfo; // resolved from DatabaseName

    TSchemeCacheNavigateContext(const TActorId& sender, ui64 cookie, TAutoPtr<TSchemeCacheNavigate> request, const TInstant& now = TInstant::Now())
        : Sender(sender)
        , Cookie(cookie)
        , WaitCounter(0)
        , Request(request)
        , CreatedAt(now)
    {}
};

class TDescribeResult
    : public TAtomicRefCount<TDescribeResult>
    , public NKikimrScheme::TEvDescribeSchemeResult
{
public:
    using TPtr = TIntrusivePtr<TDescribeResult>;
    using TCPtr = TIntrusiveConstPtr<TDescribeResult>;

public:
    TDescribeResult() = default;

    TDescribeResult(const TDescribeResult&) = delete;
    TDescribeResult(TDescribeResult&&) = delete;

    TDescribeResult& operator=(const TDescribeResult&) = delete;
    TDescribeResult& operator=(TDescribeResult&&) = delete;

private:
    explicit TDescribeResult(const NKikimrScheme::TEvDescribeSchemeResult& result)
        : TEvDescribeSchemeResult(result)
    {}

    explicit TDescribeResult(NKikimrScheme::TEvDescribeSchemeResult&& result)
        : TEvDescribeSchemeResult(std::move(result))
    {}

public:
    static TPtr Create(const NKikimrScheme::TEvDescribeSchemeResult& result) {
        return new TDescribeResult(result);
    }

    static TPtr Create(NKikimrScheme::TEvDescribeSchemeResult&& result) {
        return new TDescribeResult(std::move(result));
    }
};

} // NSchemeCache

struct TEvTxProxySchemeCache {
    enum EEv {
        EvResolveKeySet = EventSpaceBegin(TKikimrEvents::ES_SCHEME_CACHE),
        EvInvalidateDistEntry, // unused
        EvResolveKeySetResult,
        EvNavigateKeySet,
        EvNavigateKeySetResult,
        EvInvalidateTable,
        EvInvalidateTableResult,
        EvWatchPathId,
        EvWatchRemove,
        EvWatchNotifyUpdated,
        EvWatchNotifyDeleted,
        EvWatchNotifyUnavailable,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SCHEME_CACHE), "expect EvEnd < EventSpaceEnd(ES_SCHEME_CACHE)");

private:
    template <typename TDerived, ui32 EventType, typename TRequest>
    struct TEvBasic : public TEventLocal<TDerived, EventType> {
        TAutoPtr<TRequest> Request;

        TEvBasic(TAutoPtr<TRequest> request)
            : Request(request)
        {}
    };

public:
    struct TEvResolveKeySet : public TEvBasic<TEvResolveKeySet, EvResolveKeySet, NSchemeCache::TSchemeCacheRequest> {
        using TEvBasic::TEvBasic;
    };

    struct TEvResolveKeySetResult : public TEvBasic<TEvResolveKeySetResult, EvResolveKeySetResult, NSchemeCache::TSchemeCacheRequest> {
        using TEvBasic::TEvBasic;
    };

    struct TEvNavigateKeySet : public TEvBasic<TEvNavigateKeySet, EvNavigateKeySet, NSchemeCache::TSchemeCacheNavigate> {
        using TEvBasic::TEvBasic;
    };

    struct TEvNavigateKeySetResult : public TEvBasic<TEvNavigateKeySetResult, EvNavigateKeySetResult, NSchemeCache::TSchemeCacheNavigate> {
        using TEvBasic::TEvBasic;
    };

    struct TEvInvalidateTable : public TEventLocal<TEvInvalidateTable, EvInvalidateTable> {
        const TTableId TableId;
        const TActorId Sender;

        TEvInvalidateTable(const TTableId& tableId, const TActorId& sender)
            : TableId(tableId)
            , Sender(sender)
        {}
    };

    struct TEvInvalidateTableResult : public TEventLocal<TEvInvalidateTableResult, EvInvalidateTableResult> {
        const TActorId Sender;

        TEvInvalidateTableResult(const TActorId& sender)
            : Sender(sender)
        {}
    };

    struct TEvWatchPathId : public TEventLocal<TEvWatchPathId, EvWatchPathId> {
        const TPathId PathId;
        const ui64 Key;

        explicit TEvWatchPathId(const TPathId& pathId, ui64 key = 0)
            : PathId(pathId)
            , Key(key)
        {}
    };

    struct TEvWatchRemove : public TEventLocal<TEvWatchRemove, EvWatchRemove> {
        const ui64 Key;

        explicit TEvWatchRemove(ui64 key = 0)
            : Key(key)
        {}
    };

    struct TEvWatchNotifyUpdated : public TEventLocal<TEvWatchNotifyUpdated, EvWatchNotifyUpdated> {
        using TDescribeResult = NSchemeCache::TDescribeResult;

        const ui64 Key;
        const TString Path;
        const TPathId PathId;
        TDescribeResult::TCPtr Result;

        TEvWatchNotifyUpdated(ui64 key, const TString& path, const TPathId& pathId, TDescribeResult::TCPtr result)
            : Key(key)
            , Path(path)
            , PathId(pathId)
            , Result(std::move(result))
        {}
    };

    struct TEvWatchNotifyDeleted : public TEventLocal<TEvWatchNotifyDeleted, EvWatchNotifyDeleted> {
        const ui64 Key;
        const TString Path;
        const TPathId PathId;

        TEvWatchNotifyDeleted(ui64 key, const TString& path, const TPathId& pathId)
            : Key(key)
            , Path(path)
            , PathId(pathId)
        {}
    };

    struct TEvWatchNotifyUnavailable : public TEventLocal<TEvWatchNotifyUnavailable, EvWatchNotifyUnavailable> {
        const ui64 Key;
        const TString Path;
        const TPathId PathId;

        TEvWatchNotifyUnavailable(ui64 key, const TString& path, const TPathId& pathId)
            : Key(key)
            , Path(path)
            , PathId(pathId)
        {}
    };
};

inline TActorId MakeSchemeCacheID() {
    return TActorId(0, TStringBuf("SchmCcheSrv"));
}

} // NKikimr
