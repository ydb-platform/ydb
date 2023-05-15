#pragma once

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/utils/resetable_setting.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/abstract.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/join.h>

namespace NKikimr {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NYql {

using NUdf::EDataSlot;

struct TKikimrQueryPhaseLimits {
    ui32 AffectedShardsLimit = 0;
    ui32 ReadsetCountLimit = 0;
    ui64 ComputeNodeMemoryLimitBytes = 0;
    ui64 TotalReadSizeLimitBytes = 0;
};

struct TKikimrQueryLimits {
    TKikimrQueryPhaseLimits PhaseLimits;
};

struct TIndexDescription {
    enum class EType : ui32 {
        GlobalSync = 0,
        GlobalAsync = 1,

    };

    // Index states here must be in sync with NKikimrSchemeOp::EIndexState protobuf
    enum class EIndexState : ui32 {
        Invalid = 0,  // this state should not be used
        Ready = 1,    // index is ready to use
        NotReady = 2, // index is visible but not ready to use
        WriteOnly = 3 // index is visible only write operations to index are allowed
    };

    const TString Name;
    const TVector<TString> KeyColumns;
    const TVector<TString> DataColumns;
    const EType Type;
    const EIndexState State;
    const ui64 SchemaVersion;
    const ui64 LocalPathId;
    const ui64 PathOwnerId;

    TIndexDescription(const TString& name, const TVector<TString>& keyColumns, const TVector<TString>& dataColumns,
        EType type, EIndexState state, ui64 schemaVersion, ui64 localPathId, ui64 pathOwnerId)
        : Name(name)
        , KeyColumns(keyColumns)
        , DataColumns(dataColumns)
        , Type(type)
        , State(state)
        , SchemaVersion(schemaVersion)
        , LocalPathId(localPathId)
        , PathOwnerId(pathOwnerId)
    {}

    TIndexDescription(const NKikimrSchemeOp::TIndexDescription& index)
        : Name(index.GetName())
        , KeyColumns(index.GetKeyColumnNames().begin(), index.GetKeyColumnNames().end())
        , DataColumns(index.GetDataColumnNames().begin(), index.GetDataColumnNames().end())
        , Type(ConvertIndexType(index))
        , State(static_cast<EIndexState>(index.GetState()))
        , SchemaVersion(index.GetSchemaVersion())
        , LocalPathId(index.GetLocalPathId())
        , PathOwnerId(index.HasPathOwnerId() ? index.GetPathOwnerId() : 0ul)
    {}

    TIndexDescription(const NKikimrKqp::TIndexDescriptionProto* message)
        : Name(message->GetName())
        , KeyColumns(message->GetKeyColumns().begin(), message->GetKeyColumns().end())
        , DataColumns(message->GetDataColumns().begin(), message->GetDataColumns().end())
        , Type(static_cast<EType>(message->GetType()))
        , State(static_cast<EIndexState>(message->GetState()))
        , SchemaVersion(message->GetSchemaVersion())
        , LocalPathId(message->GetLocalPathId())
        , PathOwnerId(message->GetPathOwnerId())
    {}

    static TIndexDescription::EType ConvertIndexType(const NKikimrSchemeOp::TIndexDescription& index) {
        auto type = NYql::TIndexDescription::EType::GlobalSync;
        if (index.GetType() == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync) {
            type = NYql::TIndexDescription::EType::GlobalAsync;
        }

        return type;
    }

    void ToMessage(NKikimrKqp::TIndexDescriptionProto* message) const {
        message->SetName(Name);
        message->SetType(static_cast<ui32>(Type));
        message->SetState(static_cast<ui32>(State));
        message->SetSchemaVersion(SchemaVersion);
        message->SetLocalPathId(LocalPathId);
        message->SetPathOwnerId(PathOwnerId);

        for(auto& key: KeyColumns) {
            message->AddKeyColumns(key);
        }

        for(auto& data: DataColumns) {
            message->AddDataColumns(data);
        }
    }

    bool IsSameIndex(const TIndexDescription& other) const {
        return Name == other.Name &&
            KeyColumns == other.KeyColumns &&
            DataColumns == other.DataColumns &&
            Type == other.Type;
    }

    bool ItUsedForWrite() const {
        switch (Type) {
            case EType::GlobalSync:
                return true;
            case EType::GlobalAsync:
                return false;
        }
    }
};

struct TColumnFamily {
    TString Name;
    TMaybe<TString> Data;
    TMaybe<TString> Compression;
};

struct TTtlSettings {
    TString ColumnName;
    TDuration ExpireAfter;

    static bool TryParse(const NNodes::TCoNameValueTupleList& node, TTtlSettings& settings, TString& error);
};

struct TTableSettings {
    TMaybe<TString> CompactionPolicy;
    TVector<TString> PartitionBy;
    TMaybe<TString> AutoPartitioningBySize;
    TMaybe<ui64> PartitionSizeMb;
    TMaybe<TString> AutoPartitioningByLoad;
    TMaybe<ui64> MinPartitions;
    TMaybe<ui64> MaxPartitions;
    TMaybe<ui64> UniformPartitions;
    TVector<TVector<std::pair<EDataSlot, TString>>> PartitionAtKeys;
    TMaybe<TString> KeyBloomFilter;
    TMaybe<TString> ReadReplicasSettings;
    TResetableSetting<TTtlSettings, void> TtlSettings;
    TResetableSetting<TString, void> Tiering;
    TMaybe<TString> PartitionByHashFunction;

    // These parameters are only used for external sources
    TMaybe<TString> DataSourcePath;
    TMaybe<TString> Location;
    TVector<std::pair<TString, TString>> ExternalSourceParameters;

    bool IsSet() const;
};

struct TKikimrColumnMetadata {
    TString Name;
    ui32 Id = 0;
    TString Type;
    bool NotNull = false;
    NKikimr::NScheme::TTypeInfo TypeInfo;
    TString TypeMod;
    TVector<TString> Families;

    TKikimrColumnMetadata() = default;

    TKikimrColumnMetadata(const TString& name, ui32 id, const TString& type, bool notNull,
        NKikimr::NScheme::TTypeInfo typeInfo = {}, const TString& typeMod = {})
        : Name(name)
        , Id(id)
        , Type(type)
        , NotNull(notNull)
        , TypeInfo(typeInfo)
        , TypeMod(typeMod)
    {}

    explicit TKikimrColumnMetadata(const NKikimrKqp::TKqpColumnMetadataProto* message)
        : Name(message->GetName())
        , Id(message->GetId())
        , Type(message->GetType())
        , NotNull(message->GetNotNull())
        , Families(message->GetFamily().begin(), message->GetFamily().end())
    {
        auto typeInfoMod = NKikimr::NScheme::TypeInfoModFromProtoColumnType(message->GetTypeId(),
            message->HasTypeInfo() ? &message->GetTypeInfo() : nullptr);
        TypeInfo = typeInfoMod.TypeInfo;
        TypeMod = typeInfoMod.TypeMod;
    }

    void ToMessage(NKikimrKqp::TKqpColumnMetadataProto* message) const {
        message->SetName(Name);
        message->SetId(Id);
        message->SetType(Type);
        message->SetNotNull(NotNull);
        auto columnType = NKikimr::NScheme::ProtoColumnTypeFromTypeInfoMod(TypeInfo, TypeMod);
        message->SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *message->MutableTypeInfo() = *columnType.TypeInfo;
        }
        for(auto& family: Families) {
            message->AddFamily(family);
        }
    }

    bool IsSameScheme(const TKikimrColumnMetadata& other) const {
        return Name == other.Name && Type == other.Type && NotNull == other.NotNull;
    }

    void SetNotNull() {
        NotNull = true;
    }
};

struct TKikimrPathId {
    explicit TKikimrPathId(const std::pair<ui64, ui64>& raw)
        : Raw(raw) {}

    TKikimrPathId(ui64 ownerId, ui64 tableId)
        : TKikimrPathId(std::make_pair(ownerId, tableId)) {}

    TKikimrPathId(const NKikimrKqp::TKqpPathIdProto* message)
        : TKikimrPathId(std::make_pair(message->GetOwnerId(), message->GetTableId())) {}

    ui64 OwnerId() const { return Raw.first; }
    ui64 TableId() const { return Raw.second; }

    TString ToString() const {
        return ::ToString(OwnerId()) + ':' + ::ToString(TableId());
    }

    bool operator==(const TKikimrPathId& x) const {
        return Raw == x.Raw;
    }

    bool operator!=(const TKikimrPathId& x) const {
        return !operator==(x);
    }

    ui64 Hash() const noexcept {
        return THash<decltype(Raw)>()(Raw);
    }

    static TKikimrPathId Parse(const TStringBuf& str);

    std::pair<ui64, ui64> Raw;

    void ToMessage(NKikimrKqp::TKqpPathIdProto* message) const {
        message->SetOwnerId(OwnerId());
        message->SetTableId(TableId());
    }
};

enum class EKikimrTableKind : ui32 {
    Unspecified = 0,
    Datashard = 1,
    SysView = 2,
    Olap = 3,
    External = 4
};

enum class ETableType : ui32 {
    Unknown = 0,
    Table = 1,
    TableStore = 2,
    ExternalTable = 3
};

ETableType GetTableTypeFromString(const TStringBuf& tableType);

bool GetTopicMeteringModeFromString(const TString& meteringMode,
                                                        Ydb::Topic::MeteringMode& result);
TVector<Ydb::Topic::Codec> GetTopicCodecsFromString(const TStringBuf& codecsStr);


enum class EStoreType : ui32 {
    Row = 0,
    Column = 1
};

struct TExternalSource {
    TString Type;
    TString TableLocation;
    TString TableContent;
    TString DataSourcePath;
    TString DataSourceLocation;
    TString DataSourceInstallation;
    NKikimrSchemeOp::TAuth DataSourceAuth;
};

struct TKikimrTableMetadata : public TThrRefBase {
    bool DoesExist = false;
    TString Cluster;
    TString Name;
    TKikimrPathId PathId;
    TString SysView;
    ui64 SchemaVersion = 0;
    THashMap<TString, TString> Attributes;
    EKikimrTableKind Kind = EKikimrTableKind::Unspecified;
    ETableType TableType = ETableType::Table;
    EStoreType StoreType = EStoreType::Row;

    ui64 RecordsCount = 0;
    ui64 DataSize = 0;
    ui64 MemorySize = 0;
    ui32 ShardsCount = 0;

    TInstant LastAccessTime;
    TInstant LastUpdateTime;

    TMap<TString, TKikimrColumnMetadata> Columns;
    TVector<TString> KeyColumnNames;
    TVector<TString> ColumnOrder;

    // Indexes and SecondaryGlobalIndexMetadata must be in same order
    TVector<TIndexDescription> Indexes;
    TVector<TIntrusivePtr<TKikimrTableMetadata>> SecondaryGlobalIndexMetadata;

    TVector<TColumnFamily> ColumnFamilies;
    TTableSettings TableSettings;

    TExternalSource ExternalSource;

    TKikimrTableMetadata(const TString& cluster, const TString& table)
        : Cluster(cluster)
        , Name(table)
        , PathId(std::make_pair(0, 0)) {}

    TKikimrTableMetadata()
        : TKikimrTableMetadata("", "") {}

    TKikimrTableMetadata(const NKikimrKqp::TKqpTableMetadataProto* message)
        : DoesExist(message->GetDoesExist())
        , Cluster(message->GetCluster())
        , Name(message->GetName())
        , PathId(&message->GetPathId())
        , SysView(message->GetSysView())
        , SchemaVersion(message->GetSchemaVersion())
        , Kind(static_cast<EKikimrTableKind>(message->GetKind()))
        , KeyColumnNames(message->GetKeyColunmNames().begin(), message->GetKeyColunmNames().end())
    {
        for(auto& attr: message->GetAttributes()) {
            Attributes.emplace(attr.GetKey(), attr.GetValue());
        }

        std::map<ui32, TString> orderMap;
        for(auto& col: message->GetColumns()) {
            Columns.emplace(col.GetName(), TKikimrColumnMetadata(&col));
            orderMap.emplace(col.GetId(), col.GetName());
        }

        Indexes.reserve(message->GetIndexes().size());
        for(auto& index: message->GetIndexes())
            Indexes.push_back(TIndexDescription(&index));

        SecondaryGlobalIndexMetadata.reserve(message->GetSecondaryGlobalIndexMetadata().size());
        for(auto& sgim: message->GetSecondaryGlobalIndexMetadata())
           SecondaryGlobalIndexMetadata.push_back(MakeIntrusive<TKikimrTableMetadata>(&sgim));

        ColumnOrder.reserve(Columns.size());
        for(auto& [_, name]: orderMap) {
            ColumnOrder.emplace_back(name);
        }
    }

    bool IsSameTable(const TKikimrTableMetadata& other) {
        if (!DoesExist) {
            return false;
        }

        if (Cluster != other.Cluster || Name != other.Name || Columns.size() != other.Columns.size() ||
                KeyColumnNames != other.KeyColumnNames || Indexes.size() != other.Indexes.size()) {
            return false;
        }

        for (auto& [name, column]: Columns) {
            auto otherColumn = other.Columns.FindPtr(name);
            if (!otherColumn) {
                return false;
            }

            if (!column.IsSameScheme(*otherColumn)) {
                return false;
            }
        }

        for (size_t i = 0; i < Indexes.size(); i++) {
            if (!Indexes[i].IsSameIndex(other.Indexes[i])) {
                return false;
            }
        }

        return true;
    }

    void ToMessage(NKikimrKqp::TKqpTableMetadataProto* message) const {
        message->SetDoesExist(DoesExist);
        message->SetCluster(Cluster);
        message->SetName(Name);
        message->SetSysView(SysView);
        PathId.ToMessage(message->MutablePathId());
        message->SetSchemaVersion(SchemaVersion);
        message->SetKind(static_cast<ui32>(Kind));
        for(auto& [key, value] : Attributes) {
            message->AddAttributes()->SetKey(key);
            message->AddAttributes()->SetValue(value);
        }

        for(auto& [name, column] : Columns) {
            column.ToMessage(message->AddColumns());
        }

        for(auto& key: KeyColumnNames) {
            message->AddKeyColunmNames(key);
        }

        for(auto& index: Indexes) {
            index.ToMessage(message->AddIndexes());
        }

        for(auto& IndexTableMetadata: SecondaryGlobalIndexMetadata) {
            IndexTableMetadata->ToMessage(message->AddSecondaryGlobalIndexMetadata());
        }
    }

    TString SerializeToString() const {
        NKikimrKqp::TKqpTableMetadataProto proto;
        ToMessage(&proto);
        return proto.SerializeAsString();
    }

    std::pair<TIntrusivePtr<TKikimrTableMetadata>, TIndexDescription::EIndexState> GetIndexMetadata(const TString& indexName) const {
        YQL_ENSURE(Indexes.size(), "GetIndexMetadata called for table without indexes");
        YQL_ENSURE(Indexes.size() == SecondaryGlobalIndexMetadata.size(), "index metadata has not been loaded yet");
        for (size_t i = 0; i < Indexes.size(); i++) {
            if (Indexes[i].Name == indexName) {
                auto metadata = SecondaryGlobalIndexMetadata[i];
                YQL_ENSURE(metadata, "unexpected empty metadata for index " << indexName);
                return {metadata, Indexes[i].State};
            }
        }
        return {nullptr, TIndexDescription::EIndexState::Invalid};
    }

    bool IsOlap() const {
        return Kind == EKikimrTableKind::Olap;
    }
};

struct TCreateUserSettings {
    TString UserName;
    TString Password;
    bool PasswordEncrypted = false;
};

struct TAlterUserSettings {
    TString UserName;
    TString Password;
    bool PasswordEncrypted = false;
};

struct TDropUserSettings {
    TString UserName;
    bool Force = false;
};

struct TCreateGroupSettings {
    TString GroupName;
};

struct TAlterGroupSettings {
    enum class EAction : ui32 {
        AddRoles = 0,
        RemoveRoles = 1,
    };

    TString GroupName;
    EAction Action;
    std::vector<TString> Roles;
};

struct TDropGroupSettings {
    TString GroupName;
    bool Force = false;
};

struct TAlterColumnTableSettings {
    TString Table;
};

struct TCreateTableStoreSettings {
    TString TableStore;
    ui32 ShardsCount = 0;
    TMap<TString, TKikimrColumnMetadata> Columns;
    TVector<TString> KeyColumnNames;
    TVector<TString> ColumnOrder;
    TVector<TIndexDescription> Indexes;
};

struct TAlterTableStoreSettings {
    TString TableStore;
};

struct TDropTableStoreSettings {
    TString TableStore;
};

struct TCreateExternalTableSettings {
    TString ExternalTable;
    TString DataSourcePath;
    TString Location;
    TVector<TString> ColumnOrder;
    TMap<TString, TKikimrColumnMetadata> Columns;
    TVector<std::pair<TString, TString>> SourceTypeParameters;
};

struct TAlterExternalTableSettings {
    TString ExternalTable;
};

struct TDropExternalTableSettings {
    TString ExternalTable;
};

struct TCreateExternalDataSourceSettings {
    TString ExternalDataSource;
    TString SourceType;
    TString Location;
    TString Installation;
    TString AuthMethod;
};

struct TAlterExternalDataSourceSettings {
    TString ExternalDataSource;
};

struct TDropExternalDataSourceSettings {
    TString ExternalDataSource;
};

struct TKikimrListPathItem {
    TKikimrListPathItem(TString name, bool isDirectory) {
        Name = name;
        IsDirectory = isDirectory;
    }

    TString Name;
    bool IsDirectory;
};

typedef TIntrusivePtr<TKikimrTableMetadata> TKikimrTableMetadataPtr;

template<typename TResult>
class IKikimrAsyncResult : public TThrRefBase {
public:
    virtual bool HasResult() const = 0;
    virtual TResult GetResult() = 0;
    virtual NThreading::TFuture<bool> Continue() = 0;

    virtual ~IKikimrAsyncResult() {}
};

template<typename TResult>
class TKikimrResultHolder : public IKikimrAsyncResult<TResult> {
public:
    TKikimrResultHolder(TResult&& result)
        : Result(std::move(result)) {}

    bool HasResult() const override {
        return Full;
    }

    TResult GetResult() override {
        Full = false;
        return std::move(Result);
    }

    NThreading::TFuture<bool> Continue() override {
        return NThreading::MakeFuture<bool>(true);
    }

private:
    TResult Result;
    bool Full = true;
};

template<typename TResult>
static TIntrusivePtr<TKikimrResultHolder<TResult>> MakeKikimrResultHolder(TResult&& result) {
    return MakeIntrusive<TKikimrResultHolder<TResult>>(std::move(result));
}

class IKikimrGateway : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IKikimrGateway>;

    struct TGenericResult : public NCommon::TOperationResult {
    };

    struct TListPathResult : public TGenericResult {
        TString Path;
        TVector<TKikimrListPathItem> Items;
    };

    struct TTableMetadataResult : public TGenericResult {
        TKikimrTableMetadataPtr Metadata;
    };

    struct TQueryResult : public TGenericResult {
        TString SessionId;
        TVector<NKikimrMiniKQL::TResult*> Results;
        TMaybe<NKikimrKqp::TQueryProfile> Profile; // TODO: Deprecate.
        NKqpProto::TKqpStatsQuery QueryStats;
        std::unique_ptr<NKikimrKqp::TPreparedQuery> PreparingQuery;
        std::shared_ptr<const NKikimrKqp::TPreparedQuery> PreparedQuery;
        TString QueryAst;
        TString QueryPlan;
        std::shared_ptr<google::protobuf::Arena> ProtobufArenaPtr;
        TMaybe<ui16> SqlVersion;
    };

    struct TLoadTableMetadataSettings {
        TLoadTableMetadataSettings& WithTableStats(bool enable) {
            RequestStats_ = enable;
            return *this;
        }

        TLoadTableMetadataSettings& WithPrivateTables(bool enable) {
            WithPrivateTables_ = enable;
            return *this;
        }

        bool RequestStats_ = false;
        bool WithPrivateTables_ = false;
    };

    class IKqpTableMetadataLoader : public std::enable_shared_from_this<IKqpTableMetadataLoader> {
    public:
        virtual NThreading::TFuture<TTableMetadataResult> LoadTableMetadata(
            const TString& cluster, const TString& table, const TLoadTableMetadataSettings& settings, const TString& database,
            const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) = 0;

        virtual TVector<TString> GetCollectedSchemeData() = 0;

        virtual ~IKqpTableMetadataLoader() = default;
    };

public:
    virtual bool HasCluster(const TString& cluster) = 0;
    virtual TVector<TString> GetClusters() = 0;
    virtual TString GetDefaultCluster() = 0;
    virtual TMaybe<TString> GetSetting(const TString& cluster, const TString& name) = 0;

    virtual void SetToken(const TString& cluster, const TIntrusiveConstPtr<NACLib::TUserToken>& token) = 0;

    virtual NThreading::TFuture<TListPathResult> ListPath(const TString& cluster, const TString& path) = 0;

    virtual NThreading::TFuture<TTableMetadataResult> LoadTableMetadata(
        const TString& cluster, const TString& table, TLoadTableMetadataSettings settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateTable(TKikimrTableMetadataPtr metadata, bool createDir) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterTable(const TString& cluster, Ydb::Table::AlterTableRequest&& req, const TMaybe<TString>& requestType) = 0;

    virtual NThreading::TFuture<TGenericResult> RenameTable(const TString& src, const TString& dst, const TString& cluster) = 0;

    virtual NThreading::TFuture<TGenericResult> DropTable(const TString& cluster, const TString& table) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateTopic(const TString& cluster, Ydb::Topic::CreateTopicRequest&& request) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterTopic(const TString& cluster, Ydb::Topic::AlterTopicRequest&& request) = 0;

    virtual NThreading::TFuture<TGenericResult> DropTopic(const TString& cluster, const TString& topic) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateUser(const TString& cluster, const TCreateUserSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterUser(const TString& cluster, const TAlterUserSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> DropUser(const TString& cluster, const TDropUserSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateObject(const TString& cluster, const TCreateObjectSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterObject(const TString& cluster, const TAlterObjectSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> DropObject(const TString& cluster, const TDropObjectSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateGroup(const TString& cluster, const TCreateGroupSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterGroup(const TString& cluster, TAlterGroupSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> DropGroup(const TString& cluster, const TDropGroupSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateColumnTable(TKikimrTableMetadataPtr metadata, bool createDir) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterColumnTable(const TString& cluster, const TAlterColumnTableSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateTableStore(const TString& cluster, const TCreateTableStoreSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterTableStore(const TString& cluster, const TAlterTableStoreSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> DropTableStore(const TString& cluster, const TDropTableStoreSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateExternalTable(const TString& cluster, const TCreateExternalTableSettings& settings, bool createDir) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterExternalTable(const TString& cluster, const TAlterExternalTableSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> DropExternalTable(const TString& cluster, const TDropExternalTableSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> CreateExternalDataSource(const TString& cluster, const TCreateExternalDataSourceSettings& settings, bool createDir) = 0;

    virtual NThreading::TFuture<TGenericResult> AlterExternalDataSource(const TString& cluster, const TAlterExternalDataSourceSettings& settings) = 0;

    virtual NThreading::TFuture<TGenericResult> DropExternalDataSource(const TString& cluster, const TDropExternalDataSourceSettings& settings) = 0;

    virtual TVector<TString> GetCollectedSchemeData() = 0;

public:
    using TCreateDirFunc = std::function<void(const TString&, const TString&, NThreading::TPromise<TGenericResult>)>;

    static TString CanonizePath(const TString& path);

    template <typename TIter>
    static TString CombinePath(TIter begin, TIter end, bool canonize = true) {
        auto path = JoinRange("/", begin, end);
        return canonize
            ? CanonizePath(path)
            : path;
    }

    static TVector<TString> SplitPath(const TString& path);

    static bool TrySplitTablePath(const TString& path, std::pair<TString, TString>& result, TString& error);

    static NThreading::TFuture<TGenericResult> CreatePath(const TString& path, TCreateDirFunc createDir);

    static TString CreateIndexTablePath(const TString& tableName, const TString& indexName);

    static void BuildIndexMetadata(TTableMetadataResult& loadTableMetadataResult);
};

EYqlIssueCode YqlStatusFromYdbStatus(ui32 ydbStatus);
Ydb::FeatureFlag::Status GetFlagValue(const TMaybe<bool>& value);

void SetColumnType(Ydb::Type& protoType, const TString& typeName, bool notNull);
bool ConvertReadReplicasSettingsToProto(const TString settings, Ydb::Table::ReadReplicasSettings& proto,
    Ydb::StatusIds::StatusCode& code, TString& error);
void ConvertTtlSettingsToProto(const NYql::TTtlSettings& settings, Ydb::Table::TtlSettings& proto);

} // namespace NYql

template<>
struct THash<NYql::TKikimrPathId> {
    inline ui64 operator()(const NYql::TKikimrPathId& x) const noexcept {
        return x.Hash();
    }
};
