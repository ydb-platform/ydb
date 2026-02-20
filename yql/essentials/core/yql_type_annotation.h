#pragma once

#include "yql_data_provider.h"
#include "yql_udf_index_package_set.h"
#include "yql_udf_resolver.h"
#include "yql_user_data_storage.h"
#include "yql_arrow_resolver.h"
#include "yql_statistics.h"

#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>

#include <yql/essentials/public/udf/udf_validate.h>
#include <yql/essentials/public/udf/udf_log.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/url_lister/interface/url_lister_manager.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/core/layers/layers.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/sql/settings/translation_sql_flags.h>
#include <yql/essentials/sql/sql.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/enumbitset/enumbitset.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/digest/city.h>

#include <functional>
#include <vector>

namespace NYql {

using TTypeAnnCallableFactory = std::function<TAutoPtr<IGraphTransformer>()>;

class IUrlLoader : public TThrRefBase {
public:
    ~IUrlLoader() override = default;

    virtual TString Load(const TString& url, const TString& token) = 0;

    using TPtr = TIntrusivePtr<IUrlLoader>;
};

class TModuleResolver : public IModuleResolver {
public:
    using TModuleChecker = std::function<bool(const TString& query, const TString& fileName, TExprContext& ctx)>;

    TModuleResolver(const NSQLTranslation::TTranslators& translators, TModulesTable&& modules,
        ui64 nextUniqueId, const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags, bool optimizeLibraries = true,
        THolder<TExprContext> ownedCtx = {}, TModuleChecker moduleChecker = {})
        : Translators_(translators)
        , OwnedCtx_(std::move(ownedCtx))
        , LibsContext_(nextUniqueId)
        , Modules_(std::move(modules))
        , ClusterMapping_(clusterMapping)
        , SqlFlags_(sqlFlags)
        , ModuleChecker_(moduleChecker)
        , OptimizeLibraries_(optimizeLibraries)
    {
        if (OwnedCtx_) {
            FreezeGuard_ = MakeHolder<TExprContext::TFreezeGuard>(*OwnedCtx_);
        }
    }

    TModuleResolver(const NSQLTranslation::TTranslators& translators, const TModulesTable* parentModules,
        ui64 nextUniqueId, const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags, bool optimizeLibraries, const TSet<TString>& knownPackages, const THashMap<TString,
        THashMap<int, TLibraryCohesion>>& libs, const TString& fileAliasPrefix, TModuleChecker moduleChecker)
        : Translators_(translators)
        , ParentModules_(parentModules)
        , LibsContext_(nextUniqueId)
        , KnownPackages_(knownPackages)
        , Libs_(libs)
        , ClusterMapping_(clusterMapping)
        , SqlFlags_(sqlFlags)
        , ModuleChecker_(moduleChecker)
        , OptimizeLibraries_(optimizeLibraries)
        , FileAliasPrefix_(fileAliasPrefix)
    {
    }

    static TString NormalizeModuleName(const TString& path);

    void AttachUserData(TUserDataStorage::TPtr userData) {
        UserData_ = userData;
    }

    void SetUrlLoader(IUrlLoader::TPtr loader) {
        UrlLoader_ = loader;
    }

    void SetParameters(const NYT::TNode& node) {
        Parameters_ = node;
    }

    void SetCredentials(TCredentials::TPtr credentials) {
        Credentials_ = std::move(credentials);
    }

    void SetQContext(const TQContext& qContext) {
        QContext_ = qContext;
    }

    void SetClusterMapping(const THashMap<TString, TString>& clusterMapping) {
        ClusterMapping_ = clusterMapping;
    }
    void SetSqlFlags(const THashSet<TString>& flags) {
        SqlFlags_ = flags;
    }

    void SetModuleChecker(TModuleChecker moduleChecker) {
        ModuleChecker_ = moduleChecker;
    }

    void SetUseCanonicalLibrarySuffix(bool use) {
        UseCanonicalLibrarySuffix_ = use;
    }

    void RegisterPackage(const TString& package) override;
    bool SetPackageDefaultVersion(const TString& package, ui32 version) override;
    const TExportTable* GetModule(const TString& module) const override;
    void WriteStatistics(NYson::TYsonWriter& writer) override;
    bool AddFromFile(const std::string_view& file, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) final;
    bool AddFromUrl(const std::string_view& file, const std::string_view& url, const std::string_view& tokenName, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) final;
    bool AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) final;
    bool AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, TString& moduleName, std::vector<TString>* exports = nullptr, std::vector<TString>* imports = nullptr) final;
    bool Link(TExprContext& ctx) override;
    void UpdateNextUniqueId(TExprContext& ctx) const override;
    ui64 GetNextUniqueId() const override;
    IModuleResolver::TPtr CreateMutableChild() const override;
    void SetFileAliasPrefix(TString&& prefix) override;
    TString GetFileAliasPrefix() const override;

private:
    bool AddFromMemory(const TString& fullName, const TString& moduleName, bool sExpr, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, std::vector<TString>* exports = nullptr, std::vector<TString>* imports = nullptr);
    THashMap<TString, TLibraryCohesion> FilterLibsByVersion() const;
    static TString ExtractPackageNameFromModule(TStringBuf moduleName);
    TString SubstParameters(const TString& str);
    bool IsSExpr(bool isYql, bool isYqls, const TString& body) const;

private:
    const NSQLTranslation::TTranslators Translators_;
    THolder<TExprContext> OwnedCtx_;
    const TModulesTable* ParentModules_ = nullptr;
    TUserDataStorage::TPtr UserData_;
    IUrlLoader::TPtr UrlLoader_;
    TMaybe<NYT::TNode> Parameters_;
    TCredentials::TPtr Credentials_;
    TQContext QContext_;
    TExprContext LibsContext_;
    TSet<TString> KnownPackages_;
    THashMap<TString, ui32> PackageVersions_;
    THashMap<TString, THashMap<int, TLibraryCohesion>> Libs_;
    TModulesTable Modules_;
    THashMap<TString, TString> ClusterMapping_;
    THashSet<TString> SqlFlags_;
    TModuleChecker ModuleChecker_;
    const bool OptimizeLibraries_;
    THolder<TExprContext::TFreezeGuard> FreezeGuard_;
    TString FileAliasPrefix_;
    bool UseCanonicalLibrarySuffix_ = false;
    TSet<TString> UsedSuffixes_;
};

bool SplitUdfName(TStringBuf name, TStringBuf& moduleName, TStringBuf& funcName);

struct TUdfInfo {
    TString FileAlias;
    TString Prefix;
};
// external module name -> alias of file and prefix
using TUdfModulesTable = THashMap<TString, TUdfInfo>;

struct TYqlOperationOptions {
    TString Runner;
    TMaybe<TString> AuthenticatedUser;
    TMaybe<TString> Id;
    TMaybe<TString> SharedId;
    TMaybe<TString> QueryName;
    TMaybe<TString> Title;
    TMaybe<TString> Url;
    TMaybe<TString> AttrsYson;
    TMaybe<NYT::TNode> ParametersYson;
};

class TColumnOrder {
public:
    struct TOrderedItem {
        TString LogicalName;
        TString PhysicalName;
        TOrderedItem(const TString& logical, const TString& physical) : LogicalName(logical), PhysicalName(physical) {}
        TOrderedItem(TOrderedItem&&) = default;
        TOrderedItem(const TOrderedItem&) = default;
        TOrderedItem& operator=(const TOrderedItem&) = default;
        bool operator==(const TOrderedItem& other) const {
            return LogicalName == other.LogicalName && PhysicalName == other.PhysicalName;
        }
    };
    TColumnOrder() = default;
    TColumnOrder(const TColumnOrder&) = default;
    TColumnOrder(TColumnOrder&&) = default;
    TColumnOrder& operator=(const TColumnOrder&);
    explicit TColumnOrder(const TVector<TString>& order);
    TString AddColumn(const TString& name);

    bool IsDuplicatedIgnoreCase(const TString& name) const;

    void Shrink(size_t remain);

    void Reserve(size_t);
    void EraseIf(const std::function<bool(const TString&)>& fn);
    void EraseIf(const std::function<bool(const TOrderedItem&)>& fn);
    void Clear();

    size_t Size() const;

    TString Find(const TString&) const;

    TVector<TOrderedItem>::const_iterator begin() const {
        return Order_.cbegin();
    }

    TVector<TOrderedItem>::const_iterator end() const {
        return Order_.cend();
    }

    const TOrderedItem& operator[](size_t i) const {
        return Order_[i];
    }

    bool operator==(const TColumnOrder& other) const {
        return Order_ == other.Order_;
    }

    const TOrderedItem& at(size_t i) const {
        return Order_[i];
    }

    const TOrderedItem& front() const {
        return Order_.front();
    }

    const TOrderedItem& back() const {
        return Order_.back();
    }

    TVector<TString> GetLogicalNames() const {
        TVector<TString> res;
        res.reserve(Order_.size());
        for (const auto &[name, _]: Order_) {
            res.emplace_back(name);
        }
        return res;
    }

    TVector<TString> GetPhysicalNames() const {
        TVector<TString> res;
        res.reserve(Order_.size());
        for (const auto &[_, name]: Order_) {
            res.emplace_back(name);
        }
        return res;
    }

    bool HasDuplicates() const {
        for (const auto& e: Order_) {
            if (e.PhysicalName != e.LogicalName) {
                return true;
            }
        }
        return false;
    }
private:
    THashMap<TString, TString> GeneratedToOriginal_;
    THashMap<TString, uint64_t> UseCount_;
    THashMap<TString, uint64_t> UseCountLcase_;
    // (name, generated_name)
    TVector<TOrderedItem> Order_;
};

TString FormatColumnOrder(const TMaybe<TColumnOrder>& columnOrder, TMaybe<size_t> maxColumns = {});
ui64 AddColumnOrderHash(const TMaybe<TColumnOrder>& columnOrder, ui64 hash);

class TColumnOrderStorage: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TColumnOrderStorage>;
    TColumnOrderStorage() = default;

    TMaybe<TColumnOrder> Lookup(ui64 uniqueId) const {
        auto it = Storage_.find(uniqueId);
        if (it == Storage_.end()) {
            return {};
        }
        return it->second;
    }

    void Set(ui64 uniqueId, const TColumnOrder& order) {
        Storage_[uniqueId] = order;
    }
private:
    THashMap<ui64, TColumnOrder> Storage_;
};

enum class EHiddenMode {
    Disable /* "disable" */,
    Force /* "force" */,
    Debug /* "debug" */,
    Auto /* "auto" */
};

enum class EFallbackPolicy {
    Default     /* "default" */,
    Never       /* "never" */,
    Always      /* "always" */
};

enum class ECostBasedOptimizerType {
    Disable /* "disable" */,
    PG /* "pg" */,
    Native /* "native" */
};

enum class EMatchRecognizeStreamingMode {
    Disable,
    Auto,
    Force,
};

enum class EBlockEngineMode {
    Disable /* "disable" */,
    Auto /* "auto" */,
    Force /* "force" */,
};

enum class EEngineType {
    Default /* "default" */,
    Dq /* "dq" */,
    Ytflow /* "ytflow" */,
};

struct TUdfCachedInfo {
    TString NormalizedName;
    const TTypeAnnotationNode* FunctionType = nullptr;
    const TTypeAnnotationNode* RunConfigType = nullptr;
    const TTypeAnnotationNode* NormalizedUserType = nullptr;
    bool SupportsBlocks = false;
    bool IsStrict = false;
    TLangVersion MinLangVer = UnknownLangVersion;
    TLangVersion MaxLangVer = UnknownLangVersion;
};

struct TLineageStats {
    TMaybe<bool> Correct;
    TMaybe<bool> CorrectStandalone;
    ui64 Size = 0;
};

const TString TypeAnnotationContextComponent = "TypeAnnotationContext";
const TString NowKey = "Now";
const TString RandomKey = "Random";
const TString RandomNumberKey = "RandomNumber";
const TString RandomUuidKey = "RandomUuid";

template <typename T>
inline TString SerializeBinary(const T& value) {
    return TString((const char*)&value, sizeof(T));
}

template <typename T>
inline T DeserializeBinary(const TString& value) {
    return *(const T*)value.data();
}

template <typename T>
inline TString GetRandomKey();

template <>
inline TString GetRandomKey<ui64>() {
    return RandomNumberKey;
}

template <>
inline TString GetRandomKey<double>() {
    return RandomKey;
}

template <>
inline TString GetRandomKey<TGUID>() {
    return RandomUuidKey;
}

struct TTypeAnnotationContext: public TThrRefBase {
    TSimpleSharedPtr<NDq::TOrderingsStateMachine> SortingsFSM;
    TSimpleSharedPtr<NDq::TOrderingsStateMachine> OrderingsFSM;
    TLangVersion LangVer = MinLangVersion;
    EBackportCompatibleFeaturesMode BackportMode = EBackportCompatibleFeaturesMode::None;
    bool UseTypeDiffForConvertToError = false;
    bool DebugPositions = false;
    THashMap<TString, TIntrusivePtr<TOptimizerStatistics::TColumnStatMap>> ColumnStatisticsByTableName;
    THashMap<ui64, std::shared_ptr<TOptimizerStatistics>> StatisticsMap;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    THashMap<TString, TIntrusivePtr<IDataProvider>> DataSourceMap;
    THashMap<TString, TIntrusivePtr<IDataProvider>> DataSinkMap;
    TVector<TIntrusivePtr<IDataProvider>> DataSources;
    TVector<TIntrusivePtr<IDataProvider>> DataSinks;
    TUdfIndex::TPtr UdfIndex;
    TUdfIndexPackageSet::TPtr UdfIndexPackageSet;
    IUdfResolver::TPtr UdfResolver;
    THashMap<TString, IUdfResolver::TImport> UdfImports; // aliases of files that was imported => list of module names
    TUdfModulesTable UdfModules;
    TString PureResultDataSource;
    TVector<TString> AvailablePureResultDataSources;
    TString FullResultDataSink;
    TUserDataStorage::TPtr UserDataStorage;
    TUserDataTable UserDataStorageCrutches;
    TYqlOperationOptions OperationOptions;
    TCredentials::TPtr Credentials = MakeIntrusive<TCredentials>();
    IModuleResolver::TPtr Modules;
    IUrlListerManagerPtr UrlListerManager;
    NUdf::EValidateMode ValidateMode = NUdf::EValidateMode::None;
    bool DisableNativeUdfSupport = false;
    TMaybe<TString> OptLLVM;
    NUdf::ELogLevel RuntimeLogLevel = NUdf::ELogLevel::Info;
    bool IsReadOnly = false;
    TAutoPtr<IGraphTransformer> CustomInstantTypeTransformer;
    bool Diagnostics = false;
    THashMap<ui64, ui32> NodeToOperationId; // UniqueId->PublicId translation
    ui64 EvaluationInProgress = 0;
    THashMap<ui64, const TTypeAnnotationNode*> ExpectedTypes;
    THashMap<ui64, std::vector<const TConstraintNode*>> ExpectedConstraints;
    THashMap<ui64, TColumnOrder> ExpectedColumnOrders;
    THashSet<TString> DisableConstraintCheck;
    bool UdfSupportsYield = false;
    ui32 EvaluateForLimit = 500;
    ui32 EvaluateParallelForLimit = 5000;
    ui32 EvaluateOrderByColumnLimit = 100;
    ui32 PgIterateLimit = 500;
    bool PullUpFlatMapOverJoin = true;
    bool FilterPushdownOverJoinOptionalSide = false;
    bool RotateJoinTree = true;
    bool DeprecatedSQL = false;
    THashMap<std::tuple<TString, TString, const TTypeAnnotationNode*>, TUdfCachedInfo> UdfTypeCache; // (name,typecfg,type)->info
    bool UseTableMetaFromGraph = false;
    bool DiscoveryMode = false;
    bool WindowNewPipeline = false;
    bool ForceDq = false;
    bool DqCaptured = false; // TODO: Add before/after recapture transformers
    EFallbackPolicy DqFallbackPolicy = EFallbackPolicy::Default;
    bool StrictTableProps = true;
    bool JsonQueryReturnsJsonDocument = false;
    bool YsonCastToString = true;
    bool CaseInsensitiveNamedArgs = false;
    ui32 FolderSubDirsLimit = 1000;
    bool UseBlocks = false;
    EBlockEngineMode BlockEngineMode = EBlockEngineMode::Disable;
    THashMap<TString, size_t> NoBlockRewriteCallableStats;
    THashMap<TString, size_t> NoBlockRewriteTypeStats;
    TMaybe<bool> PgEmitAggApply;
    IArrowResolver::TPtr ArrowResolver;
    TFileStoragePtr FileStorage;
    TQContext QContext;
    ECostBasedOptimizerType CostBasedOptimizer = ECostBasedOptimizerType::Disable;
    ui32 CostBasedOptimizerVersion = 0;
    bool MatchRecognize = false;
    TMaybe<NSQLTranslation::TSqlFlags> SqlFlags;
    EMatchRecognizeStreamingMode MatchRecognizeStreaming = EMatchRecognizeStreamingMode::Force;
    i64 TimeOrderRecoverDelay = -10'000'000; //microseconds
    i64 TimeOrderRecoverAhead = 10'000'000; //microseconds
    ui32 TimeOrderRecoverRowLimit = 1'000'000;
    // compatibility with v0 or raw s-expression code
    bool OrderedColumns = false;
    bool DeriveColumnOrder = false;
    TColumnOrderStorage::TPtr ColumnOrderStorage = new TColumnOrderStorage;
    THashSet<TString> OptimizerFlags;
    THashSet<TString> PeepholeFlags;
    bool StreamLookupJoin = false;
    ui32 MaxAggPushdownPredicates = 6; // algorithm complexity is O(2^N)
    ui32 PruneKeysMemLimit = 128 * 1024 * 1024;
    bool NormalizeDependsOn = false;
    ui32 AndOverOrExpansionLimit = 100;
    bool EarlyExpandSeq = true;
    bool DirectRowDependsOn = true;
    bool EnableLineage = false;
    bool EnableStandaloneLineage = false;
    TLineageStats LineageStats;
    ui64 LineageOutputLimit = 40 * 1024 * 1024; // 40 mb limit for lineage representation
    ui64 LineageMemoryLimit = 150 * 1024 * 1024; // 150 mb limit for memory allocation in lineage calculation

    THashMap<TString, NLayers::IRemoteLayerProviderPtr> RemoteLayerProviderByName;
    NLayers::ILayersRegistryPtr LayersRegistry;

    TMaybe<TColumnOrder> LookupColumnOrder(const TExprNode& node) const;
    IGraphTransformer::TStatus SetColumnOrder(const TExprNode& node, const TColumnOrder& columnOrder, TExprContext& ctx);

    // cached constants
    std::optional<ui64> CachedNow;
    std::tuple<std::optional<ui64>, std::optional<double>, std::optional<TGUID>> CachedRandom;

    std::optional<bool> InitializeResult;
    EHiddenMode HiddenMode = EHiddenMode::Disable;
    EEngineType EngineType = EEngineType::Default;

    // temporary flag to skip applying ExpandPg rules
    bool IgnoreExpandPg = false;

    template <typename T>
    T GetRandom() const noexcept;

    template <typename T>
    T GetCachedRandom() {
        auto& cached = std::get<std::optional<T>>(CachedRandom);
        if (!cached) {
            if (QContext.CanRead()) {
                auto item = QContext.GetReader()->Get({TypeAnnotationContextComponent, GetRandomKey<T>()}).GetValueSync();
                if (!item) {
                    throw yexception() << "Missing replay data";
                }

                cached = DeserializeBinary<T>(item->Value);
            } else {
                cached = GetRandom<T>();
                if (QContext.CanWrite()) {
                    QContext.GetWriter()->Put({TypeAnnotationContextComponent, GetRandomKey<T>()}, SerializeBinary<T>(*cached)).GetValueSync();
                }
            }
        }
        return *cached;
    }

    ui64 GetCachedNow() {
        if (!CachedNow) {
            if (QContext.CanRead()) {
                auto item = QContext.GetReader()->Get({TypeAnnotationContextComponent, NowKey}).GetValueSync();
                if (!item) {
                    throw yexception() << "Missing replay data";
                }

                CachedNow = DeserializeBinary<ui64>(item->Value);
            } else {
                CachedNow = TimeProvider->Now().GetValue();
                if (QContext.CanWrite()) {
                    QContext.GetWriter()->Put({TypeAnnotationContextComponent, NowKey}, SerializeBinary<ui64>(*CachedNow)).GetValueSync();
                }
            }
        }
        return *CachedNow;
    }

    void AddDataSource(TStringBuf name, TIntrusivePtr<IDataProvider> provider) {
        DataSourceMap[name] = provider;
        DataSources.push_back(std::move(provider));
    }

    void AddDataSource(const THashSet<TString>& names, TIntrusivePtr<IDataProvider> provider) {
        for (auto name: names) {
            DataSourceMap[name] = provider;
        }
        DataSources.push_back(std::move(provider));
    }

    void AddDataSink(TStringBuf name, TIntrusivePtr<IDataProvider> provider) {
        DataSinkMap[name] = provider;
        DataSinks.push_back(std::move(provider));
    }

    void AddDataSink(const THashSet<TString>& names, TIntrusivePtr<IDataProvider> provider) {
        for (auto name: names) {
            DataSinkMap[name] = provider;
        }
        DataSinks.push_back(std::move(provider));
    }

    void AddRemoteLayersProvider(const TString& name, NLayers::IRemoteLayerProviderPtr provider) {
        RemoteLayerProviderByName[name] = provider;
    }

    bool Initialize(TExprContext& ctx);
    bool DoInitialize(TExprContext& ctx);

    TString GetDefaultDataSource() const;

    TMaybe<ui32> TranslateOperationId(ui64 id) const {
        auto it = NodeToOperationId.find(id);
        return it == NodeToOperationId.end() ? Nothing() : MakeMaybe(it->second);
    }

    bool IsConstraintCheckEnabled(TStringBuf name) const {
        return DisableConstraintCheck.find(name) == DisableConstraintCheck.end();
    }

    template <class TConstraint>
    bool IsConstraintCheckEnabled() const {
        return DisableConstraintCheck.find(TConstraint::Name()) == DisableConstraintCheck.end();
    }

    void Reset();

    /**
     * Helper method to check statistics in type annotation context
     */
    bool ContainsStats(const TExprNode* input) {
        return StatisticsMap.contains(input ? input->UniqueId() : 0);
    }

    /**
     * Helper method to fetch statistics from type annotation context
     */
    std::shared_ptr<TOptimizerStatistics> GetStats(const TExprNode* input) const {
        return StatisticsMap.Value(input ? input->UniqueId() : 0, std::shared_ptr<TOptimizerStatistics>(nullptr));
    }

    /**
     * Helper method to set statistics in type annotation context
     */
    void SetStats(const TExprNode* input, std::shared_ptr<TOptimizerStatistics> stats) {
        StatisticsMap[input ? input->UniqueId() : 0] = stats;
    }

    bool IsBlockEngineEnabled() const {
        return BlockEngineMode != EBlockEngineMode::Disable || UseBlocks;
    }

    void IncNoBlockCallable(TStringBuf callableName);
    void IncNoBlockType(const TTypeAnnotationNode& type);
    void IncNoBlockType(ETypeAnnotationKind kind);
    void IncNoBlockType(NUdf::EDataSlot slot);

    TVector<TString> GetTopNoBlocksCallables(size_t maxCount) const;
    TVector<TString> GetTopNoBlocksTypes(size_t maxCount) const;
};

template <> inline
double TTypeAnnotationContext::GetRandom<double>() const noexcept {
    return RandomProvider->GenRandReal2();
}

template <> inline
ui64 TTypeAnnotationContext::GetRandom<ui64>() const noexcept {
    return RandomProvider->GenRand64();
}

template <> inline
TGUID TTypeAnnotationContext::GetRandom<TGUID>() const noexcept {
    return RandomProvider->GenUuid4();
}

using TTypeAnnotationContextPtr = TIntrusivePtr<TTypeAnnotationContext>;

} // namespace NYql
