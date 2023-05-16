#pragma once

#include "yql_data_provider.h"
#include "yql_udf_index_package_set.h"
#include "yql_udf_resolver.h"
#include "yql_user_data_storage.h"
#include "yql_arrow_resolver.h"

#include <ydb/library/yql/public/udf/udf_validate.h>
#include <ydb/library/yql/core/credentials/yql_credentials.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/enumbitset/enumbitset.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/digest/city.h>

#include <vector>

namespace NYql {

class IUrlLoader : public TThrRefBase {
public:
    ~IUrlLoader() = default;

    virtual TString Load(const TString& url, const TString& token) = 0;

    using TPtr = TIntrusivePtr<IUrlLoader>;
};

class TModuleResolver : public IModuleResolver {
public:
    TModuleResolver(TModulesTable&& modules, ui64 nextUniqueId, const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags, bool optimizeLibraries = true, THolder<TExprContext> ownedCtx = {})
        : OwnedCtx(std::move(ownedCtx))
        , LibsContext(nextUniqueId)
        , Modules(std::move(modules))
        , ClusterMapping(clusterMapping)
        , SqlFlags(sqlFlags)
        , OptimizeLibraries(optimizeLibraries)
    {
        if (OwnedCtx) {
            FreezeGuard = MakeHolder<TExprContext::TFreezeGuard>(*OwnedCtx);
        }
    }

    TModuleResolver(const TModulesTable* parentModules, ui64 nextUniqueId, const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags, bool optimizeLibraries, const TSet<TString>& knownPackages, const THashMap<TString, THashMap<int, TLibraryCohesion>>& libs)
        : ParentModules(parentModules)
        , LibsContext(nextUniqueId)
        , KnownPackages(knownPackages)
        , Libs(libs)
        , ClusterMapping(clusterMapping)
        , SqlFlags(sqlFlags)
        , OptimizeLibraries(optimizeLibraries)
    {
    }

    static TString NormalizeModuleName(const TString& path);

    void AttachUserData(TUserDataStorage::TPtr userData) {
        UserData = userData;
    }

    void SetUrlLoader(IUrlLoader::TPtr loader) {
        UrlLoader = loader;
    }

    void SetParameters(const NYT::TNode& node) {
        Parameters = node;
    }

    void SetCredentials(TCredentials::TPtr credentials) {
        Credentials = std::move(credentials);
    }

    void RegisterPackage(const TString& package) override;
    bool SetPackageDefaultVersion(const TString& package, ui32 version) override;
    const TExportTable* GetModule(const TString& module) const override;
    bool AddFromFile(const std::string_view& file, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) final;
    bool AddFromUrl(const std::string_view& file, const std::string_view& url, const std::string_view& tokenName, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) final;
    bool AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) final;
    bool AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, TString& moduleName, std::vector<TString>* exports = nullptr, std::vector<TString>* imports = nullptr) final;
    bool Link(TExprContext& ctx) override;
    void UpdateNextUniqueId(TExprContext& ctx) const override;
    ui64 GetNextUniqueId() const override;
    IModuleResolver::TPtr CreateMutableChild() const override;

private:
    bool AddFromMemory(const TString& fullName, const TString& moduleName, bool isYql, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, std::vector<TString>* exports = nullptr, std::vector<TString>* imports = nullptr);
    THashMap<TString, TLibraryCohesion> FilterLibsByVersion() const;
    static TString ExtractPackageNameFromModule(TStringBuf moduleName);
    TString SubstParameters(const TString& str);

private:
    THolder<TExprContext> OwnedCtx;
    const TModulesTable* ParentModules = nullptr;
    TUserDataStorage::TPtr UserData;
    IUrlLoader::TPtr UrlLoader;
    TMaybe<NYT::TNode> Parameters;
    TCredentials::TPtr Credentials;
    TExprContext LibsContext;
    TSet<TString> KnownPackages;
    THashMap<TString, ui32> PackageVersions;
    THashMap<TString, THashMap<int, TLibraryCohesion>> Libs;
    TModulesTable Modules;
    const THashMap<TString, TString> ClusterMapping;
    const THashSet<TString> SqlFlags;
    const bool OptimizeLibraries;
    THolder<TExprContext::TFreezeGuard> FreezeGuard;
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

using TColumnOrder = TVector<TString>;
TString FormatColumnOrder(const TMaybe<TColumnOrder>& columnOrder);
ui64 AddColumnOrderHash(const TMaybe<TColumnOrder>& columnOrder, ui64 hash);

class TColumnOrderStorage: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TColumnOrderStorage>;
    TColumnOrderStorage() = default;

    TMaybe<TColumnOrder> Lookup(ui64 uniqueId) const {
        auto it = Storage.find(uniqueId);
        if (it == Storage.end()) {
            return {};
        }
        return it->second;
    }

    void Set(ui64 uniqueId, const TColumnOrder& order) {
        Storage[uniqueId] = order;
    }
private:
    THashMap<ui64, TColumnOrder> Storage;
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

struct TUdfCachedInfo {
    const TTypeAnnotationNode* FunctionType = nullptr;
    const TTypeAnnotationNode* RunConfigType = nullptr;
    const TTypeAnnotationNode* NormalizedUserType = nullptr;
    bool SupportsBlocks = false;
    bool IsStrict = false;
};

struct TTypeAnnotationContext: public TThrRefBase {
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
    NUdf::EValidateMode ValidateMode = NUdf::EValidateMode::None;
    bool DisableNativeUdfSupport = false;
    TMaybe<TString> OptLLVM;
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
    ui32 EvaluateOrderByColumnLimit = 100;
    bool PullUpFlatMapOverJoin = true;
    bool DeprecatedSQL = false;
    THashMap<std::tuple<TString, TString, const TTypeAnnotationNode*>, TUdfCachedInfo> UdfTypeCache; // (name,typecfg,type)->info
    bool UseTableMetaFromGraph = false;
    bool DiscoveryMode = false;
    bool ForceDq = false;
    bool DqCaptured = false; // TODO: Add before/after recapture transformers
    EFallbackPolicy DqFallbackPolicy = EFallbackPolicy::Default;
    bool StrictTableProps = true;
    bool JsonQueryReturnsJsonDocument = false;
    bool YsonCastToString = true;
    ui32 FolderSubDirsLimit = 1000;
    bool UseBlocks = false;
    IArrowResolver::TPtr ArrowResolver;

    // compatibility with v0 or raw s-expression code
    bool OrderedColumns = false;
    TColumnOrderStorage::TPtr ColumnOrderStorage = new TColumnOrderStorage;

    TMaybe<TColumnOrder> LookupColumnOrder(const TExprNode& node) const;
    IGraphTransformer::TStatus SetColumnOrder(const TExprNode& node, const TColumnOrder& columnOrder, TExprContext& ctx);

    // cached constants
    std::optional<ui64> CachedNow;
    std::tuple<std::optional<ui64>, std::optional<double>, std::optional<TGUID>> CachedRandom;

    std::optional<bool> InitializeResult;
    EHiddenMode HiddenMode = EHiddenMode::Disable;

    template <typename T>
    T GetRandom() const noexcept;

    template <typename T>
    T GetCachedRandom() noexcept {
        auto& cached = std::get<std::optional<T>>(CachedRandom);
        if (!cached) {
            cached = GetRandom<T>();
        }
        return *cached;
    }

    ui64 GetCachedNow() noexcept {
        if (!CachedNow) {
            CachedNow = TimeProvider->Now().GetValue();
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
