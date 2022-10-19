#include "yql_type_annotation.h"

#include "yql_expr_type_annotation.h"
#include "yql_library_compiler.h"
#include "yql_type_helpers.h"

#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/ast/yql_constraint.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/stream/file.h>
#include <util/string/join.h>

namespace NYql {

using namespace NKikimr;

bool TTypeAnnotationContext::Initialize(TExprContext& ctx) {
    if (!InitializeResult) {
        InitializeResult = DoInitialize(ctx);
    }

    return *InitializeResult;
}

bool TTypeAnnotationContext::DoInitialize(TExprContext& ctx) {
    for (auto& x : DataSources) {
        if (!x->Initialize(ctx)) {
            return false;
        }
    }

    for (auto& x : DataSinks) {
        if (!x->Initialize(ctx)) {
            return false;
        }
    }

    Y_ENSURE(UserDataStorage);

    // Disable "in progress" constraints
    //DisableConstraintCheck.emplace(TSortedConstraintNode::Name());
    //DisableConstraintCheck.emplace(TEmptyConstraintNode::Name());
    DisableConstraintCheck.emplace(TUniqueConstraintNode::Name());
    //DisableConstraintCheck.emplace(TMultiConstraintNode::Name());
    //DisableConstraintCheck.emplace(TVarIndexConstraintNode::Name());

    return true;
}

TString FormatColumnOrder(const TMaybe<TColumnOrder>& columnOrder) {
    TStringStream ss;
    if (columnOrder) {
        ss << "[" << JoinSeq(", ", *columnOrder) << "]";
    } else {
        ss << "default";
    }
    return ss.Str();
}

ui64 AddColumnOrderHash(const TMaybe<TColumnOrder>& columnOrder, ui64 hash) {
    if (!columnOrder) {
        return hash;
    }

    hash = CombineHashes(hash, NumericHash(columnOrder->size()));
    for (auto& col : *columnOrder) {
        hash = CombineHashes(hash, THash<TString>()(col));
    }

    return hash;
}


TMaybe<TColumnOrder> TTypeAnnotationContext::LookupColumnOrder(const TExprNode& node) const {
    return ColumnOrderStorage->Lookup(node.UniqueId());
}

IGraphTransformer::TStatus TTypeAnnotationContext::SetColumnOrder(const TExprNode& node,
    const TColumnOrder& columnOrder, TExprContext& ctx)
{
    if (!OrderedColumns) {
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_ENSURE(node.GetTypeAnn());
    YQL_ENSURE(node.IsCallable());

    if (auto existing = ColumnOrderStorage->Lookup(node.UniqueId())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Column order " << FormatColumnOrder(existing) << " is already set for node " << node.Content()));
        return IGraphTransformer::TStatus::Error;
    }

    auto nodeType = node.GetTypeAnn();
    // allow Tuple(world, sequence-of-struct)
    if (nodeType->GetKind() == ETypeAnnotationKind::Tuple) {
        if (!EnsureTupleTypeSize(node, 2, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto worldType = nodeType->Cast<TTupleExprType>()->GetItems()[0];
        if (worldType->GetKind() != ETypeAnnotationKind::World) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
                TStringBuilder() << "Expected world type as type of first tuple element, but got: " << *worldType));
            return IGraphTransformer::TStatus::Error;
        }

        nodeType = nodeType->Cast<TTupleExprType>()->GetItems()[1];
    }

    TSet<TStringBuf> allColumns = GetColumnsOfStructOrSequenceOfStruct(*nodeType);

    for (auto& col : columnOrder) {
        auto it = allColumns.find(col);
        if (it == allColumns.end()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
                TStringBuilder() << "Unable to set column order " << FormatColumnOrder(columnOrder) << " for node "
                                 << node.Content() << " with type: " << *node.GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }
        allColumns.erase(it);
    }

    if (!allColumns.empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Some columns are left unordered with column order " << FormatColumnOrder(columnOrder) << " for node "
                             << node.Content() << " with type: " << *node.GetTypeAnn()));
        return IGraphTransformer::TStatus::Error;
    }

    YQL_CLOG(DEBUG, Core) << "Setting column order " << FormatColumnOrder(columnOrder) << " for " << node.Content() << "#" << node.UniqueId();

    ColumnOrderStorage->Set(node.UniqueId(), columnOrder);
    return IGraphTransformer::TStatus::Ok;
}

const TCredential* TTypeAnnotationContext::FindCredential(const TStringBuf& name) const {
    for (auto& x : Credentials) {
        auto data = x->FindPtr(name);
        if (data) {
            return data;
        }
    }
    return nullptr;
}

TString TTypeAnnotationContext::FindCredentialContent(const TStringBuf& name1, const TStringBuf& name2, const TString& defaultContent) const {
    if (auto cred = FindCredential(name1)) {
        return cred->Content;
    }

    if (auto cred = FindCredential(name2)) {
        return cred->Content;
    }

    return defaultContent;
}

TString TTypeAnnotationContext::GetDefaultDataSource() const {
    if (!PureResultDataSource.empty()) {
        YQL_ENSURE(Find(AvailablePureResultDataSources.begin(),
            AvailablePureResultDataSources.end(), PureResultDataSource)
            != AvailablePureResultDataSources.end());
        return PureResultDataSource;
    }

    Y_ENSURE(!AvailablePureResultDataSources.empty());
    return AvailablePureResultDataSources.front();
}

bool SplitUdfName(TStringBuf name, TStringBuf& moduleName, TStringBuf& funcName) {
    moduleName = "";
    funcName = "";
    return name.TrySplit('.', moduleName, funcName);
}

TString TModuleResolver::NormalizeModuleName(const TString& path) {
    if (path.EndsWith(".sql") || path.EndsWith(".yql")) {
        return path.substr(0, path.size() - 4);
    }

    return path;
}

void TModuleResolver::RegisterPackage(const TString& package) {
    KnownPackages.insert(package);
}

bool TModuleResolver::SetPackageDefaultVersion(const TString& package, ui32 version) {
    if (!KnownPackages.contains(package)) {
        return false;
    }
    PackageVersions[package] = version;
    return true;
}

const TExportTable* TModuleResolver::GetModule(const TString& module) const {
    // ParentModules and Modules should not have common keys
    const TString normalizedModuleName = NormalizeModuleName(module);
    if (ParentModules) {
        if (auto table = ParentModules->FindPtr(normalizedModuleName)) {
            return table;
        }
    }

    return Modules.FindPtr(normalizedModuleName);
}

bool TModuleResolver::AddFromUrl(const TStringBuf& file, const TStringBuf& url, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion) {
    if (!UserData) {
        ctx.AddError(TIssue(TPosition(), "Loading libraries is prohibited"));
        return false;
    }

    TUserDataBlock block;
    block.Type = EUserDataType::URL;
    block.Data = url;
    block.Data = SubstParameters(block.Data);
    UserData->AddUserDataBlock(file, block);

    return AddFromFile(file, ctx, syntaxVersion, packageVersion);
}

bool TModuleResolver::AddFromFile(const TStringBuf& file, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion) {
    if (!UserData) {
        ctx.AddError(TIssue(TPosition(), "Loading libraries is prohibited"));
        return false;
    }

    const auto fullName = TUserDataStorage::MakeFullName(file);
    bool isSql = file.EndsWith(".sql");
    bool isYql = file.EndsWith(".yql");
    if (!isSql && !isYql) {
        ctx.AddError(TIssue(TStringBuilder() << "Unsupported syntax of library file, expected one of (.sql, .yql): " << file));
        return false;
    }

    const TUserDataBlock* block = UserData->FindUserDataBlock(fullName);

    if (!block) {
        ctx.AddError(TIssue(TStringBuilder() << "File not found: " << file));
        return false;
    }

    auto moduleName = TModuleResolver::NormalizeModuleName(TString(file));
    if (GetModule(moduleName) || Libs.contains(moduleName)) {
        auto it = Libs.find(moduleName);
        if (it != Libs.end() && it->second.contains(packageVersion)) {
            // TODO (YQL-7170): find better fix
            // ctx.AddError(TIssue({0,0,TString(fullName)}, TStringBuilder() << "File is already loaded as library"));
            return true;  // false
        }
    }

    TString body;
    switch (block->Type) {
    case EUserDataType::RAW_INLINE_DATA:
        body = block->Data;
        break;
    case EUserDataType::PATH:
        body = TFileInput(block->Data).ReadAll();
        break;
    case EUserDataType::URL:
        if (!UrlLoader) {
            ctx.AddError(TIssue(TStringBuilder() << "Unable to load file \"" << file
                << "\" from url, because url loader is not available"));
            return false;
        }

        body = UrlLoader->Load(block->Data, block->UrlToken);
        break;
    default:
        throw yexception() << "Unknown block type " << block->Type;
    }

    return AddFromMemory(fullName, moduleName, isYql, body, ctx, syntaxVersion, packageVersion);
}

bool TModuleResolver::AddFromMemory(const TStringBuf& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion) {
    TString unusedModuleName;
    return AddFromMemory(file, body, ctx, syntaxVersion, packageVersion, unusedModuleName);
}

bool TModuleResolver::AddFromMemory(const TStringBuf& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TString& moduleName, std::vector<TString>* exports, std::vector<TString>* imports) {
    const auto fullName = TUserDataStorage::MakeFullName(file);
    bool isSql = file.EndsWith(".sql");
    bool isYql = file.EndsWith(".yql");
    if (!isSql && !isYql) {
        ctx.AddError(TIssue(TStringBuilder() << "Unsupported syntax of library file, expected one of (.sql, .yql): " << file));
        return false;
    }

    moduleName = TModuleResolver::NormalizeModuleName(TString(file));
    if (GetModule(moduleName) || Libs.contains(moduleName)) {
        auto it = Libs.find(moduleName);
        if (it != Libs.end() && it->second.contains(packageVersion)) {
            // TODO (YQL-7170): find better fix
            // ctx.AddError(TIssue({0,0,TString(fullName)}, TStringBuilder() << "File is already loaded as library"));
            return true;  // false
        }
    }

    return AddFromMemory(fullName, moduleName, isYql, body, ctx, syntaxVersion, packageVersion, exports, imports);
}

bool TModuleResolver::AddFromMemory(const TString& fullName, const TString& moduleName, bool isYql, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, std::vector<TString>* exports, std::vector<TString>* imports) {
    TAstParseResult astRes;
    if (isYql) {
        astRes = ParseAst(body, nullptr, fullName);
        if (!astRes.IsOk()) {
            ctx.IssueManager.AddIssues(astRes.Issues);
            ctx.AddError(TIssue(TStringBuilder() << "Failed to parse YQL: " << fullName));
            return false;
        }
    } else {
        NSQLTranslation::TTranslationSettings settings;
        settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
        settings.File = fullName;
        settings.ClusterMapping = ClusterMapping;
        settings.Flags = SqlFlags;
        settings.SyntaxVersion = syntaxVersion;
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Silent;
        astRes = SqlToYql(body, settings);
        if (!astRes.IsOk()) {
            ctx.IssueManager.AddIssues(astRes.Issues);
            ctx.AddError(TIssue(TStringBuilder() << "Failed to parse SQL: " << fullName));
            return false;
        }
    }

    TLibraryCohesion cohesion;
    if (!CompileExpr(*astRes.Root, cohesion, LibsContext)) {
        ctx.IssueManager.AddIssues(LibsContext.IssueManager.GetIssues());
        ctx.AddError(TIssue(TStringBuilder() << "Failed to compile: " << fullName));
        return false;
    }

    if (OptimizeLibraries) {
        if (!OptimizeLibrary(cohesion, LibsContext)) {
            ctx.IssueManager.AddIssues(LibsContext.IssueManager.GetIssues());
            ctx.AddError(TIssue(TStringBuilder() << "Failed to optimize: " << fullName));
            return false;
        }
    }

    if (exports) {
        exports->clear();
        for (auto p : cohesion.Exports.Symbols()) {
            exports->push_back(p.first);
        }
    }

    if (imports) {
        imports->clear();
        for (auto p : cohesion.Imports) {
            imports->push_back(p.second.first);
        }
    }

    Libs[moduleName][packageVersion] = std::move(cohesion);
    return true;
}

bool TModuleResolver::Link(TExprContext& ctx) {
    std::function<const TExportTable*(const TString&)> f = [this](const TString& normalizedModuleName) -> const TExportTable* {
        return this->GetModule(normalizedModuleName);
    };

    THashMap<TString, TLibraryCohesion> libs = FilterLibsByVersion();
    if (!LinkLibraries(libs, ctx, LibsContext, f)) {
        return false;
    }

    for (auto& x : libs) {
        Modules.emplace(x.first, std::move(x.second.Exports));
    }

    Libs.clear();
    PackageVersions.clear();
    return true;
}

THashMap<TString, TLibraryCohesion> TModuleResolver::FilterLibsByVersion() const {
    THashMap<TString, TLibraryCohesion> result;
    for (auto p : Libs) {
        YQL_ENSURE(!p.second.empty());

        auto packageName = ExtractPackageNameFromModule(p.first);
        if (!packageName) {
            YQL_ENSURE(p.second.size() == 1);
            result.emplace(p.first, p.second.begin()->second);
            continue;
        }

        if (!KnownPackages.contains(packageName)) {
            ythrow yexception() << "Unknown package " << packageName << " is used in module " << p.first;
        }

        auto it = PackageVersions.find(packageName);
        const ui32 version = (it != PackageVersions.end()) ? it->second : 0;
        auto cohesionIt = p.second.find(version);
        if (cohesionIt == p.second.end()) {
            ythrow yexception() << "Unable to find library version " << version << " for package " << packageName << " and module " << p.first;
        }
        result.emplace(p.first, cohesionIt->second);
    }
    return result;
}

TString TModuleResolver::ExtractPackageNameFromModule(TStringBuf moduleName) {
    // naming convention: pkg.$code_project_name.$code_package_name.$module_name_within_package
    // module_name_within_package can contain dots
    // function returns $code_project_name.$code_package_name and we call it "package" at worker side
    TStringBuf pkg = moduleName.NextTok('/');
    if (pkg != "pkg") {
        return "";
    }

    TStringBuf project = moduleName.NextTok('/');
    TStringBuf package = moduleName.NextTok('/');
    if (package.empty()) {
        return "";
    }

    return TString(project) + "." + package;
}

void TModuleResolver::UpdateNextUniqueId(TExprContext& ctx) const {
    if (UserData && ctx.NextUniqueId < LibsContext.NextUniqueId) {
        ctx.NextUniqueId = LibsContext.NextUniqueId;
    }
}

ui64 TModuleResolver::GetNextUniqueId() const {
    return LibsContext.NextUniqueId;
}

IModuleResolver::TPtr TModuleResolver::CreateMutableChild() const {
    if (UserData || UrlLoader) {
        throw yexception() << "Module resolver should not contain user data and URL loader";
    }

    return std::make_shared<TModuleResolver>(&Modules, LibsContext.NextUniqueId, ClusterMapping, SqlFlags, OptimizeLibraries, KnownPackages, Libs);
}

TString TModuleResolver::SubstParameters(const TString& str) {
    if (!Parameters) {
        return str;
    }

    return ::NYql::SubstParameters(str, Parameters, nullptr);
}

} // namespace NYql
