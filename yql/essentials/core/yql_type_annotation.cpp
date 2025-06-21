#include "yql_type_annotation.h"

#include "yql_expr_type_annotation.h"
#include "yql_library_compiler.h"
#include "yql_type_helpers.h"

#include <yql/essentials/ast/yql_constraint.h>
#include <yql/essentials/utils/log/log.h>

#include <util/stream/file.h>
#include <util/string/join.h>

namespace NYql {

using namespace NKikimr;

const TString ModuleResolverComponent = "ModuleResolver";

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
    UserDataStorage->FillUserDataUrls();


    return true;
}

void TTypeAnnotationContext::Reset() {
    UdfImports.clear();
    UdfModules.clear();
    UdfTypeCache.clear();
    NodeToOperationId.clear();
    EvaluationInProgress = 0;
    ExpectedTypes.clear();
    ExpectedConstraints.clear();
    ExpectedColumnOrders.clear();
    StatisticsMap.clear();
    NoBlockRewriteCallableStats.clear();
    NoBlockRewriteTypeStats.clear();
}

void TTypeAnnotationContext::IncNoBlockCallable(TStringBuf callableName) {
    ++NoBlockRewriteCallableStats[callableName];
}

void TTypeAnnotationContext::IncNoBlockType(const TTypeAnnotationNode& type) {
    if (type.GetKind() == ETypeAnnotationKind::Data) {
        IncNoBlockType(type.Cast<TDataExprType>()->GetSlot());
    } else {
        IncNoBlockType(type.GetKind());
    }
}

void TTypeAnnotationContext::IncNoBlockType(ETypeAnnotationKind kind) {
    ++NoBlockRewriteTypeStats[ToString(kind)];
}

void TTypeAnnotationContext::IncNoBlockType(NUdf::EDataSlot slot) {
    ++NoBlockRewriteTypeStats[ToString(slot)];
}

namespace {

template<typename T>
TVector<T> GetMaxByCount(const THashMap<T, size_t>& stats, size_t maxCount) {
    TVector<T> result;
    result.reserve(stats.size());
    for (auto& [key, _] : stats) {
        result.push_back(key);
    }
    size_t n = std::min(maxCount, stats.size());
    std::partial_sort(result.begin(), result.begin() + n, result.end(),
        [&stats](const T& l, const T& r) {
            const auto& cntLeft = stats.find(l)->second;
            const auto& cntRight = stats.find(r)->second;
            if (cntLeft != cntRight) {
                return cntLeft < cntRight;
            }
            return l < r;
        });
    result.resize(n);
    return result;
}

}

TVector<TString> TTypeAnnotationContext::GetTopNoBlocksCallables(size_t maxCount) const {
    return GetMaxByCount(NoBlockRewriteCallableStats, maxCount);
}

TVector<TString> TTypeAnnotationContext::GetTopNoBlocksTypes(size_t maxCount) const {
    return GetMaxByCount(NoBlockRewriteTypeStats, maxCount);
}

TString TColumnOrder::Find(const TString& name) const {
    auto it = GeneratedToOriginal_.find(name);
    if (it == GeneratedToOriginal_.end()) {
        return name;
    }
    return it->second;
}

TColumnOrder& TColumnOrder::operator=(const TColumnOrder& rhs) {
    GeneratedToOriginal_ = rhs.GeneratedToOriginal_;
    Order_ = rhs.Order_;
    UseCountLcase_ = rhs.UseCountLcase_;
    UseCount_ = rhs.UseCount_;
    return *this;
}

TColumnOrder::TColumnOrder(const TVector<TString>& order) {
    Reserve(order.size());
    for (auto& e: order) {
        AddColumn(e);
    }
}

TString TColumnOrder::AddColumn(const TString& name) {
    auto lcase = to_lower(name);
    ++UseCountLcase_[lcase];
    if (uint64_t count = ++UseCount_[name]; count > 1) {
        TString generated = name + "_generated_" + ToString(count);
        GeneratedToOriginal_[generated] = name;
        Order_.emplace_back(name, generated);
        ++UseCount_[generated];
        return generated;
    }
    Order_.emplace_back(name, name);
    GeneratedToOriginal_[name] = name;
    return name;
}

bool TColumnOrder::IsDuplicatedIgnoreCase(const TString& name) const {
    auto it = UseCountLcase_.find(to_lower(name));
    return it != UseCountLcase_.end() && it->second > 1;
}

void TColumnOrder::Shrink(size_t remain) {
    for (size_t i = remain; i < Order_.size(); ++i) {
        --UseCountLcase_[to_lower(Order_[i].LogicalName)];
        if (!--UseCount_[Order_[i].LogicalName]) {
            UseCount_.erase(Order_[i].LogicalName);
        }
        if (!--UseCount_[Order_[i].PhysicalName]) {
            UseCount_.erase(Order_[i].PhysicalName);
        }
        GeneratedToOriginal_.erase(Order_[i].PhysicalName);
    }
    while (Order_.size() > remain) {
        Order_.pop_back(); // No default ctor, can't call .resize()
    }
}

void TColumnOrder::Reserve(size_t count) {
    Order_.reserve(count);
}

void TColumnOrder::Clear() {
    Order_.clear();
    GeneratedToOriginal_.clear();
    UseCount_.clear();
    UseCountLcase_.clear();
}

void TColumnOrder::EraseIf(const std::function<bool(const TString&)>& fn) {
    TColumnOrder newOrder;
    for (const auto& e: Order_) {
        if (!fn(e.LogicalName)) {
            newOrder.AddColumn(e.LogicalName);
        }
    }
    std::swap(*this, newOrder);
}

void TColumnOrder::EraseIf(const std::function<bool(const TOrderedItem&)>& fn) {
    TColumnOrder newOrder;
    for (const auto& e: Order_) {
        if (!fn(e)) {
            newOrder.AddColumn(e.LogicalName);
        }
    }
    std::swap(*this, newOrder);
}

size_t TColumnOrder::Size() const {
    return Order_.size();
}

TString FormatColumnOrder(const TMaybe<TColumnOrder>& columnOrder, TMaybe<size_t> maxColumns) {
    TStringStream ss;
    if (columnOrder) {
        if (maxColumns.Defined() && columnOrder->Size() > *maxColumns) {
            size_t i = 0;
            ss << "[";
            for (auto& [e, gen_e]: *columnOrder) {
                if (i++ >= *maxColumns) {
                    break;
                }
                ss << "(" << e << "->" << gen_e << ")";
                if (++i != columnOrder->Size()) {
                    ss << ", ";
                }
            }
            ss << ", ... ]";
        } else {
            ss << "[";
            size_t i = 0;
            for (auto& [e, gen_e]: *columnOrder) {

                ss << "(" << e << "->" << gen_e << ")";
                if (++i != columnOrder->Size()) {
                    ss << ", ";
                }
            }
            ss << "]";
        }
    } else {
        ss << "default";
    }
    return ss.Str();
}

ui64 AddColumnOrderHash(const TMaybe<TColumnOrder>& columnOrder, ui64 hash) {
    if (!columnOrder) {
        return hash;
    }

    hash = CombineHashes(hash, NumericHash(columnOrder->Size()));
    for (auto& [col, gen_col] : *columnOrder) {
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
    if (!DeriveColumnOrder) {
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

    for (auto& [col, gen_col] : columnOrder) {
        auto it = allColumns.find(gen_col);
        if (it == allColumns.end()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
                TStringBuilder() << "Unable to set column order " << FormatColumnOrder(columnOrder) << " for node "
                                 << node.Content() << " with type: " << *node.GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }
        allColumns.erase(it);
    }

    if (!allColumns.empty() && !(allColumns.size() == 1 && *allColumns.begin() == BlockLengthColumnName)) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Some columns are left unordered with column order " << FormatColumnOrder(columnOrder) << " for node "
                             << node.Content() << " with type: " << *node.GetTypeAnn()));
        return IGraphTransformer::TStatus::Error;
    }

    YQL_CLOG(TRACE, Core) << "Setting column order " << FormatColumnOrder(columnOrder, 10) << " for " << node.Content() << "#" << node.UniqueId();

    ColumnOrderStorage->Set(node.UniqueId(), columnOrder);
    return IGraphTransformer::TStatus::Ok;
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

    if (path.EndsWith(".yqls")) {
        return path.substr(0, path.size() - 5);
    }

    return path;
}

void TModuleResolver::RegisterPackage(const TString& package) {
    KnownPackages_.insert(package);
}

bool TModuleResolver::SetPackageDefaultVersion(const TString& package, ui32 version) {
    if (!KnownPackages_.contains(package)) {
        return false;
    }
    PackageVersions_[package] = version;
    return true;
}

const TExportTable* TModuleResolver::GetModule(const TString& module) const {
    // ParentModules and Modules should not have common keys
    const TString normalizedModuleName = NormalizeModuleName(module);
    if (ParentModules_) {
        if (auto table = ParentModules_->FindPtr(normalizedModuleName)) {
            return table;
        }
    }

    return Modules_.FindPtr(normalizedModuleName);
}

void TModuleResolver::WriteStatistics(NYson::TYsonWriter& writer) {
    writer.OnKeyedItem("UsedSuffixes");
    writer.OnStringScalar(JoinRange(",", UsedSuffixes_.begin(), UsedSuffixes_.end()));
}

bool TModuleResolver::AddFromUrl(const std::string_view& file, const std::string_view& url, const std::string_view& tokenName, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) {
    if (!UserData_) {
        ctx.AddError(TIssue(pos, "Loading libraries is prohibited"));
        return false;
    }

    TUserDataBlock block;
    block.Type = EUserDataType::URL;
    block.Data = url;
    block.Data = SubstParameters(block.Data);
    if (!tokenName.empty()) {
        if (!Credentials_) {
            ctx.AddError(TIssue(pos, "Missing credentials"));
            return false;
        }
        auto cred = Credentials_->FindCredential(tokenName);
        if (!cred) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown token name: " << tokenName));
            return false;
        }
        block.UrlToken = cred->Content;
    }

    UserData_->AddUserDataBlock(file, block);

    return AddFromFile(file, ctx, syntaxVersion, packageVersion, pos);
}

bool TModuleResolver::AddFromFile(const std::string_view& file, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) {
    if (!UserData_) {
        ctx.AddError(TIssue(pos, "Loading libraries is prohibited"));
        return false;
    }

    const auto fullName = TUserDataStorage::MakeFullName(file);
    const bool isSql = file.ends_with(".sql");
    const bool isYql = file.ends_with(".yql");
    const bool isYqls = file.ends_with(".yqls");
    if (!isSql && !isYql && !isYqls) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Unsupported syntax of library file, expected one of (.sql, .yql, .yqls): " << file));
        return false;
    }

    UsedSuffixes_.insert(TString(file.substr(1 + file.rfind('.'))));
    const TUserDataBlock* block = UserData_->FindUserDataBlock(fullName);

    if (!block) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "File not found: " << file));
        return false;
    }

    auto moduleName = TModuleResolver::NormalizeModuleName(TString(file));
    if (GetModule(moduleName) || Libs_.contains(moduleName)) {
        auto it = Libs_.find(moduleName);
        if (it != Libs_.end() && it->second.contains(packageVersion)) {
            // TODO (YQL-7170): find better fix
            // ctx.AddError(TIssue({0,0,TString(fullName)}, TStringBuilder() << "File is already loaded as library"));
            return true;  // false
        }
    }

    TString body;
    if (!QContext_.CanRead()) {
        switch (block->Type) {
        case EUserDataType::RAW_INLINE_DATA:
            body = block->Data;
            break;
        case EUserDataType::PATH:
            body = TFileInput(block->Data).ReadAll();
            break;
        case EUserDataType::URL:
            if (!UrlLoader_) {
                ctx.AddError(TIssue(pos, TStringBuilder() << "Unable to load file \"" << file
                    << "\" from url, because url loader is not available"));
                return false;
            }

            body = UrlLoader_->Load(block->Data, block->UrlToken);
            break;
        default:
            throw yexception() << "Unknown block type " << block->Type;
        }
    }

    return AddFromMemory(fullName, moduleName, isYql || isYqls, body, ctx, syntaxVersion, packageVersion, pos);
}

bool TModuleResolver::AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos) {
    TString unusedModuleName;
    return AddFromMemory(file, body, ctx, syntaxVersion, packageVersion, pos, unusedModuleName);
}

bool TModuleResolver::AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, TString& moduleName, std::vector<TString>* exports, std::vector<TString>* imports) {
    const auto fullName = TUserDataStorage::MakeFullName(file);
    const bool isSql = file.ends_with(".sql");
    const bool isYql = file.ends_with(".yql");
    const bool isYqls = file.ends_with(".yqls");
    if (!isSql && !isYql && !isYqls) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Unsupported syntax of library file, expected one of (.sql, .yql, .yqls): " << file));
        return false;
    }

    UsedSuffixes_.insert(TString(file.substr(1 + file.rfind('.'))));
    moduleName = TModuleResolver::NormalizeModuleName(TString(file));
    if (GetModule(moduleName) || Libs_.contains(moduleName)) {
        auto it = Libs_.find(moduleName);
        if (it != Libs_.end() && it->second.contains(packageVersion)) {
            // TODO (YQL-7170): find better fix
            // ctx.AddError(TIssue({0,0,TString(fullName)}, TStringBuilder() << "File is already loaded as library"));
            return true;  // false
        }
    }

    return AddFromMemory(fullName, moduleName, isYql || isYqls, body, ctx, syntaxVersion, packageVersion, pos, exports, imports);
}

bool TModuleResolver::AddFromMemory(const TString& fullName, const TString& moduleName, bool sExpr, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, std::vector<TString>* exports, std::vector<TString>* imports) {
    auto query = body;
    if (QContext_.CanRead()) {
        auto item = QContext_.GetReader()->Get({ModuleResolverComponent, fullName}).GetValueSync();
        if (!item) {
            throw yexception() << "Missing replay data";
        }

        query = item->Value;
    } else if (QContext_.CanWrite()) {
        QContext_.GetWriter()->Put({ModuleResolverComponent, fullName}, query).GetValueSync();
    }

    const auto addSubIssues = [](TIssue&& issue, const TIssues& issues) {
        std::for_each(issues.begin(), issues.end(), [&](const TIssue& i) {
            issue.AddSubIssue(MakeIntrusive<TIssue>(i));
        });
        return std::move(issue);
    };

    TAstParseResult astRes;
    if (sExpr) {
        astRes = ParseAst(query, nullptr, fullName);
    } else {
        NSQLTranslation::TTranslationSettings settings;
        settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
        settings.File = fullName;
        settings.ClusterMapping = ClusterMapping_;
        settings.Flags = SqlFlags_;
        settings.SyntaxVersion = syntaxVersion;
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Silent;
        settings.FileAliasPrefix = FileAliasPrefix_;
        astRes = SqlToYql(Translators_, query, settings);
    }

    if (!astRes.IsOk()) {
        ctx.AddError(addSubIssues(TIssue(pos, TStringBuilder() << "Failed to parse: " << fullName), astRes.Issues));
        return false;
    }

    if (!astRes.Issues.Empty()) {
        auto issue = TIssue(pos, TStringBuilder() << "Parsing issues for: " << fullName);
        issue.SetCode(TIssuesIds::INFO, NYql::TSeverityIds::S_INFO);
        ctx.IssueManager.RaiseIssue(addSubIssues(std::move(issue), astRes.Issues));
    }

    if (!sExpr && ModuleChecker_) {
        if (!ModuleChecker_(query, fullName, ctx)) {
            return false;
        }
    }

    TLibraryCohesion cohesion;
    if (!CompileExpr(*astRes.Root, cohesion, LibsContext_)) {
        ctx.AddError(addSubIssues(TIssue(pos, TStringBuilder() << "Failed to compile: " << fullName), LibsContext_.IssueManager.GetIssues()));
        return false;
    }

    if (OptimizeLibraries_) {
        if (!OptimizeLibrary(cohesion, LibsContext_)) {
            ctx.AddError(addSubIssues(TIssue(pos, TStringBuilder() << "Failed to optimize: " << fullName), LibsContext_.IssueManager.GetIssues()));
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

    Libs_[moduleName][packageVersion] = std::move(cohesion);
    return true;
}

bool TModuleResolver::Link(TExprContext& ctx) {
    std::function<const TExportTable*(const TString&)> f = [this](const TString& normalizedModuleName) -> const TExportTable* {
        return this->GetModule(normalizedModuleName);
    };

    THashMap<TString, TLibraryCohesion> libs = FilterLibsByVersion();
    if (!LinkLibraries(libs, ctx, LibsContext_, f)) {
        return false;
    }

    for (auto& x : libs) {
        Modules_.emplace(x.first, std::move(x.second.Exports));
    }

    Libs_.clear();
    PackageVersions_.clear();
    return true;
}

THashMap<TString, TLibraryCohesion> TModuleResolver::FilterLibsByVersion() const {
    THashMap<TString, TLibraryCohesion> result;
    for (auto p : Libs_) {
        YQL_ENSURE(!p.second.empty());

        auto packageName = ExtractPackageNameFromModule(p.first);
        if (!packageName) {
            YQL_ENSURE(p.second.size() == 1);
            result.emplace(p.first, p.second.begin()->second);
            continue;
        }

        if (!KnownPackages_.contains(packageName)) {
            ythrow yexception() << "Unknown package " << packageName << " is used in module " << p.first;
        }

        auto it = PackageVersions_.find(packageName);
        const ui32 version = (it != PackageVersions_.end()) ? it->second : 0;
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
    if (UserData_ && ctx.NextUniqueId < LibsContext_.NextUniqueId) {
        ctx.NextUniqueId = LibsContext_.NextUniqueId;
    }
}

ui64 TModuleResolver::GetNextUniqueId() const {
    return LibsContext_.NextUniqueId;
}

IModuleResolver::TPtr TModuleResolver::CreateMutableChild() const {
    if (UserData_ || UrlLoader_) {
        throw yexception() << "Module resolver should not contain user data and URL loader";
    }

    return std::make_shared<TModuleResolver>(Translators_, &Modules_, LibsContext_.NextUniqueId, ClusterMapping_, SqlFlags_, OptimizeLibraries_, KnownPackages_, Libs_, FileAliasPrefix_, ModuleChecker_);
}

void TModuleResolver::SetFileAliasPrefix(TString&& prefix) {
    FileAliasPrefix_ = std::move(prefix);
}

TString TModuleResolver::GetFileAliasPrefix() const {
    return FileAliasPrefix_;
}

TString TModuleResolver::SubstParameters(const TString& str) {
    if (!Parameters_) {
        return str;
    }

    return ::NYql::SubstParameters(str, Parameters_, nullptr);
}

} // namespace NYql
