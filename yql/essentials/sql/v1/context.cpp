#include "context.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/yql_paths.h>

#include <util/folder/pathsplit.h>
#include <util/string/join.h>
#include <util/stream/null.h>
#include <util/generic/scope.h>

#ifdef GetMessage
    #undef GetMessage
#endif

using namespace NYql;

namespace NSQLTranslationV1 {

namespace {

TNodePtr AddTablePathPrefix(TContext& ctx, TStringBuf prefixPath, const TDeferredAtom& path) {
    if (prefixPath.empty()) {
        return path.Build();
    }

    if (path.GetLiteral()) {
        return BuildQuotedAtom(path.Build()->GetPos(), BuildTablePath(prefixPath, *path.GetLiteral()));
    }

    auto pathNode = path.Build();
    pathNode = new TCallNodeImpl(pathNode->GetPos(), "String", {pathNode});
    auto prefixNode = BuildLiteralRawString(pathNode->GetPos(), TString(prefixPath));

    TNodePtr buildPathNode = new TCallNodeImpl(pathNode->GetPos(), "BuildTablePath", {prefixNode, pathNode});

    TDeferredAtom result;
    MakeTableFromExpression(ctx.Pos(), ctx, buildPathNode, result);
    return result.Build();
}

typedef bool TContext::*TPragmaField;

// TODO(vitya-smirnov): register thsese names automatically using TABLE_ELEM macro.
THashMap<TStringBuf, TPragmaField> CTX_PRAGMA_FIELDS = {
    {"AnsiOptionalAs", &TContext::AnsiOptionalAs},
    {"WarnOnAnsiAliasShadowing", &TContext::WarnOnAnsiAliasShadowing},
    {"PullUpFlatMapOverJoin", &TContext::PragmaPullUpFlatMapOverJoin},
    {"FilterPushdownOverJoinOptionalSide", &TContext::FilterPushdownOverJoinOptionalSide},
    {"RotateJoinTree", &TContext::RotateJoinTree},
    {"DqEngineEnable", &TContext::DqEngineEnable},
    {"DqEngineForce", &TContext::DqEngineForce},
    {"RegexUseRe2", &TContext::PragmaRegexUseRe2},
    {"OrderedColumns", &TContext::OrderedColumns},
    {"DeriveColumnOrder", &TContext::DeriveColumnOrder},
    {"BogousStarInGroupByOverJoin", &TContext::BogousStarInGroupByOverJoin},
    {"CoalesceJoinKeysOnQualifiedAll", &TContext::CoalesceJoinKeysOnQualifiedAll},
    {"UnorderedSubqueries", &TContext::UnorderedSubqueries},
    {"FlexibleTypes", &TContext::FlexibleTypes},
    {"AnsiCurrentRow", &TContext::AnsiCurrentRow},
    {"EmitStartsWith", &TContext::EmitStartsWith},
    {"AnsiLike", &TContext::AnsiLike},
    {"UseBlocks", &TContext::UseBlocks},
    {"EmitTableSource", &TContext::EmitTableSource},
    {"BlockEngineEnable", &TContext::BlockEngineEnable},
    {"BlockEngineForce", &TContext::BlockEngineForce},
    {"UnorderedResult", &TContext::UnorderedResult},
    {"CompactNamedExprs", &TContext::CompactNamedExprs},
    {"ValidateUnusedExprs", &TContext::ValidateUnusedExprs},
    {"AnsiImplicitCrossJoin", &TContext::AnsiImplicitCrossJoin},
    {"DistinctOverWindow", &TContext::DistinctOverWindow},
    {"EmitUnionMerge", &TContext::EmitUnionMerge},
    {"SeqMode", &TContext::SeqMode},
    {"DistinctOverKeys", &TContext::DistinctOverKeys},
    {"GroupByExprAfterWhere", &TContext::GroupByExprAfterWhere},
    {"FailOnGroupByExprOverride", &TContext::FailOnGroupByExprOverride},
    {"OptimizeSimpleILIKE", &TContext::OptimizeSimpleIlike},
    {"DebugPositions", &TContext::DebugPositions},
    {"ExceptIntersectBefore202503", &TContext::ExceptIntersectBefore202503},
    {"WindowNewPipeline", &TContext::WindowNewPipeline},
};

typedef TMaybe<bool> TContext::*TPragmaMaybeField;

THashMap<TStringBuf, TPragmaMaybeField> CTX_PRAGMA_MAYBE_FIELDS = {
    {"AnsiRankForNullableKeys", &TContext::AnsiRankForNullableKeys},
    {"AnsiInForEmptyOrNullableItemsCollections", &TContext::AnsiInForEmptyOrNullableItemsCollections},
    {"EmitAggApply", &TContext::EmitAggApply},
    {"CompactGroupBy", &TContext::CompactGroupBy},
    {"DirectRowDependsOn", &TContext::DirectRowDependsOn},
};

} // namespace

TContext::TContext(const TLexers& lexers, const TParsers& parsers,
                   const NSQLTranslation::TTranslationSettings& settings,
                   const NSQLTranslation::TSQLHints& hints,
                   TIssues& issues,
                   const TString& query)
    : Lexers(lexers)
    , Parsers(parsers)
    , ClusterMapping_(settings.ClusterMapping)
    , PathPrefix_(settings.PathPrefix)
    , ClusterPathPrefixes_(settings.ClusterPathPrefixes)
    , SqlHints_(hints)
    , Settings(settings)
    , Query(query)
    , Pool(new TMemoryPool(4096))
    , Issues(issues)
    , IncrementMonCounterFunction(settings.IncrementCounter)
    , HasPendingErrors(false)
    , DqEngineEnable(Settings.DqDefaultAuto->Allow())
    , AnsiQuotedIdentifiers(settings.AnsiLexer)
    , WarningPolicy(settings.IsReplay)
    , BlockEngineEnable(Settings.BlockDefaultAuto->Allow())
    , StrictWarningAsError(Settings.Flags.contains("StrictWarningAsError"))
{
    if (settings.LangVer >= MakeLangVersion(2025, 2)) {
        GroupByExprAfterWhere = true;
    }

    if (settings.LangVer >= MakeLangVersion(2025, 3)) {
        FlattenAndAggrExprsPersistence = EFlattenAndAggrExprsPersistence::Auto;
    }

    if (settings.LangVer >= MakeLangVersion(2025, 4)) {
        DisableLegacyNotNull = true;
    }

    if (settings.Flags.contains("AutoYqlSelect")) {
        SetYqlSelectMode(EYqlSelectMode::Auto);
    }

    for (auto lib : settings.Libraries) {
        Libraries.emplace(lib, TLibraryStuff());
    }

    Scoped = CreateScopedState();
    AllScopes.push_back(Scoped);

    Position_.File = settings.File;

    for (auto& flag : settings.Flags) {
        bool value = true;
        TStringBuf key = flag;
        auto ptr = CTX_PRAGMA_FIELDS.FindPtr(key);
        auto ptrMaybe = CTX_PRAGMA_MAYBE_FIELDS.FindPtr(key);
        if (!ptr && !ptrMaybe && key.SkipPrefix("Disable")) {
            value = false;
            ptr = CTX_PRAGMA_FIELDS.FindPtr(key);
            ptrMaybe = CTX_PRAGMA_MAYBE_FIELDS.FindPtr(key);
        }
        if (ptr) {
            this->*(*ptr) = value;
        } else if (ptrMaybe) {
            this->*(*ptrMaybe) = value;
        }
    }
    DiscoveryMode = (NSQLTranslation::ESqlMode::DISCOVERY == Settings.Mode);
}

TContext::~TContext()
{
    for (auto& x : AllScopes) {
        x->Clear();
    }
}

const NYql::TPosition& TContext::Pos() const {
    return Position_;
}

TString TContext::MakeName(const TString& name) {
    auto iter = GenIndexes.find(name);
    if (iter == GenIndexes.end()) {
        iter = GenIndexes.emplace(name, 0).first;
    }
    TStringBuilder str;
    str << name << iter->second;
    ++iter->second;
    return str;
}

void TContext::PushCurrentBlocks(TBlocks* blocks) {
    YQL_ENSURE(blocks);
    CurrentBlocks_.push_back(blocks);
}

void TContext::PopCurrentBlocks() {
    YQL_ENSURE(!CurrentBlocks_.empty());
    CurrentBlocks_.pop_back();
}

TBlocks& TContext::GetCurrentBlocks() const {
    YQL_ENSURE(!CurrentBlocks_.empty());
    return *CurrentBlocks_.back();
}

IOutputStream& TContext::Fatal() {
    bool isError = false;
    return MakeIssue(
        TSeverityIds::S_FATAL,
        NYql::TIssuesIds::UNEXPECTED,
        TPosition(),
        /*forceError=*/false,
        isError);
}

IOutputStream& TContext::Error(NYql::TIssueCode code) {
    return Error(Pos(), code);
}

IOutputStream& TContext::Error(NYql::TPosition pos, NYql::TIssueCode code) {
    HasPendingErrors = true;
    bool isError;
    return MakeIssue(TSeverityIds::S_ERROR, code, pos, false, isError);
}

bool TContext::Warning(NYql::TPosition pos, NYql::TIssueCode code, std::function<void(IOutputStream&)> message,
                       bool forceError) {
    bool isError;
    IOutputStream& out = MakeIssue(TSeverityIds::S_WARNING, code, pos, forceError, isError);
    message(out);
    return !StrictWarningAsError || !isError;
}

IOutputStream& TContext::Info(NYql::TPosition pos) {
    bool isError;
    return MakeIssue(TSeverityIds::S_INFO, TIssuesIds::INFO, pos, false, isError);
}

void TContext::SetWarningPolicyFor(NYql::TIssueCode code, NYql::EWarningAction action) {
    TString codePattern = ToString(code);
    TString actionString = ToString(action);

    TWarningRule rule;
    TString parseError;
    auto parseResult = TWarningRule::ParseFrom(codePattern, actionString, rule, parseError);
    YQL_ENSURE(parseResult == TWarningRule::EParseResult::PARSE_OK);
    WarningPolicy.AddRule(rule);
}

TVector<NSQLTranslation::TSQLHint> TContext::PullHintForToken(NYql::TPosition tokenPos) {
    TVector<NSQLTranslation::TSQLHint> result;
    auto it = SqlHints_.find(tokenPos);
    if (it == SqlHints_.end()) {
        return result;
    }
    result = std::move(it->second);
    SqlHints_.erase(it);
    return result;
}

bool TContext::WarnUnusedHints() {
    if (SqlHints_.empty()) {
        return true;
    }

    auto firstUnused = SqlHints_.begin();
    YQL_ENSURE(!firstUnused->second.empty());
    const NSQLTranslation::TSQLHint& hint = firstUnused->second.front();
    return Warning(hint.Pos, TIssuesIds::YQL_UNUSED_HINT, [&](auto& out) {
        out << "Hint " << hint.Name << " will not be used";
    });
}

IOutputStream& TContext::MakeIssue(ESeverity severity, TIssueCode code, NYql::TPosition pos,
                                   bool forceError, bool& isError) {
    isError = (severity == TSeverityIds::S_ERROR);

    if (severity == TSeverityIds::S_WARNING) {
        if (forceError) {
            severity = TSeverityIds::S_ERROR;
            HasPendingErrors = true;
            isError = true;
        } else {
            auto action = WarningPolicy.GetAction(code);
            if (action == EWarningAction::ERROR) {
                severity = TSeverityIds::S_ERROR;
                HasPendingErrors = true;
                isError = true;
            } else if (action == EWarningAction::DISABLE) {
                return Cnull;
            }
        }
    }

    // we have the last cell for issue, let's fill it with our internal error
    if (severity >= TSeverityIds::S_WARNING) {
        const bool aboveHalf = Issues.Size() > Settings.MaxErrors / 2;
        if (aboveHalf) {
            return Cnull;
        }
    } else {
        if (Settings.MaxErrors == Issues.Size() + 1) {
            Issues.AddIssue(TIssue(NYql::TPosition(), TString(TStringBuf("Too many issues"))));
            Issues.back().SetCode(UNEXPECTED_ERROR, TSeverityIds::S_ERROR);
        }

        if (Settings.MaxErrors <= Issues.Size()) {
            ythrow NAST::TTooManyErrors() << "Too many issues";
        }
    }

    Issues.AddIssue(TIssue(pos, TString()));
    auto& curIssue = Issues.back();
    curIssue.Severity = severity;
    curIssue.IssueCode = code;
    IssueMsgHolder_.Reset(new TStringOutput(*Issues.back().MutableMessage()));
    return *IssueMsgHolder_;
}

bool TContext::IsDynamicCluster(const TDeferredAtom& cluster) const {
    const TString* clusterPtr = cluster.GetLiteral();
    if (!clusterPtr) {
        return false;
    }
    TString unused;
    if (ClusterMapping_.GetClusterProvider(*clusterPtr, unused)) {
        return false;
    }
    if (Settings.AssumeYdbOnClusterWithSlash && clusterPtr->StartsWith('/')) {
        return false;
    }
    return !Settings.DynamicClusterProvider.empty();
}

bool TContext::SetPathPrefix(const TString& value, TMaybe<TString> arg) {
    if (arg.Defined()) {
        if (*arg == YtProviderName || *arg == KikimrProviderName || *arg == RtmrProviderName)
        {
            ProviderPathPrefixes_[*arg] = value;
            return true;
        }

        TString normalizedClusterName;
        if (!GetClusterProvider(*arg, normalizedClusterName)) {
            Error() << "Unknown cluster or provider: " << *arg;
            IncrementMonCounter("sql_errors", "BadPragmaValue");
            return false;
        }

        ClusterPathPrefixes_[normalizedClusterName] = value;
    } else {
        PathPrefix_ = value;
    }

    return true;
}

TNodePtr TContext::GetPrefixedPath(const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& path) {
    TStringBuf prefixPath = GetPrefixPath(service, cluster);
    if (prefixPath) {
        return AddTablePathPrefix(*this, prefixPath, path);
    }
    return path.Build();
}

TStringBuf TContext::GetPrefixPath(const TString& service, const TDeferredAtom& cluster) const {
    if (IsDynamicCluster(cluster)) {
        return {};
    }
    auto* clusterPrefix = cluster.GetLiteral()
                              ? ClusterPathPrefixes_.FindPtr(*cluster.GetLiteral())
                              : nullptr;
    if (clusterPrefix && !clusterPrefix->empty()) {
        return *clusterPrefix;
    } else {
        auto* providerPrefix = ProviderPathPrefixes_.FindPtr(service);
        if (providerPrefix && !providerPrefix->empty()) {
            return *providerPrefix;
        } else if (!PathPrefix_.empty()) {
            return PathPrefix_;
        }
        return {};
    }
}

TNodePtr TContext::UniversalAlias(const TString& baseName, TNodePtr&& node) {
    auto alias = MakeName(baseName);
    UniversalAliases.emplace(alias, node);
    return BuildAtom(node->GetPos(), alias, TNodeFlags::Default);
}

bool TContext::IsAlreadyDeclared(const TString& varName) const {
    return Variables.find(varName) != Variables.end() && !WeakVariables.contains(varName);
}

void TContext::DeclareVariable(const TString& varName, const TPosition& pos, const TNodePtr& typeNode, bool isWeak) {
    if (isWeak) {
        auto inserted = Variables.emplace(varName, std::make_pair(pos, typeNode));
        YQL_ENSURE(inserted.second);
        WeakVariables.insert(varName);
    } else {
        WeakVariables.erase(WeakVariables.find(varName));
        Variables[varName] = std::make_pair(pos, typeNode);
    }
}

bool TContext::AddExport(TPosition pos, const TString& name) {
    if (IsAnonymousName(name)) {
        Error(pos) << "Can not export anonymous name " << name;
        return false;
    }
    if (!Scoped->LookupNode(name)) {
        Error(pos) << "Unable to export unknown symbol: " << name;
        return false;
    }
    Exports.emplace(name);
    return true;
}

TString TContext::AddImport(const TVector<TString>& modulePath) {
    YQL_ENSURE(!modulePath.empty());
    TString path = JoinRange("/", modulePath.cbegin(), modulePath.cend());
    if (!path.StartsWith('/')) {
        path = Settings.FileAliasPrefix + path;
    }

    auto iter = ImportModuleAliases.find(path);
    if (iter == ImportModuleAliases.end()) {
        const TString alias = MakeName(TStringBuilder() << modulePath.back() << "_module");
        iter = ImportModuleAliases.emplace(path, alias).first;
    }
    return iter->second;
}

TString TContext::AddSimpleUdf(const TString& udf) {
    auto& name = SimpleUdfs[udf];
    if (name.empty()) {
        name = TStringBuilder() << "Udf" << SimpleUdfs.size();
    }

    return name;
}

void TContext::SetPackageVersion(const TString& packageName, ui32 version) {
    PackageVersions[packageName] = version;
}

void TScopedState::UseCluster(const TString& service, const TDeferredAtom& cluster) {
    YQL_ENSURE(!cluster.Empty());
    if (cluster.GetLiteral()) {
        if (!Local.UsedPlainClusters.insert(*cluster.GetLiteral()).second) {
            return;
        }
    } else {
        if (!Local.UsedExprClusters.insert(cluster.Build().Get()).second) {
            return;
        }
    }
    Local.UsedClusters.push_back({service, cluster});
}

void TScopedState::AddExprCluster(TNodePtr expr, TContext& ctx) {
    auto node = expr.Get();
    if (Local.ExprClustersMap.count(node)) {
        return;
    }
    auto name = ctx.MakeName("cluster");
    auto wrappedNode = expr->Y("EvaluateAtom", expr);
    Local.ExprClustersMap.insert({node, {name, wrappedNode}});
    Local.ExprClusters.push_back(expr);
}

const TVector<std::pair<TString, TDeferredAtom>>& TScopedState::GetUsedClusters() {
    return Local.UsedClusters;
}

TNodePtr TScopedState::WrapCluster(const TDeferredAtom& cluster, TContext& ctx) {
    auto node = cluster.Build();
    if (!cluster.GetLiteral()) {
        if (ctx.CompactNamedExprs) {
            return node->Y("EvaluateAtom", node);
        }
        AddExprCluster(node, ctx);
        auto exprIt = Local.ExprClustersMap.find(node.Get());
        YQL_ENSURE(exprIt != Local.ExprClustersMap.end());
        return node->AstNode(exprIt->second.first);
    }

    return node;
}

void TScopedState::Clear() {
    *this = TScopedState();
}

TNodePtr TScopedState::LookupNode(const TString& name) {
    auto mapIt = NamedNodes.find(name);
    if (mapIt == NamedNodes.end()) {
        return nullptr;
    }
    Y_DEBUG_ABORT_UNLESS(!mapIt->second.empty());
    mapIt->second.front()->IsUsed = true;
    return mapIt->second.front()->Node->Clone();
}

bool TContext::HasNonYtProvider(const ISource& source) const {
    TTableList tableList;
    source.GetInputTables(tableList);

    TSet<TString> clusters;
    for (auto& it : tableList) {
        if (it.Service != YtProviderName) {
            return true;
        }
    }

    for (auto& cl : Scoped->Local.UsedClusters) {
        if (cl.first != YtProviderName) {
            return true;
        }
    }

    return false;
}

bool TContext::UseUnordered(const ISource& source) const {
    return !HasNonYtProvider(source);
}

bool TContext::UseUnordered(const TTableRef& table) const {
    return YtProviderName == table.Service;
}

TScopedStatePtr TContext::CreateScopedState() const {
    auto state = MakeIntrusive<TScopedState>();
    state->UnicodeLiterals = Settings.UnicodeLiterals;

    if (Settings.DefaultCluster) {
        state->CurrCluster = TDeferredAtom({}, Settings.DefaultCluster);

        const auto provider = GetClusterProvider(Settings.DefaultCluster);
        YQL_ENSURE(provider);
        state->CurrService = *provider;
    }

    return state;
}

bool TContext::EnsureBackwardCompatibleFeatureAvailable(
    TPosition position,
    TStringBuf feature,
    NYql::TLangVersion version)
{
    if (!IsBackwardCompatibleFeatureAvailable(version)) {
        Error(position)
            << feature << " is not available before language version "
            << NYql::FormatLangVersion(version);
        return false;
    }

    return true;
}

bool TContext::IsBackwardCompatibleFeatureAvailable(NYql::TLangVersion featureVer) const {
    return NYql::IsBackwardCompatibleFeatureAvailable(
        Settings.LangVer, featureVer, Settings.BackportMode);
}

TMaybe<EColumnRefState> GetFunctionArgColumnStatus(TContext& ctx, const TString& module, const TString& func, size_t argIndex) {
    static const TSet<TStringBuf> DenyForAllArgs = {
        "datatype",
        "optionaltype",
        "listtype",
        "streamtype",
        "dicttype",
        "tupletype",
        "resourcetype",
        "taggedtype",
        "varianttype",
        "callabletype",
        "optionalitemtype",
        "listitemtype",
        "streamitemtype",
        "dictkeytype",
        "dictpayloadtype",
        "tupleelementtype",
        "structmembertype",
        "callableresulttype",
        "callableargumenttype",
        "variantunderlyingtype",
    };
    static const TMap<std::pair<TStringBuf, size_t>, EColumnRefState> PositionalArgsCustomStatus = {
        {{"frombytes", 1}, EColumnRefState::Deny},
        {{"enum", 0}, EColumnRefState::Deny},
        {{"asenum", 0}, EColumnRefState::Deny},
        {{"variant", 1}, EColumnRefState::Deny},
        {{"variant", 2}, EColumnRefState::Deny},
        {{"asvariant", 1}, EColumnRefState::Deny},
        {{"astagged", 1}, EColumnRefState::Deny},
        {{"ensuretype", 1}, EColumnRefState::Deny},
        {{"ensuretype", 2}, EColumnRefState::Deny},
        {{"ensureconvertibleto", 1}, EColumnRefState::Deny},
        {{"ensureconvertibleto", 2}, EColumnRefState::Deny},

        {{"nothing", 0}, EColumnRefState::Deny},
        {{"formattype", 0}, EColumnRefState::Deny},
        {{"instanceof", 0}, EColumnRefState::Deny},
        {{"pgtype", 0}, EColumnRefState::AsPgType},
        {{"pgconst", 0}, EColumnRefState::Deny},
        {{"pgconst", 1}, EColumnRefState::AsPgType},
        {{"pgcast", 1}, EColumnRefState::AsPgType},

        {{"unpickle", 0}, EColumnRefState::Deny},
        {{"typehandle", 0}, EColumnRefState::Deny},

        {{"listcreate", 0}, EColumnRefState::Deny},
        {{"setcreate", 0}, EColumnRefState::Deny},
        {{"dictcreate", 0}, EColumnRefState::Deny},
        {{"dictcreate", 1}, EColumnRefState::Deny},
        {{"weakfield", 1}, EColumnRefState::Deny},

        {{"Yson::ConvertTo", 1}, EColumnRefState::Deny},
    };

    TString normalized;
    if (module.empty()) {
        normalized = to_lower(func);
    } else if (to_upper(module) == "YQL") {
        normalized = "YQL::" + func;
    } else {
        normalized = module + "::" + func;
    }

    if (normalized == "typeof" && argIndex == 0) {
        // TODO: more such cases?
        return ctx.GetTopLevelColumnReferenceState();
    }

    if (DenyForAllArgs.contains(normalized)) {
        return EColumnRefState::Deny;
    }

    auto it = PositionalArgsCustomStatus.find(std::make_pair(normalized, argIndex));
    if (it != PositionalArgsCustomStatus.end()) {
        return it->second;
    }
    return {};
}

TTranslation::TTranslation(TContext& ctx)
    : Ctx_(ctx)
{
}

TContext& TTranslation::Context() {
    return Ctx_;
}

IOutputStream& TTranslation::Error() {
    return Ctx_.Error();
}

TNodePtr TTranslation::GetNamedNode(const TString& name) {
    if (name == "$_") {
        Ctx_.Error() << "Unable to reference anonymous name " << name;
        return nullptr;
    }
    auto res = Ctx_.Scoped->LookupNode(name);
    if (!res) {
        Ctx_.Error() << "Unknown name: " << name;
    }
    return SafeClone(res);
}

TString TTranslation::PushNamedNode(TPosition namePos, const TString& name, const TNodeBuilderByName& builder) {
    TString resultName = name;
    if (IsAnonymousName(name)) {
        resultName = "$_yql_anonymous_name_" + ToString(Ctx_.AnonymousNameIndex++);
        YQL_ENSURE(Ctx_.Scoped->NamedNodes.find(resultName) == Ctx_.Scoped->NamedNodes.end());
    }
    auto node = builder(resultName);
    Y_DEBUG_ABORT_UNLESS(node);
    auto mapIt = Ctx_.Scoped->NamedNodes.find(resultName);
    if (mapIt == Ctx_.Scoped->NamedNodes.end()) {
        auto result = Ctx_.Scoped->NamedNodes.insert(std::make_pair(resultName, TDeque<TNodeWithUsageInfoPtr>()));
        Y_DEBUG_ABORT_UNLESS(result.second);
        mapIt = result.first;
    }

    mapIt->second.push_front(MakeIntrusive<TNodeWithUsageInfo>(node, namePos, Ctx_.ScopeLevel));
    return resultName;
}

TString TTranslation::PushNamedNode(NYql::TPosition namePos, const TString& name, NSQLTranslationV1::TNodePtr node) {
    return PushNamedNode(namePos, name, [node](const TString&) { return node; });
}

TString TTranslation::PushNamedAtom(TPosition namePos, const TString& name) {
    auto buildAtom = [namePos](const TString& resultName) {
        return BuildAtom(namePos, resultName);
    };
    return PushNamedNode(namePos, name, buildAtom);
}

bool TTranslation::PopNamedNode(const TString& name) {
    auto mapIt = Ctx_.Scoped->NamedNodes.find(name);
    Y_DEBUG_ABORT_UNLESS(mapIt != Ctx_.Scoped->NamedNodes.end());
    Y_DEBUG_ABORT_UNLESS(mapIt->second.size() > 0);

    Y_DEFER {
        mapIt->second.pop_front();
        if (mapIt->second.empty()) {
            Ctx_.Scoped->NamedNodes.erase(mapIt);
        }
    };

    auto& top = mapIt->second.front();
    if (!top->IsUsed && !Ctx_.HasPendingErrors && !name.StartsWith("$_")) {
        if (!Ctx_.Warning(top->NamePos, TIssuesIds::YQL_UNUSED_SYMBOL, [&](auto& out) {
                out << "Symbol " << name << " is not used";
            })) {
            return false;
        }
    }
    return true;
}

bool TTranslation::WarnUnusedNodes() const {
    if (Ctx_.HasPendingErrors) {
        // result is not reliable in this case
        return true;
    }

    for (const auto& [name, items] : Ctx_.Scoped->NamedNodes) {
        if (name.StartsWith("$_")) {
            continue;
        }

        for (const auto& item : items) {
            if (!item->IsUsed && item->Level == Ctx_.ScopeLevel) {
                if (!Ctx_.Warning(item->NamePos, TIssuesIds::YQL_UNUSED_SYMBOL, [&](auto& out) {
                        out << "Symbol " << name << " is not used";
                    })) {
                    return false;
                }
            }
        }
    }

    return true;
}

TString GetDescription(const google::protobuf::Message& node, const google::protobuf::FieldDescriptor* d) {
    const auto& field = node.GetReflection()->GetMessage(node, d);
    return field.GetReflection()->GetString(field, d->message_type()->FindFieldByName("Descr"));
}

TString TTranslation::AltDescription(const google::protobuf::Message& node, ui32 altCase, const google::protobuf::Descriptor* descr) const {
    return GetDescription(node, descr->FindFieldByNumber(altCase));
}

void TTranslation::AltNotImplemented(const TString& ruleName, ui32 altCase, const google::protobuf::Message& node, const google::protobuf::Descriptor* descr) {
    Error() << ruleName << ": alternative is not implemented yet: " << AltDescription(node, altCase, descr);
}

void EnumerateSqlFlags(std::function<void(std::string_view)> callback) {
    for (const auto& x : CTX_PRAGMA_FIELDS) {
        callback(x.first);
        callback(TString("Disable") + x.first);
    }

    for (const auto& x : CTX_PRAGMA_MAYBE_FIELDS) {
        callback(x.first);
        callback(TString("Disable") + x.first);
    }
}

} // namespace NSQLTranslationV1
