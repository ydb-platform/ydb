#include "context.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/yql_paths.h>

#include <util/folder/pathsplit.h>
#include <util/string/join.h>
#include <util/stream/null.h>

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
    pathNode = new TCallNodeImpl(pathNode->GetPos(), "String", { pathNode });
    auto prefixNode = BuildLiteralRawString(pathNode->GetPos(), TString(prefixPath));

    TNodePtr buildPathNode = new TCallNodeImpl(pathNode->GetPos(), "BuildTablePath", { prefixNode, pathNode });

    TDeferredAtom result;
    MakeTableFromExpression(ctx.Pos(), ctx, buildPathNode, result);
    return result.Build();
}

typedef bool TContext::*TPragmaField;

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
    {"BogousStarInGroupByOverJoin", &TContext::BogousStarInGroupByOverJoin},
    {"CoalesceJoinKeysOnQualifiedAll", &TContext::CoalesceJoinKeysOnQualifiedAll},
    {"UnorderedSubqueries", &TContext::UnorderedSubqueries},
    {"FlexibleTypes", &TContext::FlexibleTypes},
    {"AnsiCurrentRow", &TContext::AnsiCurrentRow},
    {"EmitStartsWith", &TContext::EmitStartsWith},
    {"AnsiLike", &TContext::AnsiLike},
    {"UseBlocks", &TContext::UseBlocks},
    {"BlockEngineEnable", &TContext::BlockEngineEnable},
    {"BlockEngineForce", &TContext::BlockEngineForce},
    {"UnorderedResult", &TContext::UnorderedResult},
    {"CompactNamedExprs", &TContext::CompactNamedExprs},
    {"ValidateUnusedExprs", &TContext::ValidateUnusedExprs},
    {"AnsiImplicitCrossJoin", &TContext::AnsiImplicitCrossJoin},
};

typedef TMaybe<bool> TContext::*TPragmaMaybeField;

THashMap<TStringBuf, TPragmaMaybeField> CTX_PRAGMA_MAYBE_FIELDS = {
    {"AnsiRankForNullableKeys", &TContext::AnsiRankForNullableKeys},
    {"AnsiInForEmptyOrNullableItemsCollections", &TContext::AnsiInForEmptyOrNullableItemsCollections},
    {"EmitAggApply", &TContext::EmitAggApply},
    {"CompactGroupBy", &TContext::CompactGroupBy},
};

} // namespace

TContext::TContext(const NSQLTranslation::TTranslationSettings& settings,
                   const NSQLTranslation::TSQLHints& hints,
                   TIssues& issues)
    : ClusterMapping(settings.ClusterMapping)
    , PathPrefix(settings.PathPrefix)
    , ClusterPathPrefixes(settings.ClusterPathPrefixes)
    , SQLHints(hints)
    , Settings(settings)
    , Pool(new TMemoryPool(4096))
    , Issues(issues)
    , IncrementMonCounterFunction(settings.IncrementCounter)
    , HasPendingErrors(false)
    , DqEngineEnable(Settings.DqDefaultAuto->Allow())
    , AnsiQuotedIdentifiers(settings.AnsiLexer)
    , BlockEngineEnable(Settings.BlockDefaultAuto->Allow())
{
    for (auto lib : settings.Libraries) {
        Libraries.emplace(lib, TLibraryStuff());
    }

    Scoped = MakeIntrusive<TScopedState>();
    AllScopes.push_back(Scoped);
    Scoped->UnicodeLiterals = settings.UnicodeLiterals;
    if (settings.DefaultCluster) {
        Scoped->CurrCluster = TDeferredAtom({}, settings.DefaultCluster);
        auto provider = GetClusterProvider(settings.DefaultCluster);
        YQL_ENSURE(provider);
        Scoped->CurrService = *provider;
    }

    Position.File = settings.File;

    for (auto& flag: settings.Flags) {
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
    for (auto& x: AllScopes) {
        x->Clear();
    }
}

const NYql::TPosition& TContext::Pos() const {
    return Position;
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
    CurrentBlocks.push_back(blocks);
}

void TContext::PopCurrentBlocks() {
    YQL_ENSURE(!CurrentBlocks.empty());
    CurrentBlocks.pop_back();
}

TBlocks& TContext::GetCurrentBlocks() const {
    YQL_ENSURE(!CurrentBlocks.empty());
    return *CurrentBlocks.back();
}

IOutputStream& TContext::Error(NYql::TIssueCode code) {
    return Error(Pos(), code);
}

IOutputStream& TContext::Error(NYql::TPosition pos, NYql::TIssueCode code) {
    HasPendingErrors = true;
    return MakeIssue(TSeverityIds::S_ERROR, code, pos);
}

IOutputStream& TContext::Warning(NYql::TPosition pos, NYql::TIssueCode code) {
    return MakeIssue(TSeverityIds::S_WARNING, code, pos);
}

IOutputStream& TContext::Info(NYql::TPosition pos) {
    return MakeIssue(TSeverityIds::S_INFO, TIssuesIds::INFO, pos);
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
    auto it = SQLHints.find(tokenPos);
    if (it == SQLHints.end()) {
        return result;
    }
    result = std::move(it->second);
    SQLHints.erase(it);
    return result;
}

void TContext::WarnUnusedHints() {
    if (!SQLHints.empty()) {
        // warn about first unused hint
        auto firstUnused = SQLHints.begin();
        YQL_ENSURE(!firstUnused->second.empty());
        const NSQLTranslation::TSQLHint& hint = firstUnused->second.front();
        Warning(hint.Pos, TIssuesIds::YQL_UNUSED_HINT) << "Hint " << hint.Name << " will not be used";
    }
}

IOutputStream& TContext::MakeIssue(ESeverity severity, TIssueCode code, NYql::TPosition pos) {
    if (severity == TSeverityIds::S_WARNING) {
        auto action = WarningPolicy.GetAction(code);
        if (action == EWarningAction::ERROR) {
            severity = TSeverityIds::S_ERROR;
            HasPendingErrors = true;
        } else if (action == EWarningAction::DISABLE) {
            return Cnull;
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
            ythrow NProtoAST::TTooManyErrors() << "Too many issues";
        }
    }

    Issues.AddIssue(TIssue(pos, TString()));
    auto& curIssue = Issues.back();
    curIssue.Severity = severity;
    curIssue.IssueCode = code;
    IssueMsgHolder.Reset(new TStringOutput(*Issues.back().MutableMessage()));
    return *IssueMsgHolder;
}

bool TContext::IsDynamicCluster(const TDeferredAtom& cluster) const {
    const TString* clusterPtr = cluster.GetLiteral();
    if (!clusterPtr) {
        return false;
    }
    TString unused;
    if (ClusterMapping.GetClusterProvider(*clusterPtr, unused)) {
        return false;
    }
    if (Settings.AssumeYdbOnClusterWithSlash && clusterPtr->StartsWith('/')) {
        return false;
    }
    return !Settings.DynamicClusterProvider.Empty();
}

bool TContext::SetPathPrefix(const TString& value, TMaybe<TString> arg) {
    if (arg.Defined()) {
        if (*arg == YtProviderName
            || *arg == KikimrProviderName
            || *arg == RtmrProviderName
            )
        {
            ProviderPathPrefixes[*arg] = value;
            return true;
        }

        TString normalizedClusterName;
        if (!GetClusterProvider(*arg, normalizedClusterName)) {
            Error() << "Unknown cluster or provider: " << *arg;
            IncrementMonCounter("sql_errors", "BadPragmaValue");
            return false;
        }

        ClusterPathPrefixes[normalizedClusterName] = value;
    } else {
        PathPrefix = value;
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
                            ? ClusterPathPrefixes.FindPtr(*cluster.GetLiteral())
                            : nullptr;
    if (clusterPrefix && !clusterPrefix->empty()) {
        return *clusterPrefix;
    } else {
        auto* providerPrefix = ProviderPathPrefixes.FindPtr(service);
        if (providerPrefix && !providerPrefix->empty()) {
            return *providerPrefix;
        } else if (!PathPrefix.empty()) {
            return PathPrefix;
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
    if (Exports.contains(name)) {
        Error(pos) << "Duplicate export symbol: " << name;
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
    for (auto& it: tableList) {
        if (it.Service != YtProviderName) {
            return true;
        }
    }

    for (auto& cl: Scoped->Local.UsedClusters) {
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


TMaybe<EColumnRefState> GetFunctionArgColumnStatus(TContext& ctx, const TString& module, const TString& func, size_t argIndex) {
    static const TSet<TStringBuf> denyForAllArgs = {
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
    static const TMap<std::pair<TStringBuf, size_t>, EColumnRefState> positionalArgsCustomStatus = {
        { {"frombytes", 1},           EColumnRefState::Deny },
        { {"enum", 0},                EColumnRefState::Deny },
        { {"asenum", 0},              EColumnRefState::Deny },
        { {"variant", 1},             EColumnRefState::Deny },
        { {"variant", 2},             EColumnRefState::Deny },
        { {"asvariant", 1},           EColumnRefState::Deny },
        { {"astagged", 1},            EColumnRefState::Deny },
        { {"ensuretype", 1},          EColumnRefState::Deny },
        { {"ensuretype", 2},          EColumnRefState::Deny },
        { {"ensureconvertibleto", 1}, EColumnRefState::Deny },
        { {"ensureconvertibleto", 2}, EColumnRefState::Deny },

        { {"nothing", 0},             EColumnRefState::Deny },
        { {"formattype", 0},          EColumnRefState::Deny },
        { {"instanceof", 0},          EColumnRefState::Deny },
        { {"pgtype", 0},              EColumnRefState::AsPgType },
        { {"pgconst", 0},             EColumnRefState::Deny },
        { {"pgconst", 1},             EColumnRefState::AsPgType },
        { {"pgcast", 1},              EColumnRefState::AsPgType },

        { {"unpickle", 0},            EColumnRefState::Deny },
        { {"typehandle", 0},          EColumnRefState::Deny },

        { {"listcreate", 0},          EColumnRefState::Deny },
        { {"setcreate", 0},           EColumnRefState::Deny },
        { {"dictcreate", 0},          EColumnRefState::Deny },
        { {"dictcreate", 1},          EColumnRefState::Deny },
        { {"weakfield", 1},           EColumnRefState::Deny },

        { {"Yson::ConvertTo", 1},     EColumnRefState::Deny },
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

    if (denyForAllArgs.contains(normalized)) {
        return EColumnRefState::Deny;
    }

    auto it = positionalArgsCustomStatus.find(std::make_pair(normalized, argIndex));
    if (it != positionalArgsCustomStatus.end()) {
        return it->second;
    }
    return {};
}

TTranslation::TTranslation(TContext& ctx)
    : Ctx(ctx)
{
}

TContext& TTranslation::Context() {
    return Ctx;
}

IOutputStream& TTranslation::Error() {
    return Ctx.Error();
}

TNodePtr TTranslation::GetNamedNode(const TString& name) {
    if (name == "$_") {
        Ctx.Error() << "Unable to reference anonymous name " << name;
        return nullptr;
    }
    auto res = Ctx.Scoped->LookupNode(name);
    if (!res) {
        Ctx.Error() << "Unknown name: " << name;
    }
    return SafeClone(res);
}

TString TTranslation::PushNamedNode(TPosition namePos, const TString& name, const TNodeBuilderByName& builder) {
    TString resultName = name;
    if (IsAnonymousName(name)) {
        resultName = "$_yql_anonymous_name_" + ToString(Ctx.AnonymousNameIndex++);
        YQL_ENSURE(Ctx.Scoped->NamedNodes.find(resultName) == Ctx.Scoped->NamedNodes.end());
    }
    auto node = builder(resultName);
    Y_DEBUG_ABORT_UNLESS(node);
    auto mapIt = Ctx.Scoped->NamedNodes.find(resultName);
    if (mapIt == Ctx.Scoped->NamedNodes.end()) {
        auto result = Ctx.Scoped->NamedNodes.insert(std::make_pair(resultName, TDeque<TNodeWithUsageInfoPtr>()));
        Y_DEBUG_ABORT_UNLESS(result.second);
        mapIt = result.first;
    }

    mapIt->second.push_front(MakeIntrusive<TNodeWithUsageInfo>(node, namePos, Ctx.ScopeLevel));
    return resultName;
}

TString TTranslation::PushNamedNode(NYql::TPosition namePos, const TString &name, NSQLTranslationV1::TNodePtr node) {
    return PushNamedNode(namePos, name, [node](const TString&) { return node; });
}

TString TTranslation::PushNamedAtom(TPosition namePos, const TString& name) {
    auto buildAtom = [namePos](const TString& resultName) {
        return BuildAtom(namePos, resultName);
    };
    return PushNamedNode(namePos, name, buildAtom);
}

void TTranslation::PopNamedNode(const TString& name) {
    auto mapIt = Ctx.Scoped->NamedNodes.find(name);
    Y_DEBUG_ABORT_UNLESS(mapIt != Ctx.Scoped->NamedNodes.end());
    Y_DEBUG_ABORT_UNLESS(mapIt->second.size() > 0);
    auto& top = mapIt->second.front();
    if (!top->IsUsed && !Ctx.HasPendingErrors && !name.StartsWith("$_")) {
        Ctx.Warning(top->NamePos, TIssuesIds::YQL_UNUSED_SYMBOL) << "Symbol " << name << " is not used";
    }
    mapIt->second.pop_front();
    if (mapIt->second.empty()) {
        Ctx.Scoped->NamedNodes.erase(mapIt);
    }
}

void TTranslation::WarnUnusedNodes() const {
    if (Ctx.HasPendingErrors) {
        // result is not reliable in this case
        return;
    }
    for (const auto& [name, items]: Ctx.Scoped->NamedNodes) {
        if (name.StartsWith("$_")) {
            continue;
        }
        for (const auto& item : items) {
            if (!item->IsUsed && item->Level == Ctx.ScopeLevel) {
                Ctx.Warning(item->NamePos, TIssuesIds::YQL_UNUSED_SYMBOL) << "Symbol " << name << " is not used";
            }
        }
    }
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

} // namespace NSQLTranslationV1
