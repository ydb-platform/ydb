#include "context.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/folder/pathsplit.h>
#include <util/string/join.h>
#include <util/stream/null.h>

#ifdef GetMessage
#undef GetMessage
#endif

using namespace NYql;

namespace NSQLTranslationV0 {

namespace {

TNodePtr AddTablePathPrefix(TContext &ctx, TStringBuf prefixPath, const TDeferredAtom& path) {
    Y_UNUSED(ctx);
    if (prefixPath.empty()) {
        return path.Build();
    }

    YQL_ENSURE(path.GetLiteral(), "TODO support prefix for deferred atoms");
    prefixPath.SkipPrefix("//");

    TPathSplitUnix prefixPathSplit(prefixPath);
    TPathSplitUnix pathSplit(*path.GetLiteral());

    if (pathSplit.IsAbsolute) {
        return path.Build();
    }

    return BuildQuotedAtom(path.Build()->GetPos(), prefixPathSplit.AppendMany(pathSplit.begin(), pathSplit.end()).Reconstruct());
}

typedef bool TContext::*TPragmaField;

THashMap<TStringBuf, TPragmaField> CTX_PRAGMA_FIELDS = {
    {"PullUpFlatMapOverJoin", &TContext::PragmaPullUpFlatMapOverJoin},
};

} // namespace

TContext::TContext(const NSQLTranslation::TTranslationSettings& settings,
                   TIssues& issues)
    : ClusterMapping(settings.ClusterMapping)
    , PathPrefix(settings.PathPrefix)
    , ClusterPathPrefixes(settings.ClusterPathPrefixes)
    , Settings(settings)
    , Pool(new TMemoryPool(4096))
    , Issues(issues)
    , IncrementMonCounterFunction(settings.IncrementCounter)
    , CurrCluster(settings.DefaultCluster)
    , HasPendingErrors(false)
    , Libraries(settings.Libraries)
{
    Position.File = settings.File;

    for (auto& flag: settings.Flags) {
        bool value = true;
        TStringBuf key = flag;
        auto ptr = CTX_PRAGMA_FIELDS.FindPtr(key);
        if (!ptr && key.SkipPrefix("Disable")) {
            value = false;
            ptr = CTX_PRAGMA_FIELDS.FindPtr(key);
        }
        if (ptr) {
            this->*(*ptr) = value;
        }
    }
}

TContext::~TContext()
{
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

IOutputStream& TContext::Error() {
    return Error(Pos());
}

IOutputStream& TContext::Error(NYql::TPosition pos) {
    HasPendingErrors = true;
    return MakeIssue(TSeverityIds::S_ERROR, TIssuesIds::DEFAULT_ERROR, pos);
}

IOutputStream& TContext::Warning(NYql::TPosition pos, NYql::TIssueCode code) {
    return MakeIssue(TSeverityIds::S_WARNING, code, pos);
}

IOutputStream& TContext::Info(NYql::TPosition pos) {
    return MakeIssue(TSeverityIds::S_INFO, TIssuesIds::INFO, pos);
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

TNodePtr TContext::GetPrefixedPath(const TString& cluster, const TDeferredAtom& path) {
    auto* clusterPrefix = ClusterPathPrefixes.FindPtr(cluster);
    if (clusterPrefix && !clusterPrefix->empty()) {
        return AddTablePathPrefix(*this, *clusterPrefix, path);
    } else {
        auto provider = GetClusterProvider(cluster);
        YQL_ENSURE(provider.Defined());

        auto* providerPrefix = ProviderPathPrefixes.FindPtr(*provider);
        if (providerPrefix && !providerPrefix->empty()) {
            return AddTablePathPrefix(*this, *providerPrefix, path);
        } else if (!PathPrefix.empty()) {
            return AddTablePathPrefix(*this, PathPrefix, path);
        }

        return path.Build();
    }
}

TNodePtr TContext::UniversalAlias(const TString& baseName, TNodePtr&& node) {
    auto alias = MakeName(baseName);
    UniversalAliases.emplace(alias, node);
    return BuildAtom(node->GetPos(), alias, TNodeFlags::Default);
}

TString TContext::HasBlockShortcut(const TNodePtr& baseNode) {
    YQL_ENSURE(ShortcutCurrentLevel, "Push\\Pop shortcuts not balanced");
    auto shortIter = Shortcuts.find(ShortcutCurrentLevel);
    if (shortIter == Shortcuts.end()) {
        return {};
    }
    const auto& baseMap = shortIter->second.BaseMap;
    const auto iter = baseMap.find(baseNode.Get());
    if (iter == baseMap.end()) {
        return {};
    }
    return iter->second;
}

TString TContext::RegisterBlockShortcut(const TNodePtr& baseNode, const TNodePtr& node, const TString& baseName) {
    YQL_ENSURE(ShortcutCurrentLevel, "Push\\Pop shortcuts not balanced");
    YQL_ENSURE(node->HasState(ENodeState::Initialized));
    YQL_ENSURE(!HasBlockShortcut(baseNode));
    const auto alias = MakeName(baseName);
    auto& shortcuts = Shortcuts[ShortcutCurrentLevel];
    shortcuts.BaseMap.emplace(baseNode.Get(), alias);
    shortcuts.Goal.emplace_back(std::make_pair(alias, node));
    return alias;
}

TNodePtr TContext::GetBlockShortcut(const TString& alias) const {
    YQL_ENSURE(ShortcutCurrentLevel, "Push\\Pop shortcuts not balanced");
    auto shortIter = Shortcuts.find(ShortcutCurrentLevel);
    YQL_ENSURE(shortIter != Shortcuts.end(), "Expected block shortcut exist");
    for (const auto& shortcutPair: shortIter->second.Goal) {
        if (shortcutPair.first == alias) {
            return shortcutPair.second;
        }
    }
    Y_ABORT("Expected block shortcut exist");
}

TNodePtr TContext::GroundBlockShortcuts(NYql::TPosition pos, TNodePtr groundList) {
    YQL_ENSURE(ShortcutCurrentLevel, "Push\\Pop shortcuts not balanced");
    auto shortIter = Shortcuts.find(ShortcutCurrentLevel);
    TNodePtr result = groundList;
    if (shortIter != Shortcuts.end()) {
        if (!result) {
            result = new TAstListNodeImpl(pos);
        }
        for (const auto& shortcutPair: shortIter->second.Goal) {
            result = result->L(result, result->Y("let", shortcutPair.first, shortcutPair.second));
        }
    }
    PopBlockShortcuts();
    return result;
}

TNodePtr TContext::GroundBlockShortcutsForExpr(const TNodePtr& expr) {
    YQL_ENSURE(ShortcutCurrentLevel, "Push\\Pop shortcuts not balanced");
    YQL_ENSURE(expr);
    auto ground = GroundBlockShortcuts(expr->GetPos());
    return GroundWithExpr(ground, expr);
}

void TContext::PushBlockShortcuts() {
    ++ShortcutCurrentLevel;
}

void TContext::PopBlockShortcuts() {
    YQL_ENSURE(ShortcutCurrentLevel, "Push\\Pop shortcuts not balanced");
    auto shortcuts = Shortcuts.find(ShortcutCurrentLevel);
    --ShortcutCurrentLevel;
    if (shortcuts != Shortcuts.end()) {
        Shortcuts.erase(shortcuts);
    }
}

bool TContext::DeclareVariable(const TString& varName, const TNodePtr& typeNode) {
    Variables.emplace(varName, typeNode);
    return true;
}

bool TContext::AddExports(const TVector<TString>& symbols) {
    for (const auto& symbol: symbols) {
        if (Exports.contains(symbol)) {
            Error() << "Duplicate export symbol: " << symbol;
            return false;
        }
        if (NamedNodes.find(symbol) == NamedNodes.end()) {
            Error() << "Unknown named node: " << symbol;
            return false;
        }
        Exports.emplace(symbol);
    }
    return true;
}

TString TContext::AddImport(const TVector<TString>& modulePath) {
    YQL_ENSURE(!modulePath.empty());
    const TString path = JoinRange("/", modulePath.cbegin(), modulePath.cend());
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

TString TContext::GetServiceName(const ISource& source) const {
    TTableList tableList;
    source.GetInputTables(tableList);

    TSet<TString> clusters;
    for (auto& it: tableList) {
        if (auto provider = GetClusterProvider(it.Cluster)) {
            return *provider;
        }
    }

    for (auto& cluster: UsedClusters) {
        if (auto provider = GetClusterProvider(cluster)) {
            return *provider;
        }
    }

    return CurrCluster.empty() ? TString() : GetClusterProvider(CurrCluster).GetOrElse(TString());
}

bool TContext::UseUnordered(const ISource& source) const {
    return YtProviderName == to_lower(GetServiceName(source));
}

bool TContext::UseUnordered(const TTableRef& table) const {
    return YtProviderName == to_lower(GetClusterProvider(table.Cluster).GetOrElse(TString()));
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
    auto mapIt = Ctx.NamedNodes.find(name);
    if (mapIt == Ctx.NamedNodes.end()) {
        Ctx.Error() << "Unknown name: " << name;
        return nullptr;
    }
    Y_DEBUG_ABORT_UNLESS(!mapIt->second.empty());
    return mapIt->second.top()->Clone();
}

void TTranslation::PushNamedNode(const TString& name, TNodePtr node) {
    Y_DEBUG_ABORT_UNLESS(node);
    auto mapIt = Ctx.NamedNodes.find(name);
    if (mapIt == Ctx.NamedNodes.end()) {
        auto result = Ctx.NamedNodes.insert(std::make_pair(name, TStack<TNodePtr>()));
        Y_DEBUG_ABORT_UNLESS(result.second);
        mapIt = result.first;
    }

    mapIt->second.push(node);
}

void TTranslation::PopNamedNode(const TString& name) {
    auto mapIt = Ctx.NamedNodes.find(name);
    Y_DEBUG_ABORT_UNLESS(mapIt != Ctx.NamedNodes.end());
    Y_DEBUG_ABORT_UNLESS(mapIt->second.size() > 0);
    mapIt->second.pop();
    if (mapIt->second.empty()) {
        Ctx.NamedNodes.erase(mapIt);
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
    Error() << ruleName << ": alternative is not implemented yet: " << GetDescription(node, descr->FindFieldByNumber(altCase));
}

} // namespace NSQLTranslationV0
