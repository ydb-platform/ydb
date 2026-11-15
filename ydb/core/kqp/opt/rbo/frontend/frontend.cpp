#include "like.h"
#include "frontend.h"
#include "bindings.h"
#include "predicate.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <cmath>
#include <limits>
#include <locale>
#include <map>
#include <set>
#include <sstream>
#include <tuple>
#include <unordered_set>

namespace NKikimr::NKqp::NFrontend {

TParameterType::TParameterType(TRows rows) {
    std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) { return a.Name < b.Name; });
    Value = std::move(rows);
}

namespace {

std::string Lower(std::string_view value) {
    std::string result(value);
    for (auto& ch : result) if (ch >= 'A' && ch <= 'Z') ch += 'a' - 'A';
    return result;
}

void CheckColumnProfile(std::string_view name, TSourcePosition position) {
    if (name.starts_with("_yql_"))
        throw TUnsupportedFeature(EFeature::Backend, position, "system columns are outside native-read-v1");
}

void CheckFunctionProfile(std::string_view name, TSourcePosition position) {
    // Unavailable aggregates must be rejected before their row arguments
    // are resolved in the post-aggregation scope.
    if (name == "stddev_pop" || name == "stddevpop" || name == "stddevpopulation" || name == "populationstddev" ||
        name == "stddevp" || name == "variance" || name == "var_samp" || name == "var_pop")
        throw TUnsupportedFeature(EFeature::Function, position, "aggregate is outside the native profile: " + std::string(name));
    // These convenience aliases in the experimental expression API do
    // not have the same meaning (or do not exist) in YQL SQL.
    if (name == "lower" || name == "upper" || name == "substr" || name == "contains" ||
        name == "string::tolower" || name == "string::toupper" || name == "concat")
        throw TUnsupportedFeature(EFeature::Function, position, "function is outside the compatible native profile: " + std::string(name));
}

const std::string& PragmaString(const TStatement& statement, std::size_t index) {
    const auto& value = statement.PragmaValues.at(index);
    if (value.Kind == EPragmaValueKind::Binding)
        throw TUnsupportedFeature(EFeature::Pragma, value.Position, "dynamic pragma values are outside the native profile");
    if (value.Kind != EPragmaValueKind::String)
        throw TFrontendError(value.Position, "expected string value for pragma " + statement.Name);
    return value.Text;
}

const std::string& PragmaValue(const TStatement& statement) {
    if (statement.PragmaValues.size() > 1)
        throw TFrontendError(statement.Position, "expected at most one value for pragma " + statement.Name);
    static const std::string empty;
    return statement.PragmaValues.empty() ? empty : PragmaString(statement, 0);
}

bool PragmaBoolean(const TStatement& statement, bool emptyEnabled = false) {
    const auto value = Lower(PragmaValue(statement));
    if ((emptyEnabled && value.empty()) || value == "true" || value == "t" || value == "yes" || value == "y" ||
        value == "on" || value == "1" || value == "da") return true;
    if (value == "false" || value == "f" || value == "no" || value == "n" || value == "off" || value == "0" || value == "net") return false;
    throw TFrontendError(statement.Position, "invalid Boolean value for pragma " + statement.Name);
}

void CheckWarningPragma(const TStatement& statement) {
    if (statement.PragmaValues.size() != 2)
        throw TFrontendError(statement.Position, "warning pragma requires action and issue code");
    const auto action = Lower(PragmaString(statement, 0));
    if (action != "disable" && action != "default" && action != "error")
        throw TFrontendError(statement.Position, "unknown warning action: " + PragmaString(statement, 0));
    const auto& pattern = PragmaString(statement, 1);
    if (pattern != "*") {
        std::uint32_t code = 0;
        const auto parsed = std::from_chars(pattern.data(), pattern.data() + pattern.size(), code);
        if (parsed.ec != std::errc{} || parsed.ptr != pattern.data() + pattern.size())
            throw TFrontendError(statement.Position, "warning issue code must be Uint32 or '*'");
    }
    // The native frontend currently emits errors, not warnings. Disabling a
    // warning is therefore already satisfied. Restoring or promoting warnings
    // needs a native diagnostic policy and must not be silently ignored.
    if (action != "disable")
        throw TUnsupportedFeature(EFeature::Pragma, statement.Position, "native warning actions support only disable");
}

std::optional<TPlannerSetting> PlannerSetting(const TStatement& statement) {
    using S = EPlannerSetting;
    auto name = Lower(statement.Name);
    const auto dot = name.find('.');
    if (dot == std::string::npos || (name.substr(0, dot) != "ydb" && name.substr(0, dot) != "kikimr")) return {};
    name.erase(0, dot + 1);
    if (name.empty() || name.front() == '_') return {};
    if (name.find("__") != std::string::npos || std::count(name.begin(), name.end(), '_') >= 5)
        throw TFrontendError(statement.Position, "invalid underscores in pragma name: " + statement.Name);
    std::erase(name, '_');
    static constexpr std::pair<std::string_view, S> names[]{
        {"optdisableautoindexselection", S::OptDisableAutoIndexSelection},
        {"enableautoindexselectionforindexlookupjoin", S::EnableAutoIndexSelectionForIndexLookupJoin},
        {"optdisabletopsort", S::OptDisableTopSort}, {"optenablepredicateextract", S::OptEnablePredicateExtract},
        {"optenableinplaceupdate", S::OptEnableInplaceUpdate},
        {"optshuffleelimination", S::OptShuffleElimination}, {"optshuffleeliminationwithmap", S::OptShuffleEliminationWithMap},
        {"optshuffleeliminationforaggregation", S::OptShuffleEliminationForAggregation},
        {"enablenewrbophysicalstagepeephole", S::EnableNewRBOPhysicalStagePeephole}, {"windowfunctionsv2", S::WindowFunctionsV2},
        {"optenableolappushdown", S::OptEnableOlapPushdown}, {"optenableolappushdownaggregate", S::OptEnableOlapPushdownAggregate},
        {"optenableolappushdownprojections", S::OptEnableOlapPushdownProjections}, {"optenableolappushdownregexp", S::OptEnableOlapPushdownRegexp},
        {"dqhashoperatorsuseblocks", S::DqHashOperatorsUseBlocks}, {"useblockhashjoin", S::UseBlockHashJoin},
        {"disableblockexecution", S::DisableBlockExecution}, {"optfallbacktolegacyoptimizer", S::OptFallbackToLegacyOptimizer},
        {"costbasedoptimizationlevel", S::CostBasedOptimizationLevel}, {"dqchannelversion", S::DqChannelVersion},
        {"costbasedoptimization", S::CostBasedOptimization}, {"hashjoinmode", S::HashJoinMode}, {"optimizerhints", S::OptimizerHints},
    };
    const auto found = std::find_if(std::begin(names), std::end(names), [&](const auto& entry) { return entry.first == name; });
    if (found == std::end(names)) return {};
    const auto kind = found->second;
    const auto& argument = PragmaValue(statement);
    const auto badValue = [&] { return TFrontendError(statement.Position, "invalid value for pragma " + statement.Name + ": " + argument); };
    if (kind == S::OptimizerHints || kind == S::HashJoinMode) return TPlannerSetting{kind, argument, statement.Position};
    if (kind == S::CostBasedOptimization) {
        const auto value = Lower(argument);
        if (value != "on" && value != "off" && value != "auto") throw badValue();
        return TPlannerSetting{kind, value, statement.Position};
    }
    if (kind == S::CostBasedOptimizationLevel || kind == S::DqChannelVersion) {
        std::string_view value = argument;
        if (value.starts_with('+')) value.remove_prefix(1);
        std::uint32_t number = 0;
        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), number);
        if (error != std::errc{} || end != value.data() + value.size()) throw badValue();
        return TPlannerSetting{kind, number, statement.Position};
    }
    const bool enabled = PragmaBoolean(statement, true);
    if (kind == S::OptFallbackToLegacyOptimizer && enabled)
        throw TUnsupportedFeature(EFeature::Pragma, statement.Position, "native plans cannot request fallback to the YQL optimizer");
    return TPlannerSetting{kind, enabled, statement.Position};
}

void WindowChildren(const TSyntaxWindow& window, std::vector<TSyntaxId>& out) {
    for (const auto& key : window.Partition) out.push_back(key.Expression);
    for (const auto& key : window.Order) out.push_back(key.Expression);
    if (window.BeginOffset != NoSyntax) out.push_back(window.BeginOffset);
    if (window.EndOffset != NoSyntax) out.push_back(window.EndOffset);
}

bool IsAggregate(std::string_view name);

// Resolve each reference to its lexical declaration once. Closures keep
// these IDs when later statements shadow a name; expanding a lambda or query
// never copies an environment or re-resolves its free names.
class TGlobalBindings {
public:
    static constexpr std::uint32_t Unknown = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::uint32_t Parameter = Unknown - 1;
    enum EOption : std::uint8_t { AnsiIn = 1, AnsiCurrentRow = 2, UnicodeLiterals = 4, ClassicDivision = 8, CheckedOps = 16 };
    struct TExternal { const TScalarBinding* Body; bool Callable; };

    TGlobalBindings(const TParsedQuery& syntax, const TBindSettings& settings)
        : Syntax(syntax), Settings(settings), References(syntax.Expressions.size(), Unknown), Seen(syntax.Expressions.size()),
          Options((settings.AnsiIn ? AnsiIn : 0) | (settings.AnsiCurrentRow ? AnsiCurrentRow : 0) |
              (settings.UnicodeLiterals ? UnicodeLiterals : 0) | (settings.ClassicDivision ? ClassicDivision : 0) |
              (settings.CheckedOps ? CheckedOps : 0)),
          ExpressionOptions(syntax.Expressions.size(), Options), CapturedPragmas(syntax.Statements.size()),
          QueryBindings(syntax.Statements.size()), Prefix(settings.TablePathPrefix) {
        Definitions.reserve(syntax.Statements.size());
        for (const auto& statement : syntax.Statements) Definitions.push_back(statement.Expression);
        for (const auto& [name, type] : settings.ParameterTypes) Names[name] = Parameter;
        if (settings.ExternalBindings) {
            const auto add = [&](const std::string& name, const TScalarBinding& body, bool callable) {
                if (name.empty() || name.starts_with('$')) throw TFrontendError({}, "external binding names must omit '$'");
                if (!callable && body.Arity()) throw TFrontendError({}, "external scalar value cannot have formal arguments");
                if (!Names.emplace(name, Definitions.size()).second) throw TFrontendError({}, "duplicate external binding: " + name);
                Definitions.push_back(NoSyntax);
                External.push_back({&body, callable});
            };
            for (const auto& [name, body] : settings.ExternalBindings->Values) add(name, body, false);
            for (const auto& [name, body] : settings.ExternalBindings->Callables) {
                if (name.empty() || name.starts_with('$')) throw TFrontendError({}, "external binding names must omit '$'");
                if (name.find("::") == std::string::npos) add(name, body, true);
            }
        }
        for (std::uint32_t i = 0; i < syntax.Statements.size(); ++i) {
            const auto& statement = syntax.Statements[i];
            if (statement.Explain != EExplainMode::None)
                throw TUnsupportedFeature(EFeature::Statement, statement.Position, "SQL EXPLAIN requires an explain-result execution contract");
            if (statement.Kind == EStatementKind::Pragma) CapturedPragmas[i] = ApplyPragma(statement);
            Expression(statement.Expression);
            if (statement.Query) Select(*statement.Query);
            if (statement.Write) {
                const auto& write = *statement.Write;
                Source(write.Target, true);
                if (write.Input) Select(*write.Input);
                for (const auto& row : write.Rows) for (auto value : row) Expression(value);
                Expression(write.Where);
                for (const auto& assignment : write.Assignments) Expression(assignment.Expression);
            }
            if (statement.Kind == EStatementKind::Declaration) Names[statement.Name] = Parameter;
            if (statement.Kind == EStatementKind::Binding) {
                QueryBindings[i] = statement.Query;
                if (!statement.Query && statement.Expression != NoSyntax &&
                    Syntax.Expressions[statement.Expression].Kind == ESyntaxKind::Variable) {
                    const auto binding = References[statement.Expression];
                    if (binding < QueryBindings.size()) QueryBindings[i] = QueryBindings[binding];
                }
                Names[statement.Name] = i;
            }
        }
    }

    std::uint32_t operator[](TSyntaxId expression) const { return References.at(expression); }
    std::span<const TExternal> ExternalValues() const { return External; }
    TSyntaxId Definition(std::uint32_t binding) const {
        return binding < Definitions.size() ? Definitions[binding] : NoSyntax;
    }
    const std::vector<std::uint32_t>& LambdaBindings(TSyntaxId lambda) const { return Lambdas.at(lambda); }
    bool IsReferenced(TSyntaxId expression) const { return Seen.at(expression); }
    bool Uses(TSyntaxId expression, EOption option) const { return ExpressionOptions.at(expression) & option; }
    std::uint8_t Flags(TSyntaxId expression) const { return ExpressionOptions.at(expression); }
    bool IsCapturedPragma(std::size_t statement) const { return CapturedPragmas.at(statement); }
    bool AllowsUnnamedGroups(const TSelect& query) const { return UnnamedGroups.contains(&query); }
    std::uint32_t SourceBinding(const TSource& source) const { return Sources.at(&source); }
    const TCteBinding* SourceCte(const TSource& source) const {
        const auto it = CteSources.find(&source);
        return it == CteSources.end() ? nullptr : it->second;
    }
    const TTableAddress& PhysicalAddress(const TSource& source) const { return PhysicalAddresses.at(&source); }
    const TSyntaxWindow& Window(const TSyntaxWindow& syntax) const {
        return syntax.ReferenceOnly ? *Windows.at(&syntax) : syntax;
    }

    bool IsQueryBinding(std::size_t index) const { return bool(QueryBindings.at(index)); }
    const TSelect* QueryReference(TSyntaxId expression) const {
        if (Syntax.Expressions.at(expression).Kind != ESyntaxKind::Variable) return {};
        const auto binding = References.at(expression);
        return binding < QueryBindings.size() ? QueryBindings[binding].get() : nullptr;
    }
    std::shared_ptr<TSelect> Query(const TSource& source) const {
        if (source.ParameterTable) return {};
        if (source.Query) return source.Query;
        if (!source.NameIsBinding) {
            const auto* cte = SourceCte(source);
            return cte ? cte->Query : nullptr;
        }
        const auto binding = Sources.at(&source);
        if (binding >= Syntax.Statements.size()) throw TFrontendError(source.Position, "unknown query binding: " + source.Name);
        if (!QueryBindings[binding]) throw TUnsupportedFeature(EFeature::Statement, source.Position, "native sources require a named query: " + source.Name);
        return QueryBindings[binding];
    }

private:
    // Interpret expression pragmas once from the owning statement syntax.
    // Capture the resulting options alongside lexical references at definitions,
    // including query and lambda bodies that are bound later.
    bool ApplyPragma(const TStatement& statement) {
        const auto name = Lower(statement.Name);
        if (name == "tablepathprefix") {
            if (statement.PragmaValues.empty()) throw TFrontendError(statement.Position, "TablePathPrefix requires one string value");
            Prefix = PragmaValue(statement);
            return true;
        }
        if (name == "classicdivision" || name == "checkedops") {
            if (statement.PragmaValues.size() != 1) throw TFrontendError(statement.Position, statement.Name + " requires one Boolean string value");
            const bool enabled = PragmaBoolean(statement);
            const auto flag = name == "classicdivision" ? ClassicDivision : CheckedOps;
            Options = enabled ? Options | flag : Options & ~flag;
            return true;
        }
        static constexpr std::pair<std::string_view, EOption> flags[]{
            {"ansicurrentrow", AnsiCurrentRow}, {"ansiinforemptyornullableitemscollections", AnsiIn},
            {"unicodeliterals", UnicodeLiterals}};
        const bool disabled = name.starts_with("disable");
        const auto flagName = std::string_view(name).substr(disabled ? 7 : 0);
        for (const auto& [label, flag] : flags) if (flagName == label) {
            if (!statement.PragmaValues.empty()) return false;
            Options = disabled ? Options & ~flag : Options | flag;
            return true;
        }
        if (flagName == "yqlselectallowunnamedgroupbyexpr") {
            if (!statement.PragmaValues.empty()) return false;
            AllowUnnamedGroups = !disabled;
            return true;
        }
        return false;
    }

    std::uint32_t Resolve(const std::string& name) const {
        for (auto scope = LocalScopes.rbegin(); scope != LocalScopes.rend(); ++scope)
            if (const auto found = scope->find(name); found != scope->end()) return found->second;
        const auto found = Names.find(name);
        return found == Names.end() ? Unknown : found->second;
    }
    void Expression(TSyntaxId root) {
        if (root == NoSyntax || Seen.at(root)) return;
        std::vector<TSyntaxId> pending{root};
        while (!pending.empty()) {
            const auto id = pending.back(); pending.pop_back();
            if (Seen.at(id)) continue;
            Seen[id] = true;
            ExpressionOptions[id] = Options;
            const auto& node = Syntax.Expressions[id];
            if (node.NullTreatment != ENullTreatment::Default)
                throw TUnsupportedFeature(EFeature::Window, node.Position, "NULL treatment requires native execution support");
            if (node.Filter != NoSyntax) {
                if (node.Kind != ESyntaxKind::Call || !IsAggregate(Lower(node.Text)))
                    throw TFrontendError(node.Position, "FILTER requires an aggregate function");
                pending.push_back(node.Filter);
            }
            if (node.Lambda) {
                if (++Depth > Settings.MaxExpansionDepth)
                    throw TResourceLimitError(node.Position, "binding scope depth limit exceeded");
                LocalScopes.emplace_back();
                std::vector<std::uint32_t> bindings;
                for (const auto& name : node.Lambda->Parameters) {
                    const auto binding = Definitions.size();
                    if (!LocalScopes.back().emplace(name, binding).second)
                        throw TFrontendError(node.Position, "duplicate lambda parameter");
                    Definitions.push_back(NoSyntax);
                    bindings.push_back(binding);
                }
                for (std::size_t i = 0; i < node.Lambda->Locals.size(); ++i) {
                    Expression(node.Arguments[i]);
                    const auto binding = Definitions.size();
                    LocalScopes.back()[node.Lambda->Locals[i]] = binding;
                    Definitions.push_back(node.Arguments[i]);
                    bindings.push_back(binding);
                }
                Expression(node.Arguments.back());
                Lambdas.emplace(id, std::move(bindings));
                LocalScopes.pop_back();
                --Depth;
                continue;
            }
            if (node.Kind == ESyntaxKind::Variable) References[id] = Resolve(node.Text);
            if (node.Kind == ESyntaxKind::Call && node.Text.starts_with('$')) References[id] = Resolve(node.Text.substr(1));
            pending.insert(pending.end(), node.Arguments.begin(), node.Arguments.end());
            if (node.Window) {
                if (node.Window->ReferenceOnly) {
                    const auto found = WindowScope ? WindowScope->find(node.Window->Name) : TWindowNames::const_iterator{};
                    if (!WindowScope || found == WindowScope->end())
                        throw TFrontendError(node.Window->Position, "unknown window: " + node.Window->Name);
                    Windows.emplace(node.Window.get(), found->second);
                } else CheckWindow(*node.Window);
                WindowChildren(Window(*node.Window), pending);
            }
            if (node.Query) Select(*node.Query);
        }
    }
    void Source(const TSource& source, bool physical = false) {
        if (source.Options && (!source.Options->Hints.empty() || source.Options->Sampling))
            throw TUnsupportedFeature(EFeature::Backend, source.Position, "source hints and sampling require native execution support");
        if (!physical) {
            if (source.Query) { Select(*source.Query); return; }
            if (source.NameIsBinding) { Sources.emplace(&source, Resolve(source.Name.substr(1))); return; }
            for (auto scope = CteScopes.rbegin(); source.Cluster.empty() && scope != CteScopes.rend(); ++scope) {
                if (const auto it = scope->find(source.Name); it != scope->end()) {
                    CteSources.emplace(&source, it->second);
                    return;
                }
            }
        }
        TTableAddress address{source.Cluster.empty() ? Settings.DefaultCluster : source.Cluster, source.Name};
        const auto configured = Settings.ClusterPrefixes.find(address.Cluster);
        const auto& prefix = configured == Settings.ClusterPrefixes.end() ? Prefix : configured->second;
        if (!prefix.empty() && !address.Name.starts_with('/'))
            address.Name = prefix + (prefix.ends_with('/') ? "" : "/") + address.Name;
        PhysicalAddresses.emplace(&source, std::move(address));
    }
    void Select(const TSelect& query) {
        if (++Depth > Settings.MaxExpansionDepth) throw TResourceLimitError(query.Position, "binding scope depth limit exceeded");
        TWindowNames windows;
        for (const auto& definition : query.Windows) {
            CheckWindow(*definition.Specification);
            if (!windows.emplace(definition.Name, definition.Specification.get()).second)
                throw TFrontendError(definition.Specification->Position, "duplicate window: " + definition.Name);
        }
        const auto* outerWindows = std::exchange(WindowScope, &windows);
        if (AllowUnnamedGroups) UnnamedGroups.insert(&query);
        CteScopes.emplace_back();
        for (const auto& cte : query.Ctes) {
            if (CteScopes.back().contains(cte.Name)) throw TFrontendError(cte.Query->Position, "duplicate query binding: " + cte.Name);
            Select(*cte.Query);
            CteScopes.back().emplace(cte.Name, &cte);
        }
        if (query.Source) Source(*query.Source);
        for (const auto& join : query.Joins) { Source(join.Source); Expression(join.Predicate); }
        for (const auto& item : query.Projection) Expression(item.Expression);
        Expression(query.Where);
        for (const auto& item : query.GroupBy) Expression(item.Expression);
        Expression(query.Having);
        for (const auto& item : query.OrderBy) Expression(item.Expression);
        Expression(query.Limit);
        Expression(query.Offset);
        for (const auto& branch : query.SetInputs) Select(*branch);
        CteScopes.pop_back();
        WindowScope = outerWindows;
        --Depth;
    }

    static void CheckWindow(const TSyntaxWindow& window) {
        if (!window.Name.empty())
            throw TUnsupportedFeature(EFeature::Window, window.Position, "inherited window specification requires native execution support");
        if (window.CompactPartition || window.Exclusion != EWindowExclusion::Default)
            throw TUnsupportedFeature(EFeature::Window, window.Position, "compact partition and window exclusion require native execution support");
        for (const auto& key : window.Partition) if (!key.Alias.empty())
            throw TUnsupportedFeature(EFeature::Window, window.Position, "aliased window partition key requires native execution support");
    }

    const TParsedQuery& Syntax;
    const TBindSettings& Settings;
    std::vector<std::uint32_t> References;
    std::vector<bool> Seen;
    std::uint8_t Options;
    std::vector<std::uint8_t> ExpressionOptions;
    std::vector<bool> CapturedPragmas;
    bool AllowUnnamedGroups = false;
    std::unordered_set<const TSelect*> UnnamedGroups;
    std::unordered_map<std::string, std::uint32_t> Names;
    std::vector<TSyntaxId> Definitions;
    std::vector<TExternal> External;
    std::unordered_map<TSyntaxId, std::vector<std::uint32_t>> Lambdas;
    std::vector<std::unordered_map<std::string, std::uint32_t>> LocalScopes;
    std::unordered_map<const TSource*, std::uint32_t> Sources;
    std::vector<std::shared_ptr<TSelect>> QueryBindings;
    std::string Prefix;
    std::vector<std::unordered_map<std::string, const TCteBinding*>> CteScopes;
    std::unordered_map<const TSource*, const TCteBinding*> CteSources;
    std::unordered_map<const TSource*, TTableAddress> PhysicalAddresses;
    using TWindowNames = std::unordered_map<std::string_view, const TSyntaxWindow*>;
    const TWindowNames* WindowScope = nullptr;
    std::unordered_map<const TSyntaxWindow*, const TSyntaxWindow*> Windows;
    std::size_t Depth = 0;
};

bool IsSampleStddev(std::string_view name) {
    return name == "stddev" || name == "stddev_sample" || name == "stddev_samp" || name == "stddev_s" ||
        name == "stddevsample" || name == "stddevsamp" || name == "stddevs";
}

bool IsValueByAggregate(std::string_view name) {
    return name == "min_by" || name == "max_by" || name == "minby" || name == "maxby";
}

bool IsAggregate(std::string_view name) {
    return name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max" || name == "some" || IsSampleStddev(name) || IsValueByAggregate(name);
}

struct TScopeColumn {
    TColumnId Id;
    bool QualifiedOnly = false;
    std::string Name;
    std::string Qualifier;
    TType Type;

    TScopeColumn(TColumnId id, std::string name, std::string qualifier, TType type)
        : Id(id), Name(std::move(name)), Qualifier(std::move(qualifier)), Type(type) {}

    bool MatchesStar(std::string_view qualifier) const {
        return qualifier.empty() ? !QualifiedOnly : Qualifier == qualifier;
    }
};

struct TStringHash {
    using is_transparent = void;
    std::size_t operator()(std::string_view value) const { return std::hash<std::string_view>{}(value); }
};

class TScope {
public:
    struct TPath {
        const TScopeColumn* Column = nullptr;
        std::size_t Members = 0;
        bool Shadowed = false;
    };
    TScope() = default;
    // A read scope can share the immutable snapshot's lexical column order.
    // The snapshot outlives all binding scopes; column names and ordinals stay
    // fixed until Add/Merge builds the ordinary mutable name index.
    TScope(std::vector<TScopeColumn> columns, std::span<const std::size_t> catalogOrder)
        : Columns(std::move(columns)), CatalogOrder(catalogOrder) {}

    void Reserve(std::size_t columns) {
        Columns.reserve(columns);
        if (CatalogOrder.empty()) Names.reserve(columns * (MixedQualifiers ? 2 : 1));
    }

    void Add(TScopeColumn column) {
        MaterializeIndex();
        const auto index = Columns.size();
        // A single-source scope resolves its qualifier by prefix. Build the
        // qualified index only when independently qualified sources meet.
        if (!MixedQualifiers && !Columns.empty() && column.Qualifier != Columns.front().Qualifier) {
            MixedQualifiers = true;
            Names.reserve(Columns.capacity() * 2);
            for (std::size_t i = 0; i < Columns.size(); ++i) if (!Columns[i].Qualifier.empty())
                AddName(Columns[i].Qualifier + "." + Columns[i].Name, i);
            if (!Columns.front().Qualifier.empty()) Qualifiers.insert(Columns.front().Qualifier);
        }
        if (!column.QualifiedOnly) AddName(column.Name, index);
        if (MixedQualifiers && !column.Qualifier.empty()) {
            AddName(column.Qualifier + "." + column.Name, index);
            Qualifiers.insert(column.Qualifier);
        }
        Columns.push_back(std::move(column));
    }

    // A USING key has one unqualified binding, while each input keeps its
    // qualified binding and qualified-star projection (including NULL padding).
    void Merge(TScopeColumn column) {
        MaterializeIndex();
        for (auto& existing : Columns) if (existing.Name == column.Name) existing.QualifiedOnly = true;
        Names.erase(column.Name);
        Add(std::move(column));
    }

    const TScopeColumn* Find(std::string_view name, TSourcePosition pos) const {
        const auto index = FindIndex(name);
        if (index == Ambiguous) throw TFrontendError(pos, "ambiguous column: " + std::string(name));
        return index == Missing ? nullptr : &Columns[index];
    }
    // Identity comparison only merges names whose local binding is known.
    // Ordinary expression binding still diagnoses absent or ambiguous names.
    const TScopeColumn* FindUnambiguous(std::string_view name) const {
        const auto index = FindIndex(name);
        return index == Missing || index == Ambiguous ? nullptr : &Columns[index];
    }
    bool HasQualifier(std::string_view name) const {
        return MixedQualifiers ? Qualifiers.contains(name) : !Columns.empty() && name == Columns.front().Qualifier;
    }
    TPath FindPath(std::string_view name, std::span<const std::uint32_t> separators,
        TSourcePosition pos, bool diagnose = true) const {
        const auto first = name.substr(0, separators.empty() ? name.size() : separators.front());
        const bool qualified = !separators.empty() && HasQualifier(first);
        const auto end = qualified && separators.size() > 1 ? separators[1] :
            qualified ? name.size() : first.size();
        if (diagnose) CheckColumnProfile(qualified ? name.substr(first.size() + 1, end - first.size() - 1) : first, pos);
        // Preserve the existing ambiguity between a qualified column and a
        // literal dotted column. Member boundaries still come from syntax.
        const auto index = qualified ? FindIndex(name.substr(0, end)) : FindName(first);
        if (index == Ambiguous && diagnose) throw TFrontendError(pos, "ambiguous column: " + std::string(name));
        return {index == Missing || index == Ambiguous ? nullptr : &Columns[index], qualified ? 1u : 0u,
            qualified || index == Ambiguous};
    }

    std::vector<TColumnId> Ids() const {
        std::vector<TColumnId> result;
        result.reserve(Columns.size());
        for (const auto& column : Columns) result.push_back(column.Id);
        return result;
    }

    std::vector<TScopeColumn> Columns;
private:
    static constexpr std::size_t Ambiguous = std::numeric_limits<std::size_t>::max();
    static constexpr std::size_t Missing = Ambiguous - 1;

    template<class T>
    void AddName(T&& name, std::size_t index) {
        auto [it, inserted] = Names.try_emplace(std::forward<T>(name), index);
        if (!inserted) it->second = Ambiguous;
    }

    void MaterializeIndex() {
        if (CatalogOrder.empty()) return;
        Names.reserve(Columns.capacity());
        for (std::size_t i = 0; i < Columns.size(); ++i) AddName(Columns[i].Name, i);
        CatalogOrder = {};
    }

    std::size_t FindName(std::string_view name) const {
        if (!CatalogOrder.empty()) {
            const auto found = std::lower_bound(CatalogOrder.begin(), CatalogOrder.end(), name,
                [&](std::size_t index, std::string_view key) { return Columns[index].Name < key; });
            return found != CatalogOrder.end() && Columns[*found].Name == name ? *found : Missing;
        }
        const auto found = Names.find(name);
        return found == Names.end() ? Missing : found->second;
    }

    std::size_t FindIndex(std::string_view name) const {
        auto index = FindName(name);
        if (!MixedQualifiers && !Columns.empty()) {
            const auto& qualifier = Columns.front().Qualifier;
            if (!qualifier.empty() && name.size() > qualifier.size() && name.starts_with(qualifier) && name[qualifier.size()] == '.') {
                const auto qualified = FindName(name.substr(qualifier.size() + 1));
                // A literal dotted column name may collide with a qualified
                // reference. Keep the same ambiguity as the general index.
                if (qualified != Missing) index = index == Missing ? qualified : Ambiguous;
            }
        }
        return index;
    }

    bool MixedQualifiers = false;
    std::span<const std::size_t> CatalogOrder;
    std::unordered_map<std::string, std::size_t, TStringHash, std::equal_to<>> Names;
    std::unordered_set<std::string, TStringHash, std::equal_to<>> Qualifiers;
};

// Reserve explicit and star names before assigning anonymous projection names.
// Result naming is independent of scalar typing and never changes column IDs.
std::vector<std::string> ProjectionNames(const TSelect& query, const TParsedQuery& syntax, const TScope& scope) {
    std::vector<std::string> result(query.Projection.size());
    // A reserved star name may also be projected explicitly; two explicit
    // results with the same name remain an error.
    std::unordered_map<std::string, bool> names;
    names.reserve(query.Projection.size());
    for (std::size_t i = 0; i < query.Projection.size(); ++i) {
        const auto& projection = query.Projection[i];
        const auto& expression = syntax.Expressions[projection.Expression];
        if (expression.Kind == ESyntaxKind::Star) {
            for (const auto& column : scope.Columns)
                if (column.MatchesStar(expression.Text)) names.try_emplace(column.Name, false);
            continue;
        }
        auto& name = result[i];
        name = projection.Alias;
        if (name.empty() && expression.Kind == ESyntaxKind::Column) {
            const auto separators = syntax.NameSeparators(expression);
            // A struct member is a computed expression, so its default name
            // follows the same columnN convention as a postfix Member node.
            bool member = separators.size() > 1;
            if (separators.size() == 1 && !scope.HasQualifier(std::string_view(expression.Text).substr(0, separators.front())))
                member = scope.FindPath(expression.Text, separators, expression.Position, false).Column != nullptr;
            if (!member)
                name = query.Joins.empty() && !separators.empty() ? expression.Text.substr(separators.back() + 1) : expression.Text;
        }
        if (!name.empty()) {
            auto [entry, inserted] = names.try_emplace(name, true);
            if (!inserted && entry->second) throw TFrontendError(expression.Position, "duplicate output column: " + name);
            entry->second = true;
        }
    }
    for (std::size_t i = 0; i < result.size(); ++i) {
        if (!result[i].empty() || syntax.Expressions[query.Projection[i].Expression].Kind == ESyntaxKind::Star) continue;
        for (std::size_t suffix = i;; ++suffix) {
            auto name = "column" + std::to_string(suffix);
            if (names.try_emplace(name, true).second) { result[i] = std::move(name); break; }
        }
    }
    return result;
}

struct TBoundSource {
    TRelId Relation;
    TScope Scope;
};

struct TSyntaxKey {
    ESyntaxKind Kind;
    std::string Text;
    std::vector<std::uint32_t> Arguments;
    bool Distinct;
    std::uint32_t Binding;
    const TSelect* Query;
    std::uint32_t Window = NoSyntax;
    std::uint8_t Options = 0;
    std::uint32_t Filter = NoSyntax;
    bool operator==(const TSyntaxKey&) const = default;
};

struct TSyntaxHash {
    std::size_t operator()(const TSyntaxKey& key) const {
        std::size_t value = std::hash<std::string>{}(key.Text) ^ static_cast<unsigned>(key.Kind) ^ key.Distinct ^ key.Binding;
        value ^= std::hash<const TSelect*>{}(key.Query) ^ key.Window ^ key.Options ^ key.Filter;
        for (auto arg : key.Arguments) value ^= arg + 0x9e3779b9 + (value << 6) + (value >> 2);
        return value;
    }
};

struct TWindowKeyHash {
    std::size_t operator()(const std::vector<std::uint32_t>& key) const {
        std::size_t value = 0;
        for (auto part : key) value ^= part + 0x9e3779b9 + (value << 6) + (value >> 2);
        return value;
    }
};

// Grouping equivalence uses resolved input identities: t.id and id match,
// while l.id and r.id in a self join remain distinct. The cache belongs to one
// immutable source scope and visits each referenced syntax node once.
class TScopedSyntaxIdentity {
public:
    TScopedSyntaxIdentity(const TParsedQuery& syntax, const TGlobalBindings& bindings,
        const TScope& scope, const std::vector<std::uint32_t>& windows)
        : Syntax(syntax), Bindings(bindings), Scope(scope), Windows(windows) {}

    std::uint32_t Get(TSyntaxId id) {
        if (const auto found = Cache.find(id); found != Cache.end()) return found->second;
        const auto& node = Syntax.Expressions[id];
        const bool normalize = (node.Kind == ESyntaxKind::Call && !node.Text.starts_with('$')) || node.Kind == ESyntaxKind::Cast || node.Kind == ESyntaxKind::BitCast;
        TSyntaxKey key{node.Kind, normalize ? Lower(node.Text) : node.Text, {}, node.Distinct, Bindings[id], node.Query.get(), Windows[id]};
        key.Options = Bindings.Flags(id);
        if (node.Lambda) key.Binding = id;
        if (node.Kind == ESyntaxKind::Column) {
            const auto separators = Syntax.NameSeparators(node);
            const auto path = Scope.FindPath(node.Text, separators, node.Position, false);
            std::size_t begin = 0, member = 0;
            if (path.Column) {
                key.Binding = path.Column->Id;
                member = path.Members;
                begin = member < separators.size() ? separators[member++] + 1 : node.Text.size();
                key.Text = node.Text.substr(begin);
            }
            for (; member < separators.size(); ++member) key.Arguments.push_back(separators[member] - begin);
        }
        for (const auto argument : node.Arguments) key.Arguments.push_back(Get(argument));
        if (node.Filter != NoSyntax) key.Filter = Get(node.Filter);
        const auto value = Interned.emplace(std::move(key), Interned.size()).first->second;
        Cache.emplace(id, value);
        return value;
    }
private:
    const TParsedQuery& Syntax;
    const TGlobalBindings& Bindings;
    const TScope& Scope;
    const std::vector<std::uint32_t>& Windows;
    std::unordered_map<TSyntaxId, std::uint32_t> Cache;
    std::unordered_map<TSyntaxKey, std::uint32_t, TSyntaxHash> Interned;
};

struct TProjectionAlias {
    TExprId Expression;
    bool HasWindow;
};

struct TExprContext;

struct TAggregationContext {
    std::optional<TScopedSyntaxIdentity> Identity;
    TExprContext* Source = nullptr;
    std::vector<TDefinition> Definitions;
    std::vector<TColumnId> Keys;
    std::vector<TAggregate> Aggregates;
    std::unordered_map<std::uint32_t, TScopeColumn> Groups;
    std::unordered_map<std::uint32_t, TExprId> Calls;
    TGroupingSets GroupingSets;
    std::vector<std::pair<TColumnId, TColumnId>> GroupingColumns;
    std::vector<std::pair<TColumnId, TColumnId>> GroupingIndicators;
    std::unordered_map<TColumnId, TColumnId> GroupSources;
    std::unordered_map<TColumnId, TColumnId> Indicators;
    bool MayBeEmpty = false;
};

struct TWindowContext {
    TExprContext* Source = nullptr;
    std::vector<TDefinition> Definitions;
    std::vector<TRelation> Groups;
    // Argument and FILTER identities jointly determine the materialized input.
    std::unordered_map<std::uint64_t, TColumnId> Inputs;
    std::unordered_map<TColumnId, TType> InputTypes;
    std::unordered_map<std::uint32_t, TExprId> Calls;
    std::unordered_map<std::uint32_t, std::size_t> Specifications;
};

// Closures exist only while binding. Frames are query-owned and referenced by
// index, so a returned closure retains its lexical values without copying an
// environment or creating shared-ownership cycles. RBO receives scalars only.
inline constexpr std::size_t NoBindingFrame = std::numeric_limits<std::size_t>::max();
struct TBoundLambda {
    TSyntaxId Syntax;
    std::size_t Frame;
};
struct TBoundRegex {
    TExprId Pattern;
    bool FullMatch;
};
using TBindingValue = std::variant<TExprId, TBoundLambda, std::string, TBoundRegex, TGlobalBindings::TExternal>;
struct TBindingFrame {
    std::size_t Parent = NoBindingFrame;
    std::unordered_map<std::uint32_t, TBindingValue> Values;
};

struct TExprContext {
    TExprContext(const TScope& scope, TAggregationContext* aggregation = nullptr)
        : Scope(scope), Aggregation(aggregation) {}
    const TScope& Scope;
    const TScope* Aliases = nullptr;
    const std::unordered_map<std::string, TProjectionAlias>* ProjectionAliases = nullptr;
    TAggregationContext* Aggregation = nullptr;
    std::size_t BindingFrame = NoBindingFrame;
    std::unordered_map<TSyntaxId, TExprId> Cache;
    TExprContext* Outer = nullptr;
    TExprContext* CaptureOwner = nullptr;
    std::unordered_map<TColumnId, TColumnId> Captured;
    std::vector<TCapture> PendingCaptures;
    TRelId CaptureRelation = NoRelation;
    std::vector<TDefinition> PendingDefinitions;
    bool AllowSubqueries = true;
    TWindowContext* Windows = nullptr;
};

class TBinder {
public:
    TBinder(const TParsedQuery& syntax, const TTableResolver& catalog, const TBindSettings& settings,
        const TGlobalBindings& bindings)
        : Syntax(syntax), Catalog(catalog), Settings(settings), Bindings(bindings),
          Values(syntax.Statements.size() + bindings.ExternalValues().size())
    {
        for (std::size_t i = 0; i < bindings.ExternalValues().size(); ++i)
            Values[syntax.Statements.size() + i] = bindings.ExternalValues()[i];
        Program.DefaultCluster = settings.DefaultCluster;
        Program.Parameters = settings.ParameterTypes;
        for (const auto& [name, type] : Program.Parameters) ValidateParameter(type, {});
        // Structural interning is used by window input/specification sharing.
        // Grouping has its own lazy identity cache tied to the resolved scope.
        const bool hasWindows = std::any_of(syntax.Expressions.begin(), syntax.Expressions.end(),
            [](const auto& expression) { return bool(expression.Window); });
        if (hasWindows) Canonical.reserve(syntax.Expressions.size());
        CanonicalWindows.resize(syntax.Expressions.size(), NoSyntax);
        ContainsAggregate.reserve(syntax.Expressions.size());
        ContainsWindow.reserve(syntax.Expressions.size());
        std::unordered_map<TSyntaxKey, std::uint32_t, TSyntaxHash> interned;
        std::unordered_map<std::vector<std::uint32_t>, std::uint32_t, TWindowKeyHash> windows;
        for (std::size_t i = 0; i < syntax.Expressions.size(); ++i) {
            const auto& expr = syntax.Expressions[i];
            if (Bindings.IsReferenced(i) && (expr.Kind == ESyntaxKind::Variable || (expr.Kind == ESyntaxKind::Call && expr.Text.starts_with('$'))) &&
                Bindings[i] == TGlobalBindings::Unknown)
                throw TFrontendError(expr.Position, "unknown binding: " + expr.Text);
            bool window = bool(expr.Window);
            const auto call = expr.Kind == ESyntaxKind::Call ? Lower(expr.Text) : std::string{};
            bool aggregate = expr.Kind == ESyntaxKind::Call && !window && (IsAggregate(call) || call == "grouping");
            for (auto child : expr.Arguments) {
                if (child >= i) throw TFrontendError(expr.Position, "syntax tree is not topologically ordered");
                aggregate = aggregate || ContainsAggregate[child];
                window = window || ContainsWindow[child];
            }
            if (expr.Filter != NoSyntax) {
                if (expr.Filter >= i) throw TFrontendError(expr.Position, "filter syntax is not topologically ordered");
                aggregate = aggregate || ContainsAggregate[expr.Filter];
                window = window || ContainsWindow[expr.Filter];
            }
            if (hasWindows) {
                const auto normalized = (expr.Kind == ESyntaxKind::Call && !expr.Text.starts_with('$')) || expr.Kind == ESyntaxKind::Cast || expr.Kind == ESyntaxKind::BitCast;
                TSyntaxKey key{expr.Kind, normalized ? Lower(expr.Text) : expr.Text, {}, expr.Distinct, Bindings[i], expr.Query.get()};
                key.Options = Bindings.Flags(i);
                if (expr.Lambda) key.Binding = i;
                for (auto child : expr.Arguments) key.Arguments.push_back(Canonical[child]);
                if (expr.Filter != NoSyntax) key.Filter = Canonical[expr.Filter];
                if (expr.Window) {
                    const auto& w = Bindings.Window(*expr.Window);
                    auto canonical = [&](TSyntaxId child) {
                        if (child == NoSyntax) return NoSyntax;
                        if (child >= i) throw TFrontendError(expr.Position, "window syntax is not topologically ordered");
                        aggregate = aggregate || ContainsAggregate[child];
                        return Canonical[child];
                    };
                    std::vector<std::uint32_t> spec{static_cast<std::uint32_t>(w.Frame.Type), static_cast<std::uint32_t>(w.Frame.Begin),
                        static_cast<std::uint32_t>(w.Frame.End), canonical(w.BeginOffset), canonical(w.EndOffset), static_cast<std::uint32_t>(w.Partition.size())};
                    for (const auto& child : w.Partition) spec.push_back(canonical(child.Expression));
                    for (const auto& child : w.Order) { spec.push_back(canonical(child.Expression)); spec.push_back(child.Descending); }
                    key.Window = windows.emplace(std::move(spec), windows.size()).first->second;
                }
                CanonicalWindows[i] = key.Window;
                const auto entry = interned.emplace(std::move(key), static_cast<std::uint32_t>(interned.size())).first;
                Canonical.push_back(entry->second);
            }
            ContainsAggregate.push_back(aggregate);
            ContainsWindow.push_back(window);
        }
    }

    std::pair<TBoundProgram, TExprId> RunExpression(TSyntaxId root, std::span<const TCatalogColumn> columns, TType target) {
        TScope scope;
        scope.Reserve(columns.size());
        const auto pos = Syntax.Expressions.at(root).Position;
        for (const auto& column : columns) {
            if (scope.FindUnambiguous(column.Name)) throw TFrontendError(pos, "duplicate expression input: " + column.Name);
            scope.Add(ScopeColumn(Column(column.Name, {}, column.Type, pos)));
        }
        TExprContext context{scope};
        context.AllowSubqueries = false;
        const auto value = Program.Expressions.Convert(Expr(root, context), target, pos);
        CheckSize(pos);
        return {std::move(Program), value};
    }

    TBoundProgram Run() {
        bool orderedColumns = true;
        const auto sortPresentation = [](TQueryResult& result) {
            std::stable_sort(result.Columns.begin(), result.Columns.end(), [](const auto& a, const auto& b) { return a.Name < b.Name; });
        };
        for (std::size_t index = 0; index < Syntax.Statements.size(); ++index) {
            const auto& statement = Syntax.Statements[index];
            switch (statement.Kind) {
                case EStatementKind::Query: {
                    auto result = Select(*statement.Query);
                    // Presentation is distinct from a source's column order:
                    // aliases and nested queries retain their bound schemas.
                    const auto& projection = statement.Query->Projection;
                    if (!orderedColumns && projection.size() == 1 && Syntax.Expressions[projection[0].Expression].Kind == ESyntaxKind::Star)
                        sortPresentation(result);
                    Program.Results.push_back(std::move(result));
                    break;
                }
                case EStatementKind::Write: {
                    auto result = Write(*statement.Write);
                    if (!orderedColumns && statement.Write->ReturningAll) sortPresentation(result);
                    Program.Results.push_back(std::move(result));
                    break;
                }
                case EStatementKind::Declaration: {
                    if (statement.Name == "_") throw TFrontendError(statement.Position, "DECLARE requires a named parameter");
                    // SQL keeps the first declaration of a parameter, even if
                    // a later declaration spells a different type.
                    if (Declared.contains(statement.Name)) break;
                    TParameterType type;
                    if (statement.RowFields) {
                        TParameterType::TRows fields;
                        for (const auto& [name, value] : *statement.RowFields) fields.push_back({name, ParseType(value, statement.Position)});
                        type = TParameterType(std::move(fields));
                    } else if (statement.ScalarList) type = TParameterType::TList{ParseType(statement.Value, statement.Position)};
                    else type = ParseType(statement.Value, statement.Position);
                    ValidateParameter(type, statement.Position);
                    auto [it, inserted] = Program.Parameters.emplace(statement.Name, type);
                    if (!inserted && it->second != type) throw TFrontendError(statement.Position, "parameter type does not match DECLARE: " + statement.Name);
                    Declared.insert(statement.Name);
                    break;
                }
                case EStatementKind::Binding: {
                    if (!Bindings.IsQueryBinding(index)) {
                        const TScope empty;
                        TExprContext context{empty};
                        context.AllowSubqueries = false;
                        Values[index] = BindingValue(statement.Expression, context);
                    }
                    break;
                }
                case EStatementKind::Use:
                    throw TUnsupportedFeature(EFeature::Statement, statement.Position, "USE is not supported; resolve table paths through the catalog");
                case EStatementKind::Pragma: {
                    const auto name = Lower(statement.Name);
                    if (Bindings.IsCapturedPragma(index)) { /* Applied at each definition site. */ }
                    else if (name == "warning") CheckWarningPragma(statement);
                    else if (name == "yqlselect" && Lower(PragmaValue(statement)) == "force") {
                        // This selects relational translation, which is intrinsic here.
                    } else if (name == "ansiimplicitcrossjoin" && statement.PragmaValues.empty()) {
                        // Comma joins have one unambiguous cross-product meaning.
                    } else if ((name == "orderedcolumns" || name == "disableorderedcolumns") && statement.PragmaValues.empty()) {
                        orderedColumns = name == "orderedcolumns";
                    } else if (auto setting = PlannerSetting(statement)) Program.PlannerSettings.push_back(std::move(*setting));
                    else throw TUnsupportedFeature(EFeature::Pragma, statement.Position, "unsupported pragma: " + statement.Name);
                    break;
                }
            }
            CheckSize(statement.Position);
        }
        if (Program.Results.empty()) throw TUnsupportedFeature(EFeature::Statement, {}, "native queries require a result or effect statement");
        return std::move(Program);
    }

private:
    void ValidateParameter(const TParameterType& type, TSourcePosition pos) const {
        if (const auto* scalar = type.Scalar()) { Program.Expressions.ValidateType(*scalar, pos); return; }
        if (const auto* list = type.ScalarList()) {
            Program.Expressions.ValidateType(list->Item, pos);
            if (list->Item.IsCollection() || list->Item.Kind == ETypeKind::Calendar || list->Item.IsDocument())
                throw TUnsupportedFeature(EFeature::Type, pos, "list parameter items require ordinary scalar types");
            return;
        }
        const auto& rows = *type.Rows();
        if (rows.size() > Settings.MaxNodes) throw TResourceLimitError(pos, "parameter field limit exceeded");
        for (std::size_t i = 0; i < rows.size(); ++i) {
            Program.Expressions.ValidateType(rows[i].Type, pos);
            if (rows[i].Type.Kind == ETypeKind::Tuple || rows[i].Type.Kind == ETypeKind::Calendar)
                throw TUnsupportedFeature(EFeature::Type, pos, "parameter rows require scalar fields");
            if (i && rows[i - 1].Name == rows[i].Name) throw TFrontendError(pos, "duplicate parameter field: " + rows[i].Name);
        }
    }
    struct TDepth {
        TBinder& Binder;
        TDepth(TBinder& binder, TSourcePosition pos) : Binder(binder) {
            if (++Binder.Depth > Binder.Settings.MaxExpansionDepth) throw TResourceLimitError(pos, "binding expansion limit exceeded");
        }
        ~TDepth() { --Binder.Depth; }
    };

    void CheckSize(TSourcePosition pos) const {
        if (Program.Expressions.Size() + Program.Relations.size() + Program.Columns.size() + Program.PlannerSettings.size() + GeneratedNodes > Settings.MaxNodes) {
            throw TResourceLimitError(pos, "bound program size limit exceeded");
        }
    }
    TColumnId Column(std::string name, std::string qualifier, TType type, TSourcePosition pos) {
        CheckSize(pos);
        auto id = static_cast<TColumnId>(Program.Columns.size());
        ColumnOrigins.push_back(id);
        Program.Columns.push_back({std::move(name), std::move(qualifier), type}); return id;
    }
    TRelId Relation(TRelation relation) {
        CheckSize(relation.Position);
        auto id = static_cast<TRelId>(Program.Relations.size()); Program.Relations.push_back(std::move(relation)); return id;
    }
    TScopeColumn ScopeColumn(TColumnId id) const {
        const auto& column = Program.Columns.at(id); return {id, column.Name, column.Qualifier, column.Type};
    }
    TExprId Access(const TScopeColumn& column, TSourcePosition pos) {
        return Program.Expressions.Column(column.Id, column.Type, pos);
    }
    struct TResolvedPath { TScopeColumn Column; std::size_t Members; };
    std::optional<TResolvedPath> ResolveColumn(TExprContext& context, const TSyntaxExpr& syntax) {
        const auto separators = Syntax.NameSeparators(syntax);
        const auto path = context.Scope.FindPath(syntax.Text, separators, syntax.Position);
        if (path.Column) return TResolvedPath{*path.Column, path.Members};
        if (path.Shadowed || (context.Aggregation &&
                context.Aggregation->Source->Scope.FindPath(syntax.Text, separators, syntax.Position).Column))
            throw TFrontendError(syntax.Position, "unknown or ungrouped column: " + syntax.Text);
        if (!context.Outer) return {};
        const auto outer = ResolveColumn(*context.Outer, syntax);
        if (!outer) return {};
        if (context.Aggregation) throw TUnsupportedFeature(EFeature::Subquery, syntax.Position,
            "outer references after aggregation require explicit grouping in the native profile");
        auto& captures = context.CaptureOwner ? *context.CaptureOwner : context;
        const auto& column = outer->Column;
        auto [it, inserted] = captures.Captured.try_emplace(column.Id);
        if (inserted) {
            it->second = Column(column.Name, column.Qualifier, column.Type, syntax.Position);
            captures.PendingCaptures.push_back({it->second, column.Id});
        }
        return TResolvedPath{ScopeColumn(it->second), outer->Members};
    }
    TExprId AccessMembers(TExprId value, const TSyntaxExpr& syntax, std::size_t member) {
        const auto separators = Syntax.NameSeparators(syntax);
        for (; member < separators.size(); ++member) {
            const auto begin = separators[member] + 1;
            const auto end = member + 1 < separators.size() ? separators[member + 1] : syntax.Text.size();
            value = Program.Expressions.Member(value, std::string_view(syntax.Text).substr(begin, end - begin), syntax.Position);
        }
        return value;
    }
    TRelId BindInput(TRelId input, TExprContext& context, TSourcePosition pos) {
        if (!context.PendingCaptures.empty()) {
            if (context.CaptureRelation == NoRelation) {
                TRelation capture{ERelation::Capture, pos};
                capture.Inputs = {input};
                capture.Columns = Program.Relations[input].Columns;
                context.CaptureRelation = Relation(std::move(capture));
                input = context.CaptureRelation;
            }
            // WHERE, aggregate arguments and projections share the query's
            // capture scope. Add later references at the same input boundary,
            // so decorrelation sees one complete set of free variables.
            for (auto id = input;; id = Program.Relations[id].Inputs.at(0)) {
                auto& relation = Program.Relations[id];
                for (const auto& binding : context.PendingCaptures) relation.Columns.push_back(binding.Local);
                if (id == context.CaptureRelation) {
                    relation.Captures.insert(relation.Captures.end(), context.PendingCaptures.begin(), context.PendingCaptures.end());
                    break;
                }
                if (relation.Inputs.size() != 1)
                    throw TFrontendError(pos, "capture scope must have a single input");
            }
            context.PendingCaptures.clear();
        }
        // Nested IN lookups can depend on earlier materializations. Each Map
        // sees its input's definitions; sibling Map elements are independent.
        for (const auto& definition : std::exchange(context.PendingDefinitions, {}))
            input = Map(input, {definition}, pos);
        return input;
    }
    bool HasExternalCaptures(std::size_t firstRelation, const TExprContext* outer) const {
        std::unordered_set<TColumnId> external;
        for (auto* context = outer; context; context = context->Outer) {
            for (const auto& column : context->Scope.Columns) external.insert(column.Id);
            const auto& captures = context->CaptureOwner ? *context->CaptureOwner : *context;
            for (const auto& [source, local] : captures.Captured) external.insert(local);
        }
        for (auto i = firstRelation; i < Program.Relations.size(); ++i)
            for (const auto& capture : Program.Relations[i].Captures)
                if (external.contains(capture.Outer)) return true;
        return false;
    }
    TQueryResult Subquery(const TSelect& syntax, TExprContext& outer) {
        const auto firstRelation = Program.Relations.size();
        auto result = Select(syntax, &outer);
        const bool hasWindow = std::any_of(Program.Relations.begin() + firstRelation, Program.Relations.end(),
            [](const auto& relation) { return relation.Kind == ERelation::Window; });
        if (hasWindow && HasExternalCaptures(firstRelation, &outer))
            throw TUnsupportedFeature(EFeature::Backend, syntax.Position,
                "correlated window subqueries require backend window decorrelation");
        return result;
    }
    TColumnId Materialize(TExprId expr, std::vector<TDefinition>& definitions, TSourcePosition pos, std::string name = {}) {
        const auto& node = Program.Expressions[expr];
        if (node.Op == EOpcode::Column && !Program.Subplans.contains(node.ColumnId)) return node.ColumnId;
        if (name.empty()) name = "expr" + std::to_string(Program.Columns.size());
        auto column = Column(std::move(name), {}, node.Type, pos);
        definitions.push_back({column, expr}); return column;
    }
    TRelId Map(TRelId input, std::vector<TDefinition> definitions, TSourcePosition pos) {
        if (definitions.empty()) return input;
        TRelation map{ERelation::Map, pos}; map.Inputs = {input}; map.Columns = Program.Relations[input].Columns;
        for (const auto& definition : definitions) {
            map.Columns.push_back(definition.Column);
            const auto& expression = Program.Expressions[definition.Expression];
            if (expression.Op == EOpcode::Column) ColumnOrigins[definition.Column] = ColumnOrigins[expression.ColumnId];
        }
        map.Definitions = std::move(definitions); return Relation(std::move(map));
    }
    TRelId Filter(TRelId input, TExprId predicate, TSourcePosition pos) {
        const auto type = Program.Expressions[predicate].Type;
        if (type.Kind != ETypeKind::Bool && type.Kind != ETypeKind::Null) throw TFrontendError(pos, "predicate must have Boolean type");
        if (type.OptionalDepth > 1) throw TFrontendError(pos, "predicate must have Bool or Bool? type");
        if (type.Kind == ETypeKind::Null) predicate = Program.Expressions.Literal({ETypeKind::Bool}, false, pos);
        TRelation filter{ERelation::Filter, pos}; filter.Inputs = {input}; filter.Columns = Program.Relations[input].Columns;
        filter.Predicate = PrepareFilterPredicate(Program.Expressions, predicate); return Relation(std::move(filter));
    }

    TBoundSource Source(const TSource& source, TExprContext* outer) {
        auto result = SourceInput(source, outer);
        if (source.ColumnAliases.empty()) return result;
        if (source.ColumnAliases.size() != result.Scope.Columns.size())
            throw TFrontendError(source.Position, "column alias count does not match source column count");
        // Rename visible bindings through an ordinary map. Stored read columns
        // retain their catalog names, and every alias gets a distinct identity.
        TRelation map{ERelation::Map, source.Position}; map.Inputs = {result.Relation};
        TScope renamed;
        std::unordered_set<std::string> names;
        for (std::size_t i = 0; i < source.ColumnAliases.size(); ++i) {
            const auto& name = source.ColumnAliases[i];
            if (!names.insert(name).second) throw TFrontendError(source.Position, "duplicate column alias: " + name);
            const auto& input = result.Scope.Columns[i];
            const auto id = Column(name, input.Qualifier, input.Type, source.Position);
            map.Columns.push_back(id);
            map.Definitions.push_back({id, Program.Expressions.Column(input.Id, input.Type, source.Position)});
            renamed.Add(ScopeColumn(id));
        }
        return {Relation(std::move(map)), std::move(renamed)};
    }

    TBoundSource SourceInput(const TSource& source, TExprContext* outer) {
        TDepth depth(*this, source.Position);
        if (source.ParameterTable) {
            const auto binding = Bindings.SourceBinding(source);
            if (binding == TGlobalBindings::Unknown) throw TFrontendError(source.Position, "unknown parameter: " + source.Name);
            if (binding != TGlobalBindings::Parameter)
                throw TUnsupportedFeature(EFeature::Statement, source.Position, "AS_TABLE requires a declared or supplied row-list parameter");
            const auto name = source.Name.substr(1);
            const auto* fields = Program.Parameters.at(name).Rows();
            if (!fields) throw TFrontendError(source.Position, "AS_TABLE requires a List<Struct<...>> parameter");
            TRelation input{ERelation::Parameter, source.Position}; input.ParameterName = name;
            TScope scope;
            for (const auto& field : *fields) {
                const auto id = Column(field.Name, source.Alias, field.Type, source.Position);
                input.Columns.push_back(id); scope.Add(ScopeColumn(id));
            }
            return {Relation(std::move(input)), std::move(scope)};
        }
        auto query = Bindings.Query(source);
        const auto* cte = Bindings.SourceCte(source);
        if (query) {
            if (source.Index) throw TFrontendError(source.Position, "index VIEW requires a persistent table");
            // Named statements keep their declaration scope. A CTE can
            // capture columns only from the query that declared it.
            auto* queryOuter = source.NameIsBinding ? nullptr : cte ? CteContexts.at(cte) : outer;
            const bool reusable = !source.Query && !queryOuter;
            auto result = BindQuery(*query, queryOuter, reusable, source.Position);
            if (cte && !cte->Columns.empty()) {
                if (cte->Columns.size() != result.Columns.size())
                    throw TFrontendError(source.Position, "CTE column alias count does not match projection");
                std::unordered_set<std::string> names;
                for (std::size_t i = 0; i < cte->Columns.size(); ++i) {
                    if (!names.insert(cte->Columns[i]).second) throw TFrontendError(source.Position, "duplicate CTE column alias: " + cte->Columns[i]);
                    result.Columns[i].Name = cte->Columns[i];
                }
            }
            if (reusable) result = ReuseQuery(std::move(result), source.Alias, source.Position);
            TScope scope;
            for (const auto& column : result.Columns) scope.Add({column.Column, column.Name, source.Alias.empty() ? source.Name : source.Alias, column.Type});
            return {result.Root, std::move(scope)};
        }
        if (source.NameIsBinding) throw TFrontendError(source.Position, "unknown query binding: " + source.Name);
        return ReadTable(source, ResolveTable(source));
    }

    TQueryResult BindQuery(const TSelect& query, TExprContext* outer, bool reusable, TSourcePosition pos) {
        if (!ActiveQueries.insert(&query).second) throw TFrontendError(pos, "recursive query binding is not supported");
        TQueryResult result;
        if (const auto found = ReusableQueries.find(&query); reusable && found != ReusableQueries.end()) {
            result = found->second;
        } else {
            result = Select(query, outer);
            if (reusable) ReusableQueries.emplace(&query, result);
        }
        ActiveQueries.erase(&query);
        return result;
    }

    // One producer has distinct column identities at each use site. The same
    // contract applies to FROM sources and named queries used by IN.
    TQueryResult ReuseQuery(TQueryResult result, const std::string& qualifier, TSourcePosition pos) {
        TRelation use{ERelation::Reuse, pos};
        use.Inputs = {result.Root};
        for (auto& column : result.Columns) {
            const auto id = Column(column.Name, qualifier, column.Type, pos);
            use.Columns.push_back(id);
            use.Definitions.push_back({id, Program.Expressions.Column(column.Column, column.Type, pos)});
            column.Column = id;
        }
        result.Root = Relation(std::move(use));
        return result;
    }

    const TCatalogSnapshot& ResolveTable(const TSource& source) {
        const auto& address = Bindings.PhysicalAddress(source);
        if (const auto found = Tables.find(address); found != Tables.end()) return found->second;
        auto snapshot = Catalog(address, source.Position);
        snapshot.Validate(source.Position);
        return Tables.emplace(address, std::move(snapshot)).first->second;
    }

    TBoundSource ReadTable(const TSource& source, const TCatalogSnapshot& snapshot) {
        const auto& table = snapshot.Get();
        TRelation read{ERelation::Read, source.Position}; read.Table = {Bindings.PhysicalAddress(source).Cluster, table.Name};
        read.Alias = source.Alias.empty() ? source.Name : source.Alias;
        read.Index = source.Index;
        std::string alias = source.Alias;
        if (alias.empty()) alias = source.Name.substr(source.Name.find_last_of("/.") + 1);
        std::vector<TScopeColumn> columns;
        columns.reserve(table.Columns.size());
        read.Columns.resize(table.Columns.size());
        const auto requiredColumns = Program.Columns.size() + table.Columns.size();
        if (requiredColumns > Program.Columns.capacity()) {
            // Grow once for this source, retaining geometric capacity for later
            // sources and projections instead of moving each intermediate prefix.
            const auto capacity = std::bit_ceil(requiredColumns);
            Program.Columns.reserve(capacity);
            ColumnOrigins.reserve(capacity);
        }
        // Allocate source identities in the existing importer's lexical order.
        // CBO's ordered join keys consume these identities. Visible bindings
        // retain catalog order independently, including SELECT * and aliases.
        for (auto index : snapshot.ColumnOrder()) {
            const auto& column = table.Columns[index];
            read.Columns[index] = Column(column.Name, alias, column.Type, source.Position);
        }
        for (auto id : read.Columns) columns.push_back(ScopeColumn(id));
        return {Relation(std::move(read)), TScope(std::move(columns), snapshot.ColumnOrder())};
    }

    const TGeneratedValues& GeneratedColumns(const TCatalogTable& table, TSourcePosition pos) {
        const auto& schema = *table.WriteSchema;
        static const TGeneratedValues empty;
        if (schema.GeneratedColumns.empty()) return empty;
        if (const auto found = Generated.find(&table); found != Generated.end()) return found->second;
        std::unordered_set<std::string_view> names;
        for (const auto& column : schema.GeneratedColumns) {
            if (!names.insert(column.Name).second) throw TFrontendError(pos, "duplicate generated column: " + column.Name);
            if (!column.Stored) throw TUnsupportedFeature(EFeature::Statement, pos, "virtual generated columns require native read projection");
            if (std::find(schema.Keys.begin(), schema.Keys.end(), column.Name) != schema.Keys.end())
                throw TFrontendError(pos, "primary key cannot be a generated column: " + column.Name);
        }
        for (const auto& [name, kind] : schema.Defaults) if (names.contains(name))
            throw TFrontendError(pos, "generated column cannot also have a default: " + name);
        std::vector<TCatalogColumn> inputs;
        for (const auto& column : table.Columns) if (!names.contains(column.Name)) inputs.push_back(column);
        TGeneratedValues values;
        for (const auto& column : schema.GeneratedColumns) {
            const auto target = std::find_if(table.Columns.begin(), table.Columns.end(),
                [&](const auto& item) { return item.Name == column.Name; });
            if (target == table.Columns.end()) throw TFrontendError(pos, "unknown generated column: " + column.Name);
            TParseSettings limits;
            limits.MaxNodes = Settings.MaxNodes;
            auto value = CompileRowExpression(column.Expression, inputs, target->Type, limits);
            if (!value.Expressions[value.Root].Repeatable)
                throw TFrontendError(pos, "generated expressions must be deterministic: " + column.Name);
            GeneratedNodes += value.Expressions.Size();
            CheckSize(pos);
            values.emplace_back(column.Name, std::make_shared<const TRowExpression>(std::move(value)));
        }
        return Generated.emplace(&table, std::move(values)).first->second;
    }

    static void PrepareGeneratedColumns(const TGeneratedValues& generated,
        const std::unordered_set<std::string>& written, const std::unordered_set<std::string>& keys,
        TBoundEffect& effect, TSourcePosition pos) {
        if (effect.Kind != EWriteKind::Delete) for (const auto& [name, value] : generated) {
            // UPDATE changes generated values only when one of their inputs
            // changes. Primary keys identify rows and cannot change here.
            if (effect.Kind == EWriteKind::Update && std::none_of(value->Dependencies.begin(), value->Dependencies.end(),
                    [&](const auto dependency) {
                        const auto& key = value->Columns[dependency];
                        return written.contains(key) && !keys.contains(key);
                    })) continue;
            for (const auto dependency : value->Dependencies) {
                const auto& key = value->Columns[dependency];
                const bool defaultOnInsert = effect.Kind == EWriteKind::Upsert && !keys.contains(key) &&
                    std::find(effect.DefaultColumns.begin(), effect.DefaultColumns.end(), key) != effect.DefaultColumns.end();
                if (!written.contains(key) || defaultOnInsert)
                    throw TUnsupportedFeature(EFeature::Statement, pos, "generated expression requires stored dependency lookup: " + key);
            }
            effect.GeneratedColumns.emplace_back(name, value);
        }
    }

    TQueryResult Write(const TWriteStatement& write) {
        const auto pos = write.Target.Position;
        const auto& snapshot = ResolveTable(write.Target);
        const auto& table = snapshot.Get();
        if (!table.WriteSchema) {
            auto message = "table does not support native writes: " + table.Name;
            if (!table.WriteUnsupportedReason.empty()) message += "; " + table.WriteUnsupportedReason;
            throw TUnsupportedFeature(EFeature::Statement, pos, std::move(message));
        }
        if (write.Batch)
            throw TUnsupportedFeature(EFeature::Statement, pos, "batch writes require a batch execution plan");
        const auto& schema = *table.WriteSchema;
        std::unordered_map<std::string, TType> types;
        for (const auto& column : table.Columns) {
            Program.Expressions.ValidateType(column.Type, pos);
            types.emplace(column.Name, column.Type);
        }
        auto columnType = [&](const std::string& name) {
            const auto found = types.find(name);
            if (found == types.end()) throw TFrontendError(pos, "Column not found: " + name);
            return found->second;
        };
        const std::unordered_set<std::string> keys(schema.Keys.begin(), schema.Keys.end());
        if (keys.empty()) throw TFrontendError(pos, "write catalog has no primary key");
        for (const auto& name : schema.Keys) columnType(name);
        for (const auto& [name, kind] : schema.Defaults) columnType(name);
        const auto& generated = GeneratedColumns(table, pos);
        auto checkExplicit = [&](const std::string& name) {
            for (const auto& [column, value] : generated) if (name == column)
                throw TFrontendError(pos, "Column " + name + " is a GENERATED ALWAYS column and its value cannot be set explicitly");
        };

        TBoundEffect effect{};
        effect.Kind = write.Kind;
        effect.HasIndexes = schema.HasIndexes;
        effect.InputRows = write.Input || !write.Rows.empty();
        effect.OnConflict = write.OnConflict;
        std::unordered_set<std::string> written;
        std::vector<TDefinition> definitions;
        auto add = [&](const std::string& name, TExprId value) {
            const auto type = columnType(name);
            checkExplicit(name);
            if (!written.insert(name).second) throw TFrontendError(pos, "duplicate write column: " + name);
            value = Program.Expressions.Convert(value, type, pos);
            const auto id = Column(name, {}, type, pos);
            definitions.push_back({id, value});
            effect.Columns.emplace_back(id, name);
        };
        TRelId input;
        if (write.Input) {
            auto query = Select(*write.Input);
            if (!write.Columns.empty() && write.Columns.size() != query.Columns.size())
                throw TFrontendError(pos, "write column count does not match SELECT output");
            for (std::size_t i = 0; i < query.Columns.size(); ++i) {
                const auto& column = query.Columns[i];
                add(write.Columns.empty() ? column.Name : write.Columns[i],
                    Program.Expressions.Column(column.Column, column.Type, pos));
            }
            input = query.Root;
        } else if (!write.Rows.empty()) {
            if (write.Columns.empty()) throw TFrontendError(pos, "VALUES requires an explicit write column list");
            std::unordered_set<std::string> labels;
            for (const auto& name : write.Columns) {
                columnType(name);
                checkExplicit(name);
                if (!labels.insert(name).second) throw TFrontendError(pos, "duplicate write column: " + name);
            }
            TRelation values{ERelation::UnionAll, pos};
            if (write.Rows.size() > 1)
                for (const auto& name : write.Columns) values.Columns.push_back(Column(name, {}, columnType(name), pos));
            const TScope empty;
            for (const auto& row : write.Rows) {
                if (row.size() != write.Columns.size()) throw TFrontendError(pos, "write column count does not match VALUES row");
                TExprContext context{empty};
                context.AllowSubqueries = false;
                std::vector<TDefinition> fields;
                std::vector<TColumnId> columns;
                for (std::size_t i = 0; i < row.size(); ++i) {
                    const auto& name = write.Columns[i];
                    const auto type = columnType(name);
                    const auto value = Program.Expressions.Convert(Expr(row[i], context), type, Syntax.Expressions[row[i]].Position);
                    const auto id = Column(name, {}, type, pos);
                    fields.push_back({id, value}); columns.push_back(id);
                }
                values.Inputs.push_back(Map(Relation({ERelation::Unit, pos}), std::move(fields), pos));
                values.UnionInputs.push_back(std::move(columns));
            }
            // A single VALUES row already has the required typed bindings.
            // UnionAll remains an actual multi-input relation at the handoff.
            if (values.Inputs.size() == 1) values.Columns = values.UnionInputs.front();
            for (std::size_t i = 0; i < write.Columns.size(); ++i) {
                written.insert(write.Columns[i]);
                effect.Columns.emplace_back(values.Columns[i], write.Columns[i]);
            }
            input = values.Inputs.size() == 1 ? values.Inputs.front() : Relation(std::move(values));
        } else {
            auto source = ReadTable(write.Target, snapshot);
            TExprContext context{source.Scope};
            input = source.Relation;
            if (write.Where != NoSyntax) {
                const auto predicate = Expr(write.Where, context);
                input = Filter(BindInput(input, context, pos), predicate, pos);
            }
            for (const auto& key : schema.Keys) add(key, Access(*source.Scope.Find(key, pos), pos));
            for (const auto& assignment : write.Assignments) {
                if (keys.contains(assignment.Alias)) throw TFrontendError(pos, "UPDATE cannot change a primary key: " + assignment.Alias);
                add(assignment.Alias, Expr(assignment.Expression, context));
            }
            input = BindInput(input, context, pos);
        }
        if (write.Kind == EWriteKind::Insert || write.Kind == EWriteKind::Upsert || write.Kind == EWriteKind::Replace) {
            for (const auto& [name, kind] : schema.Defaults) if (!written.contains(name)) {
                // Runtime generation belongs to the effect, after its input is
                // evaluated. These are storage names, not input column IDs.
                effect.DefaultColumns.push_back(name);
                written.insert(name);
            }
        }
        for (const auto& key : schema.Keys) if (!written.contains(key))
            throw TFrontendError(pos, "missing key column in input: " + key);
        if (write.Kind == EWriteKind::Insert || write.Kind == EWriteKind::Replace) {
            for (const auto& [name, value] : generated) written.insert(name);
            for (const auto& column : table.Columns) if (!written.contains(column.Name)) {
                if (!column.Type.OptionalDepth) throw TFrontendError(pos, "missing non-nullable write column: " + column.Name);
                add(column.Name, Program.Expressions.Literal(column.Type, {}, pos));
            }
        }
        PrepareGeneratedColumns(generated, written, keys, effect, pos);
        if (write.OnConflict == "revert" && (!effect.DefaultColumns.empty() || !effect.GeneratedColumns.empty()))
            throw TUnsupportedFeature(EFeature::Statement, pos,
                "INSERT OR REVERT with defaults or generated columns requires shared row preparation before conflict checks");
        TRelation relation{ERelation::Effect, pos};
        relation.Table = {Bindings.PhysicalAddress(write.Target).Cluster, table.Name};
        relation.Inputs = {Map(input, std::move(definitions), pos)};
        TQueryResult result{NoRelation, {}};
        std::unordered_set<std::string> returned;
        auto returning = [&](const std::string& name) {
            const auto type = columnType(name);
            if (!returned.insert(name).second) throw TFrontendError(pos, "duplicate RETURNING column: " + name);
            const auto id = Column(name, {}, type, pos);
            effect.Returning.emplace_back(id, name);
            relation.Columns.push_back(id);
            result.Columns.push_back({id, name, type});
        };
        if (write.ReturningAll) for (const auto& column : table.Columns) returning(column.Name);
        else for (const auto& name : write.Returning) returning(name);
        relation.Effect = std::make_shared<const TBoundEffect>(std::move(effect));
        result.Root = Relation(std::move(relation));
        return result;
    }

    using TUsingKeys = std::vector<std::pair<TScopeColumn, TScopeColumn>>;

    TUsingKeys ResolveUsingKeys(const TJoin& join, const TScope& left, const TScope& right) {
        TUsingKeys keys;
        std::unordered_set<std::string_view> seen;
        for (const auto& name : join.Using) {
            const auto pos = join.Source.Position;
            if (!seen.insert(name).second) throw TFrontendError(pos, "duplicate USING column: " + name);
            const auto* lhs = left.Find(name, pos);
            const auto* rhs = right.Find(name, pos);
            if (!lhs || !rhs) throw TFrontendError(pos, "USING column must exist in both inputs: " + name);
            keys.emplace_back(*lhs, *rhs);
        }
        return keys;
    }

    TRelId MergeUsingKeys(TRelId input, TScope& output, TUsingKeys keys, EJoinKind kind, TSourcePosition pos) {
        if (kind == EJoinKind::LeftSemi || kind == EJoinKind::LeftOnly ||
            kind == EJoinKind::RightSemi || kind == EJoinKind::RightOnly) return input;
        std::vector<TDefinition> definitions;
        for (auto& [left, right] : keys) {
            if (kind == EJoinKind::Right || kind == EJoinKind::Full || kind == EJoinKind::Exclusion) left.Type = left.Type.Optional();
            if (kind == EJoinKind::Left || kind == EJoinKind::Full || kind == EJoinKind::Exclusion) right.Type = right.Type.Optional();
            const TExprId arguments[]{Access(left, pos), Access(right, pos)};
            const auto expression = Program.Expressions.Coalesce(arguments, pos);
            const auto column = Column(left.Name, {}, Program.Expressions[expression].Type, pos);
            definitions.push_back({column, expression});
            output.Merge(ScopeColumn(column));
        }
        return Map(input, std::move(definitions), pos);
    }

    TBoundSource From(const TSelect& query, TExprContext* outer) {
        TBoundSource current;
        if (query.Source) current = Source(*query.Source, outer);
        else current.Relation = Relation({ERelation::Unit, query.Position});
        bool leftAny = query.Source && query.Source->Any;
        for (const auto& join : query.Joins) {
            const bool constrained = join.Predicate != NoSyntax || !join.Using.empty();
            if (join.Kind == EJoinKind::Cross ? constrained : !constrained)
                throw TFrontendError(join.Source.Position, join.Kind == EJoinKind::Cross ?
                    "CROSS JOIN cannot have ON or USING" : "JOIN requires ON or USING");
            auto right = Source(join.Source, outer);
            auto usingKeys = ResolveUsingKeys(join, current.Scope, right.Scope);
            const auto leftColumns = current.Scope.Columns.size();
            const bool keepLeft = join.Kind != EJoinKind::RightSemi && join.Kind != EJoinKind::RightOnly;
            const bool keepRight = join.Kind != EJoinKind::LeftSemi && join.Kind != EJoinKind::LeftOnly;
            // Ordinary joins keep the combined scope after predicate binding.
            // Transfer the left index instead of copying every name and column.
            TScope combined = keepLeft && keepRight ? std::move(current.Scope) : current.Scope;
            combined.Reserve(leftColumns + right.Scope.Columns.size());
            for (const auto& column : right.Scope.Columns) combined.Add(column);
            TExprContext context{combined};
            context.Outer = outer;
            context.AllowSubqueries = false;
            std::optional<TExprId> predicate;
            if (join.Predicate != NoSyntax) predicate = Expr(join.Predicate, context);
            for (const auto& [left, right] : usingKeys) {
                const auto pos = join.Source.Position;
                const auto equal = Program.Expressions.Binary(EOpcode::Equal, Access(left, pos), Access(right, pos), pos);
                predicate = predicate ? Program.Expressions.Binary(EOpcode::And, *predicate, equal, pos) : equal;
            }
            current.Relation = BindInput(current.Relation, context, join.Source.Position);
            if (predicate && Program.Expressions[*predicate].Type.Kind != ETypeKind::Bool) throw TFrontendError(join.Source.Position, "join predicate must have Boolean type");
            // Predicate binding has indexed both inputs. NULL padding changes
            // their types, not names or positions, so reuse that scope.
            TScope output = keepLeft && keepRight ? std::move(combined)
                : keepLeft ? std::move(current.Scope) : std::move(right.Scope);
            if (keepLeft && keepRight) for (std::size_t i = 0; i < output.Columns.size(); ++i) {
                const bool padded = join.Kind == EJoinKind::Full || join.Kind == EJoinKind::Exclusion ||
                    (i < leftColumns ? join.Kind == EJoinKind::Right : join.Kind == EJoinKind::Left);
                if (padded) output.Columns[i].Type = output.Columns[i].Type.Optional();
            }
            TRelation relation{ERelation::Join, join.Source.Position}; relation.Inputs = {current.Relation, right.Relation};
            relation.Columns = output.Ids(); relation.Predicate = predicate; relation.JoinKind = join.Kind;
            relation.LeftAny = leftAny; relation.RightAny = join.Source.Any;
            leftAny = false;
            const auto root = MergeUsingKeys(Relation(std::move(relation)), output, std::move(usingKeys), join.Kind, join.Source.Position);
            current = {root, std::move(output)};
        }
        return current;
    }

    TQueryResult Select(const TSelect& query, TExprContext* outer = nullptr) {
        TDepth depth(*this, query.Position);
        struct TDeclarationScope {
            std::unordered_map<const TCteBinding*, TExprContext*>& Contexts;
            std::vector<std::pair<const TCteBinding*, std::optional<TExprContext*>>> Saved;
            TDeclarationScope(decltype(Contexts) contexts, const TSelect& query, TExprContext* outer)
                : Contexts(contexts) {
                for (const auto& cte : query.Ctes) {
                    const auto it = Contexts.find(&cte);
                    Saved.emplace_back(&cte, it == Contexts.end() ? std::nullopt : std::optional{it->second});
                    Contexts[&cte] = outer;
                }
            }
            ~TDeclarationScope() {
                for (const auto& [cte, previous] : Saved) {
                    if (previous) Contexts[cte] = *previous;
                    else Contexts.erase(cte);
                }
            }
        } scope(CteContexts, query, outer);
        if (!query.SetInputs.empty()) {
            auto result = SetQuery(query, outer);
            return result;
        }
        auto source = From(query, outer);
        TExprContext sourceContext{source.Scope};
        sourceContext.Outer = outer;
        if (query.Where != NoSyntax) {
            const auto predicate = Expr(query.Where, sourceContext);
            source.Relation = Filter(BindInput(source.Relation, sourceContext, query.Position), predicate, Syntax.Expressions[query.Where].Position);
        }

        bool aggregate = !query.GroupBy.empty() || !query.GroupingSets.empty();
        for (const auto& projection : query.Projection) aggregate = aggregate || ContainsAggregate[projection.Expression];
        if (query.Having != NoSyntax) aggregate = aggregate || ContainsAggregate[query.Having];
        for (const auto& key : query.OrderBy) aggregate = aggregate || ContainsAggregate[key.Expression];
        TAggregationContext aggregation;
        TScope grouped;
        if (aggregate) {
            aggregation.Source = &sourceContext;
            aggregation.Identity.emplace(Syntax, Bindings, source.Scope, CanonicalWindows);
            std::unordered_set<TColumnId> seen;
            std::unordered_map<std::uint32_t, TColumnId> bound;
            std::vector<TScopeColumn> keyNames;
            sourceContext.AllowSubqueries = false;
            for (const auto& key : query.GroupBy) {
                const bool unnamed = key.Alias.empty() && Syntax.Expressions[key.Expression].Kind != ESyntaxKind::Column;
                if (unnamed && !Bindings.AllowsUnnamedGroups(query))
                    throw TUnsupportedFeature(EFeature::Expression, Syntax.Expressions[key.Expression].Position,
                        "computed GROUP BY keys require an explicit alias or PRAGMA YqlSelectAllowUnnamedGroupByExpr");
                const auto canonical = aggregation.Identity->Get(key.Expression);
                auto found = bound.find(canonical);
                const auto id = found == bound.end() ? Materialize(Expr(key.Expression, sourceContext), aggregation.Definitions,
                    Syntax.Expressions[key.Expression].Position, key.Alias) : found->second;
                bound.emplace(canonical, id);
                if (seen.insert(id).second) aggregation.Keys.push_back(id);
                auto column = ScopeColumn(id);
                if (!key.Alias.empty()) column.Name = key.Alias;
                if (unnamed) { column.Name.clear(); column.Qualifier.clear(); }
                keyNames.push_back(std::move(column));
            }
            std::unordered_map<TColumnId, std::size_t> frequency;
            for (const auto& set : query.GroupingSets) {
                auto& keys = aggregation.GroupingSets.emplace_back();
                std::unordered_set<TColumnId> present;
                for (const auto index : set) {
                    if (index >= keyNames.size()) throw TFrontendError(query.Position, "invalid grouping key index");
                    const auto id = keyNames[index].Id;
                    if (present.insert(id).second) { keys.push_back(id); ++frequency[id]; }
                }
            }
            aggregation.MayBeEmpty = aggregation.Keys.empty() || std::any_of(aggregation.GroupingSets.begin(), aggregation.GroupingSets.end(),
                [](const auto& set) { return set.empty(); });
            std::unordered_map<TColumnId, TColumnId> outward;
            for (const auto id : aggregation.Keys) {
                auto output = id;
                if (!aggregation.GroupingSets.empty()) {
                    auto type = Program.Columns[id].Type;
                    if (type.Kind == ETypeKind::Null) throw TUnsupportedFeature(EFeature::GroupingSets, query.Position, "bare NULL grouping keys require an explicit scalar cast");
                    if (frequency[id] != aggregation.GroupingSets.size()) type = type.Optional();
                    output = Column(Program.Columns[id].Name, Program.Columns[id].Qualifier, type, query.Position);
                    aggregation.GroupingColumns.emplace_back(output, id);
                }
                outward.emplace(id, output);
                aggregation.GroupSources.emplace(output, id);
            }
            std::set<std::tuple<TColumnId, std::string, std::string>> names;
            for (std::size_t i = 0; i < keyNames.size(); ++i) {
                auto column = keyNames[i];
                column.Id = outward.at(column.Id); column.Type = Program.Columns[column.Id].Type;
                aggregation.Groups.emplace(aggregation.Identity->Get(query.GroupBy[i].Expression), column);
                if (!column.Name.empty() && names.emplace(column.Id, column.Name, column.Qualifier).second) grouped.Add(std::move(column));
            }
            sourceContext.AllowSubqueries = true;
        }
        TExprContext context{aggregate ? grouped : source.Scope, aggregate ? &aggregation : nullptr};
        context.Outer = outer;
        if (!aggregate) context.CaptureOwner = &sourceContext;
        TExprContext windowInput{context.Scope, context.Aggregation};
        windowInput.Outer = outer;
        if (!aggregate) windowInput.CaptureOwner = &sourceContext;
        windowInput.AllowSubqueries = false;
        TWindowContext windows;
        windows.Source = &windowInput;
        context.Windows = &windows;
        std::vector<TDefinition> definitions;
        std::vector<TResultColumn> outputs;
        const auto projectionNames = ProjectionNames(query, Syntax, context.Scope);
        bool joinStar = false;
        std::unordered_map<std::string, TProjectionAlias> projectionAliases;
        for (std::size_t index = 0; index < query.Projection.size(); ++index) {
            const auto& projection = query.Projection[index];
            const auto& syntax = Syntax.Expressions[projection.Expression];
            if (syntax.Kind == ESyntaxKind::Star) {
                joinStar = joinStar || !query.Joins.empty();
                if (!projection.Alias.empty()) throw TFrontendError(syntax.Position, "a star cannot have an alias");
                bool found = false;
                for (const auto& column : context.Scope.Columns) if (column.MatchesStar(syntax.Text)) {
                    outputs.push_back({column.Id, column.Name, column.Type}); found = true;
                }
                if (!found) throw TFrontendError(syntax.Position, "star has no matching columns");
                continue;
            }
            auto expr = Expr(projection.Expression, context);
            const auto& name = projectionNames[index];
            auto id = Materialize(expr, definitions, syntax.Position, name);
            auto type = Program.Expressions[expr].Type;
            outputs.push_back({id, name, type});
            if (query.Having != NoSyntax && !projection.Alias.empty()) projectionAliases.emplace(name, TProjectionAlias{expr, ContainsWindow[projection.Expression]});
        }

        if (joinStar) {
            std::unordered_set<std::string_view> names;
            names.reserve(outputs.size());
            for (const auto& output : outputs) if (!names.insert(output.Name).second)
                throw TUnsupportedFeature(EFeature::Expression, query.Position,
                    "join-star projections with duplicate output names require column coalescing outside native-read-v1");
        }

        // RBO result bindings are unique IDs even when SQL projects one
        // source column repeatedly under different output names.
        std::unordered_set<TColumnId> projected;
        for (auto& output : outputs) {
            if (!projected.insert(output.Column).second) {
                const auto expression = Program.Expressions.Column(output.Column, output.Type, query.Position);
                output.Column = Column(output.Name, {}, output.Type, query.Position);
                definitions.push_back({output.Column, expression});
            }
        }

        context.Windows = nullptr;
        // Projected IN lookups may use a window result. Their materializations
        // belong above the windows; HAVING lookups remain below them.
        auto afterWindows = windows.Groups.empty() ? std::vector<TDefinition>{} : std::exchange(context.PendingDefinitions, {});
        std::optional<TExprId> having;
        if (query.Having != NoSyntax) {
            if (!aggregate) throw TFrontendError(query.Position, "HAVING requires grouping or aggregation");
            // HAVING expands a projected value at its own evaluation point;
            // it cannot read a Map/window output constructed later in the plan.
            context.ProjectionAliases = &projectionAliases;
            having = Expr(query.Having, context);
            context.ProjectionAliases = nullptr;
        }
        // ORDER BY aliases bind to result columns; other expressions see the
        // appropriate input/grouping scope. They are materialized once.
        std::vector<TBoundSortKey> sort;
        std::vector<TDefinition> sortDefinitions;
        TScope outputScope;
        if (!query.OrderBy.empty()) {
            outputScope.Reserve(outputs.size());
            for (const auto& output : outputs) outputScope.Add({output.Column, output.Name, {}, output.Type});
        }
        context.AllowSubqueries = false;
        context.Aliases = &outputScope;
        for (const auto& key : query.OrderBy) {
            const auto& syntax = Syntax.Expressions[key.Expression];
            const auto separators = Syntax.NameSeparators(syntax);
            const auto path = syntax.Kind == ESyntaxKind::Column ? outputScope.FindPath(syntax.Text, separators, syntax.Position) : TScope::TPath{};
            const TScopeColumn* alias = path.Members == separators.size() ? path.Column : nullptr;
            if (alias) sort.push_back({alias->Id, key.Descending, !key.Descending || !alias->Type.OptionalDepth});
            else {
                auto expr = Expr(key.Expression, context);
                auto id = Materialize(expr, sortDefinitions, syntax.Position);
                if (query.Distinct && std::none_of(outputs.begin(), outputs.end(), [&](const auto& output) { return output.Column == id; })) {
                    throw TFrontendError(syntax.Position, "ORDER BY in SELECT DISTINCT must name a result column");
                }
                sort.push_back({id, key.Descending, !key.Descending || !Program.Expressions[expr].Type.OptionalDepth});
            }
        }
        context.AllowSubqueries = true;
        context.Aliases = nullptr;
        auto root = BindInput(source.Relation, sourceContext, query.Position);
        if (aggregate) {
            root = Map(root, std::move(aggregation.Definitions), query.Position);
            TRelation node{ERelation::Aggregate, query.Position}; node.Inputs = {root}; node.GroupKeys = std::move(aggregation.Keys);
            node.Aggregates = std::move(aggregation.Aggregates);
            node.GroupingSets = std::move(aggregation.GroupingSets);
            node.GroupingColumns = std::move(aggregation.GroupingColumns);
            node.GroupingIndicators = std::move(aggregation.GroupingIndicators);
            if (node.GroupingSets.empty()) {
                node.Columns = node.GroupKeys;
                for (const auto& agg : node.Aggregates) node.Columns.push_back(agg.Output);
            } else {
                for (const auto& [output, input] : node.GroupingColumns) node.Columns.push_back(output);
                for (const auto& [output, input] : node.GroupingIndicators) node.Columns.push_back(output);
            }
            root = Relation(std::move(node));
        }
        root = BindInput(root, context, query.Position);
        if (having) root = Filter(root, *having, query.Position);
        if (!windows.Groups.empty()) {
            root = BindInput(root, windowInput, query.Position);
            root = Map(root, std::move(windows.Definitions), query.Position);
            for (auto& window : windows.Groups) {
                window.Inputs = {root};
                window.Columns = Program.Relations[root].Columns;
                for (const auto& function : window.WindowFunctions) window.Columns.push_back(function.Output);
                root = Relation(std::move(window));
            }
        }
        for (const auto& definition : afterWindows) root = Map(root, {definition}, query.Position);
        root = Map(root, std::move(definitions), query.Position);
        root = Map(root, std::move(sortDefinitions), query.Position);
        if (query.Distinct) {
            std::unordered_map<TColumnId, std::size_t> positions;
            if (!sort.empty()) for (std::size_t i = 0; i < outputs.size(); ++i) positions.emplace(outputs[i].Column, i);
            auto result = Distinct({root, std::move(outputs)}, query.Position);
            root = result.Root; outputs = std::move(result.Columns);
            for (auto& key : sort) key.Column = outputs[positions.at(key.Column)].Column;
        }

        if (!sort.empty()) {
            TRelation node{ERelation::Sort, query.Position}; node.Inputs = {root}; node.Columns = Program.Relations[root].Columns;
            node.SortKeys = std::move(sort); root = Relation(std::move(node));
        }
        root = Limit(root, query);
        return {root, std::move(outputs)};
    }

    TRelId Limit(TRelId root, const TSelect& query) {
        if (query.Limit != NoSyntax) {
            const TScope empty;
            TExprContext limitContext{empty};
            limitContext.AllowSubqueries = false;
            auto count = Expr(query.Limit, limitContext);
            count = CheckCount(count, Syntax.Expressions[query.Limit].Position, std::numeric_limits<std::uint64_t>::max());
            TRelation node{ERelation::Limit, query.Position}; node.Inputs = {root}; node.Columns = Program.Relations[root].Columns; node.Count = count;
            if (query.Offset != NoSyntax) {
                node.Offset = CheckCount(Expr(query.Offset, limitContext), Syntax.Expressions[query.Offset].Position);
            }
            root = Relation(std::move(node));
        }
        return root;
    }

    TQueryResult Distinct(TQueryResult result, TSourcePosition pos, bool lexicalKeys = false) {
        TRelation node{ERelation::Aggregate, pos};
        node.DistinctAll = true;
        std::vector<TDefinition> before, after;
        std::vector<TResultColumn*> keys;
        for (auto& column : result.Columns) keys.push_back(&column);
        // Set-operation aggregates use lexical keys; public positions remain
        // unchanged. Fresh outputs preserve the existing DISTINCT boundary.
        if (lexicalKeys) std::stable_sort(keys.begin(), keys.end(), [](const auto* left, const auto* right) {
            return left->Name < right->Name;
        });
        const auto add = [&](TColumnId input, TColumnId output) {
            node.GroupKeys.push_back(input);
            node.Aggregates.push_back({output, input, "distinct"});
            node.Columns.push_back(output);
        };
        for (auto* key : keys) {
            auto& column = *key;
            const auto input = column.Column;
            column.Column = Column(column.Name, {}, column.Type, pos);
            ColumnOrigins[column.Column] = ColumnOrigins[input];
            // A bare Null field has one value but is not a runtime data key.
            // Reconstruct it after grouping the remaining fields.
            if (column.Type.Kind == ETypeKind::Null) {
                after.push_back({column.Column, Program.Expressions.Null(pos)});
            } else add(input, column.Column);
        }
        if (node.GroupKeys.empty()) {
            // A constant key preserves zero rows for an empty input, unlike
            // a global aggregate with no grouping keys.
            const auto input = Materialize(Program.Expressions.Literal({ETypeKind::Uint64}, std::uint64_t{0}, pos), before, pos);
            add(input, Column("distinct_key", {}, {ETypeKind::Uint64}, pos));
        }
        node.Inputs = {Map(result.Root, std::move(before), pos)};
        result.Root = Map(Relation(std::move(node)), std::move(after), pos);
        return result;
    }

    TQueryResult SetQuery(const TSelect& query, TExprContext* outer) {
        using S = ESetOperation;
        const auto pos = query.Position;
        const auto firstRelation = Program.Relations.size();
        std::vector<TQueryResult> inputs;
        struct TMember { TType Type; std::size_t Count = 0; };
        std::map<std::string, TMember> members;
        std::vector<std::string> order;
        for (const auto& syntax : query.SetInputs) {
            auto input = Select(*syntax, outer);
            std::unordered_set<std::string> seen;
            std::vector<std::string> current;
            for (const auto& column : input.Columns) {
                if (!seen.insert(column.Name).second)
                    throw TUnsupportedFeature(EFeature::SetOperation, pos, "set operands require unique result names");
                auto [it, fresh] = members.emplace(column.Name, TMember{column.Type});
                if (!fresh) it->second.Type = CommonType(it->second.Type, column.Type, pos);
                ++it->second.Count;
                current.push_back(column.Name);
            }
            // Preserve the common ordered prefix, then use lexical order for
            // other fields, matching YQL's name-based set-result schema.
            if (inputs.empty()) order = current;
            else {
                bool truncated = false;
                for (std::size_t i = 0; i < std::min(order.size(), current.size()); ++i) {
                    if (order[i] != current[i]) { order.resize(i); truncated = true; break; }
                }
                if (!truncated && current.size() > order.size()) order = std::move(current);
            }
            inputs.push_back(std::move(input));
        }
        if (outer && std::any_of(query.SetOperations.begin(), query.SetOperations.end(),
                [](S op) { return op == S::IntersectAll || op == S::ExceptAll; })) {
            // The existing backend cannot push correlation through the
            // numbering windows used below. Uncorrelated subqueries are safe.
            if (HasExternalCaptures(firstRelation, outer))
                throw TUnsupportedFeature(EFeature::Backend, pos,
                    "correlated INTERSECT ALL/EXCEPT ALL requires backend window decorrelation");
        }
        std::unordered_set<std::string> ordered(order.begin(), order.end());
        for (auto& [name, member] : members) {
            if (member.Count != inputs.size()) member.Type = member.Type.Optional();
            if (ordered.insert(name).second) order.push_back(name);
        }
        for (auto& input : inputs) {
            std::unordered_map<std::string, TResultColumn> columns;
            for (const auto& column : input.Columns) columns.emplace(column.Name, column);
            input.Columns.clear();
            std::vector<TDefinition> definitions;
            for (const auto& name : order) {
                const auto type = members.at(name).Type;
                const auto column = columns.find(name);
                auto expr = column == columns.end() ? Program.Expressions.Literal(type, std::monostate{}, pos) :
                    Program.Expressions.Convert(Program.Expressions.Column(column->second.Column, column->second.Type, pos), type, pos);
                input.Columns.push_back({Materialize(expr, definitions, pos, name), name, type});
            }
            input.Root = Map(input.Root, std::move(definitions), pos);
        }
        auto unite = [&](std::vector<TQueryResult> branches) {
            if (branches.size() == 1) return std::move(branches.front());
            TRelation node{ERelation::UnionAll, pos};
            TQueryResult result{};
            for (const auto& branch : branches) {
                node.Inputs.push_back(branch.Root);
                auto& columns = node.UnionInputs.emplace_back();
                for (const auto& column : branch.Columns) columns.push_back(column.Column);
            }
            for (const auto& name : order) {
                const auto type = members.at(name).Type;
                const auto id = Column(name, {}, type, pos);
                node.Columns.push_back(id); result.Columns.push_back({id, name, type});
            }
            result.Root = Relation(std::move(node));
            return result;
        };
        auto keys = [&](TQueryResult& input, bool numbered) {
            std::vector<TDefinition> definitions;
            std::vector<TColumnId> result;
            for (const auto& column : input.Columns) {
                const auto value = Program.Expressions.Column(column.Column, column.Type, pos);
                // Required scalar keys already have the join's equality
                // semantics. Serialize only nullable keys to make NULL equal
                // NULL for set membership, as in the existing SQL lowering.
                const auto key = column.Type.OptionalDepth || column.Type.Kind == ETypeKind::Null
                    ? Program.Expressions.Unary(EOpcode::StablePickle, value, pos) : value;
                result.push_back(Materialize(key, definitions, pos));
            }
            input.Root = Map(input.Root, std::move(definitions), pos);
            if (numbered) {
                // Equal rows receive 1..N independently on each side. Matching
                // those ordinals gives min(N,M) / max(N-M,0) without row expansion.
                TRelation window{ERelation::Window, pos}; window.Inputs = {input.Root};
                window.Columns = Program.Relations[input.Root].Columns; window.GroupKeys = result;
                window.WindowFrame = {EWindowFrame::Rows, EWindowBound::UnboundedPreceding, EWindowBound::UnboundedFollowing};
                const auto ordinal = Column("set_ordinal", {}, {ETypeKind::Uint64}, pos);
                window.WindowFunctions.push_back({ordinal, {}, "rownumber"}); window.Columns.push_back(ordinal);
                input.Root = Relation(std::move(window)); result.push_back(ordinal);
            }
            return result;
        };
        // Adjacent UNION ALL operations share one pending n-ary union, avoiding
        // a deep operator chain and repeated copies of all preceding operands.
        std::vector<std::vector<TQueryResult>> stack;
        std::size_t next = 0;
        for (const auto op : query.SetOperations) {
            if (op == S::Push) { stack.emplace_back().push_back(std::move(inputs.at(next++))); continue; }
            if (stack.size() < 2) throw std::logic_error("unbalanced native set expression");
            auto right = std::move(stack.back()); stack.pop_back();
            auto& left = stack.back();
            if (op == S::Union || op == S::UnionAll) {
                left.insert(left.end(), std::make_move_iterator(right.begin()), std::make_move_iterator(right.end()));
                if (op == S::Union) {
                    auto result = Distinct(unite(std::move(left)), pos, true);
                    left.clear(); left.push_back(std::move(result));
                }
                continue;
            }
            const bool all = op == S::IntersectAll || op == S::ExceptAll;
            auto lhs = unite(std::move(left)), rhs = unite(std::move(right));
            const auto lhsKeys = keys(lhs, all), rhsKeys = keys(rhs, all);
            std::optional<TExprId> predicate;
            for (std::size_t i = 0; i < lhsKeys.size(); ++i) {
                const auto equal = Program.Expressions.Binary(EOpcode::Equal, Access(ScopeColumn(lhsKeys[i]), pos), Access(ScopeColumn(rhsKeys[i]), pos), pos);
                predicate = predicate ? Program.Expressions.Binary(EOpcode::And, *predicate, equal, pos) : equal;
            }
            TRelation join{ERelation::Join, pos}; join.Inputs = {lhs.Root, rhs.Root}; join.Predicate = predicate;
            join.JoinKind = op == S::Intersect || op == S::IntersectAll ? EJoinKind::LeftSemi : EJoinKind::LeftOnly;
            join.Columns = Program.Relations[lhs.Root].Columns;
            lhs.Root = Relation(std::move(join));
            if (!all) lhs = Distinct(std::move(lhs), pos, true);
            left.clear(); left.push_back(std::move(lhs));
        }
        if (stack.size() != 1 || next != inputs.size()) throw std::logic_error("unbalanced native set expression");
        auto result = unite(std::move(stack.back()));
        if (!query.OrderBy.empty()) {
            TScope scope;
            for (const auto& column : result.Columns) scope.Add({column.Column, column.Name, {}, column.Type});
            TExprContext context{scope}; context.AllowSubqueries = false;
            std::vector<TDefinition> definitions;
            TRelation sort{ERelation::Sort, pos};
            std::unordered_set<TColumnId> seen;
            for (const auto& key : query.OrderBy) {
                const auto expression = Expr(key.Expression, context);
                if (Program.Expressions[expression].Type.Kind == ETypeKind::Null) continue;
                const auto column = Materialize(expression, definitions, pos);
                // Output aliases have distinct IDs but can copy the same
                // value. Keep only the first ordering of an identical value.
                if (seen.insert(ColumnOrigins[column]).second)
                    sort.SortKeys.push_back({column, key.Descending, !key.Descending || !Program.Expressions[expression].Type.OptionalDepth});
            }
            result.Root = Map(result.Root, std::move(definitions), pos);
            if (!sort.SortKeys.empty()) {
                sort.Inputs = {result.Root}; sort.Columns = Program.Relations[result.Root].Columns;
                result.Root = Relation(std::move(sort));
            }
        }
        result.Root = Limit(result.Root, query);
        return result;
    }

    TExprId CheckCount(TExprId expression, TSourcePosition pos, std::uint64_t nullDefault = 0) {
        // SQL NULL counts mean an unbounded LIMIT or a zero OFFSET. Apply this
        // clause-level default before validating/coercing the required count.
        const auto type = Program.Expressions[expression].Type;
        if (type.Kind == ETypeKind::Null || (type.IsIntegral() && type.OptionalDepth)) {
            const TExprId arguments[]{expression, Program.Expressions.Literal({ETypeKind::Uint64}, nullDefault, pos)};
            expression = Program.Expressions.Coalesce(arguments, pos);
        }
        expression = Program.Expressions.FoldConstants(std::span(&expression, 1)).front();
        const auto& node = Program.Expressions[expression];
        if (!node.Type.IsIntegral() || node.Type.OptionalDepth) throw TFrontendError(pos, "LIMIT/OFFSET must be a non-null integer");
        if (node.Op == EOpcode::Literal && std::holds_alternative<std::int64_t>(node.Value) && std::get<std::int64_t>(node.Value) < 0) {
            throw TFrontendError(pos, "LIMIT/OFFSET must be nonnegative");
        }
        if (node.Op == EOpcode::Literal) {
            const auto value = std::holds_alternative<std::int64_t>(node.Value) ?
                static_cast<std::uint64_t>(std::get<std::int64_t>(node.Value)) : std::get<std::uint64_t>(node.Value);
            return Program.Expressions.Literal({ETypeKind::Uint64}, value, pos);
        }
        // Use the ordinary implicit conversion contract for dynamic counts.
        // Narrow signed integers can widen to Uint64; Int64 cannot narrow to it.
        return Program.Expressions.Convert(expression, {ETypeKind::Uint64}, pos);
    }

    TType AggregateType(std::string_view name, TType type, bool mayBeEmpty, TSourcePosition position) {
        if (name == "count") return {ETypeKind::Uint64};
        if (type.OptionalDepth > 1)
            throw TUnsupportedFeature(EFeature::Type, position, "nested optional aggregate arguments require an explicit Unwrap");
        if (name == "sum" || name == "avg" || IsSampleStddev(name)) {
            if (!type.IsNumeric() && !(name == "sum" && type.Kind == ETypeKind::Interval))
                throw TFrontendError(position, std::string(name) + " expects a numeric argument");
            if (IsSampleStddev(name) && type.Kind == ETypeKind::Decimal)
                throw TUnsupportedFeature(EFeature::Type, position, "sample standard deviation of Decimal is outside the native backend profile");
            if ((name == "avg" && type.Kind != ETypeKind::Decimal) || IsSampleStddev(name)) type = {ETypeKind::Double, type.OptionalDepth};
            else if (type.IsIntegral()) type.Kind = type.IsSigned() ? ETypeKind::Int64 : ETypeKind::Uint64;
            if (type.Kind == ETypeKind::Decimal && name == "sum") type.Precision = 35;
            if (type.Kind == ETypeKind::Interval && name == "sum") type = type.Optional();
        }
        return mayBeEmpty ? type.Optional() : type;
    }

    TExprId FilterAggregateInput(TExprId input, TSyntaxId filter, TExprContext& context) {
        if (filter == NoSyntax) return input;
        const auto pos = Syntax.Expressions[filter].Position;
        const auto predicate = Expr(filter, context);
        auto& arena = Program.Expressions;
        // Aggregates skip absent inputs. Keep the value inside IF so excluded
        // rows cannot evaluate a throwing or non-repeatable argument.
        return arena.If(predicate, input, arena.Literal(arena[input].Type.Optional(), {}, pos), pos);
    }

    TExprId Aggregate(const TSyntaxExpr& syntax, std::uint32_t canonical, TAggregationContext& context) {
        if (auto it = context.Calls.find(canonical); it != context.Calls.end()) return it->second;
        auto name = Lower(syntax.Text);
        const bool valueBy = IsValueByAggregate(name);
        if (syntax.Arguments.size() != (valueBy ? 2u : 1u))
            throw TFrontendError(syntax.Position, valueBy ? "MIN_BY/MAX_BY requires value and key arguments" : "aggregate requires one argument");
        auto arg = syntax.Arguments[0];
        TExprId input;
        if (Syntax.Expressions[arg].Kind == ESyntaxKind::Star) {
            if (name != "count" || syntax.Distinct) throw TFrontendError(syntax.Position, "only COUNT(*) accepts star");
            input = Program.Expressions.Literal({ETypeKind::Uint64}, std::uint64_t{1}, syntax.Position);
        } else {
            input = Expr(arg, *context.Source);
        }
        if (valueBy) {
            auto& arena = Program.Expressions;
            auto key = Expr(syntax.Arguments[1], *context.Source);
            const auto payloadType = arena[input].Type;
            if (arena[key].Type.Kind == ETypeKind::Null) key = arena.Convert(key, {ETypeKind::Int8, true}, syntax.Position);
            const auto keyType = arena[key].Type;
            if (keyType.IsCollection() || keyType.IsDocument() || keyType.Kind == ETypeKind::Calendar || keyType.OptionalDepth > 1 ||
                payloadType.IsDocument() || payloadType.Kind == ETypeKind::Calendar || payloadType.OptionalDepth > 1)
                throw TUnsupportedFeature(EFeature::Type, syntax.Position, "MIN_BY/MAX_BY requires a comparable value and scalar key");
            auto requiredKey = key;
            if (keyType.OptionalDepth) requiredKey = arena.Call("Unwrap", std::span(&key, 1), syntax.Position);
            const TExprId items[]{requiredKey, input};
            input = arena.Collection(items, false, syntax.Position);
            if (keyType.OptionalDepth) input = arena.If(arena.Unary(EOpcode::IsNotNull, key, syntax.Position), input,
                arena.Literal(arena[input].Type.Optional(), std::monostate{}, syntax.Position), syntax.Position);
            // The existing MIN/MAX aggregation machinery retains one tuple.
            // SQL allows any payload for tied keys; lexicographic tie-breaking
            // preserves that contract without introducing a new optimizer rule.
            name = name.starts_with("min") ? "min" : "max";
        }
        input = FilterAggregateInput(input, syntax.Filter, *context.Source);
        const auto type = AggregateType(name, Program.Expressions[input].Type, context.MayBeEmpty, syntax.Position);
        auto inputColumn = Materialize(input, context.Definitions, syntax.Position);
        const auto innerType = AggregateType(name, Program.Expressions[input].Type, context.Keys.empty(), syntax.Position);
        auto output = Column(name + std::to_string(Program.Columns.size()), {}, innerType, syntax.Position);
        context.Aggregates.push_back({output, inputColumn, IsSampleStddev(name) ? "stddev_samp" : name, syntax.Distinct});
        if (!context.GroupingSets.empty()) {
            const auto inner = output;
            output = Column(name + std::to_string(Program.Columns.size()), {}, type, syntax.Position);
            context.GroupingColumns.emplace_back(output, inner);
        }
        auto result = Program.Expressions.Column(output, type, syntax.Position);
        if (valueBy) result = Program.Expressions.TupleElement(result, 1, syntax.Position);
        context.Calls.emplace(canonical, result);
        return result;
    }

    TExprId Grouping(const TSyntaxExpr& syntax, TExprContext& context) {
        if (!context.Aggregation) throw TFrontendError(syntax.Position, "GROUPING requires grouping keys");
        if (syntax.Arguments.empty() || syntax.Arguments.size() > 64 || syntax.Distinct)
            throw TFrontendError(syntax.Position, "GROUPING requires one to 64 grouping keys");
        auto& grouping = *context.Aggregation;
        auto& arena = Program.Expressions;
        auto result = arena.Literal({ETypeKind::Uint64}, std::uint64_t{0}, syntax.Position);
        const auto two = arena.Literal({ETypeKind::Uint64}, std::uint64_t{2}, syntax.Position);
        for (const auto argument : syntax.Arguments) {
            const auto& arg = Syntax.Expressions[argument];
            const TScopeColumn* column = nullptr;
            if (auto it = grouping.Groups.find(grouping.Identity->Get(argument)); it != grouping.Groups.end()) column = &it->second;
            else if (arg.Kind == ESyntaxKind::Column) {
                const auto separators = Syntax.NameSeparators(arg);
                const auto path = context.Scope.FindPath(arg.Text, separators, arg.Position);
                if (path.Members == separators.size()) column = path.Column;
            }
            if (!column || !grouping.GroupSources.contains(column->Id)) throw TFrontendError(arg.Position, "GROUPING argument is not a grouping key");
            if (grouping.GroupingSets.empty()) continue;
            const auto source = grouping.GroupSources.at(column->Id);
            auto [it, inserted] = grouping.Indicators.try_emplace(source);
            if (inserted) {
                it->second = Column("grouping" + std::to_string(source), {}, {ETypeKind::Uint64}, syntax.Position);
                grouping.GroupingIndicators.emplace_back(it->second, source);
            }
            result = arena.Binary(EOpcode::Add, arena.Binary(EOpcode::Multiply, result, two, syntax.Position),
                arena.Column(it->second, {ETypeKind::Uint64}, syntax.Position), syntax.Position);
        }
        return result;
    }

    TExprId Window(TSyntaxId id, TWindowContext& context) {
        if (const auto call = context.Calls.find(Canonical[id]); call != context.Calls.end()) return call->second;
        const auto& syntax = Syntax.Expressions[id];
        const auto& spec = Bindings.Window(*syntax.Window);
        auto name = Lower(syntax.Text);
        if (name == "row_number") name = "rownumber";
        if (name == "dense_rank") name = "denserank";
        const bool rank = name == "rank" || name == "denserank" || name == "rownumber";
        if (!rank && name != "count" && name != "sum" && name != "avg" && name != "min" && name != "max")
            throw TUnsupportedFeature(EFeature::Window, syntax.Position, "unsupported native window function: " + syntax.Text);
        if (syntax.Distinct) throw TUnsupportedFeature(EFeature::Window, syntax.Position, "DISTINCT window aggregate");
        if (syntax.Arguments.size() != (rank ? 0 : 1)) throw TFrontendError(syntax.Position, "invalid window function argument count");
        auto materialize = [&](TSyntaxId arg, TSyntaxId filter = NoSyntax) {
            const auto key = (std::uint64_t{Canonical[arg]} << 32) | (filter == NoSyntax ? NoSyntax : Canonical[filter]);
            if (const auto found = context.Inputs.find(key); found != context.Inputs.end()) return found->second;
            auto expression = Syntax.Expressions[arg].Kind == ESyntaxKind::Star ?
                Program.Expressions.Literal({ETypeKind::Uint64}, std::uint64_t{1}, syntax.Position) : Expr(arg, *context.Source);
            expression = FilterAggregateInput(expression, filter, *context.Source);
            const auto column = Materialize(expression, context.Definitions, syntax.Position);
            context.InputTypes[column] = Program.Expressions[expression].Type;
            context.Inputs.emplace(key, column);
            return column;
        };
        std::optional<TColumnId> input;
        if (!rank) {
            if (Syntax.Expressions[syntax.Arguments[0]].Kind == ESyntaxKind::Star && name != "count")
                throw TFrontendError(syntax.Position, "only COUNT(*) accepts star");
            input = materialize(syntax.Arguments[0], syntax.Filter);
        }
        auto [group, fresh] = context.Specifications.emplace(CanonicalWindows[id], context.Groups.size());
        if (fresh) {
            TRelation window{ERelation::Window, syntax.Position};
            std::unordered_set<TColumnId> partitions, order;
            for (const auto& key : spec.Partition) {
                if (Syntax.Expressions[key.Expression].Kind == ESyntaxKind::Star) throw TFrontendError(syntax.Position, "star is not a window partition key");
                const auto column = materialize(key.Expression);
                if (partitions.insert(column).second) window.GroupKeys.push_back(column);
            }
            for (const auto& key : spec.Order) {
                if (Syntax.Expressions[key.Expression].Kind == ESyntaxKind::Star) throw TFrontendError(syntax.Position, "star is not a window order key");
                const auto column = materialize(key.Expression);
                if (order.insert(column).second)
                    window.SortKeys.push_back({column, key.Descending, !key.Descending || !context.InputTypes.at(column).OptionalDepth});
            }
            auto frame = spec.Frame;
            using B = EWindowBound;
            using F = EWindowFrame;
            if (frame.Type == F::Default) {
                frame.Type = !window.SortKeys.empty() && Bindings.Uses(id, TGlobalBindings::AnsiCurrentRow) ? F::Range : F::Rows;
                frame.End = window.SortKeys.empty() ? B::UnboundedFollowing : B::CurrentRow;
            }
            if (frame.Begin == B::UnboundedFollowing || frame.End == B::UnboundedPreceding ||
                (frame.Begin >= B::CurrentRow && frame.End < B::CurrentRow) ||
                (frame.Begin > B::CurrentRow && frame.End == B::CurrentRow))
                throw TFrontendError(syntax.Position, "invalid window frame bounds");
            auto offset = [&](TSyntaxId expr) -> std::uint64_t {
                if (expr == NoSyntax) return 0;
                const TScope empty;
                TExprContext constants{empty}; constants.AllowSubqueries = false;
                const auto value = Expr(expr, constants);
                const auto& node = Program.Expressions[value];
                if (node.Op != EOpcode::Literal || !node.Type.IsIntegral() || node.Type.OptionalDepth)
                    throw TUnsupportedFeature(EFeature::Window, syntax.Position, "window offsets require nonnegative integer literals");
                if (frame.Type != F::Range && node.Type.Kind != ETypeKind::Int32)
                    throw TFrontendError(syntax.Position, "ROWS/GROUPS window offset literals require Int32");
                if (node.Type.IsSigned() && std::get<std::int64_t>(node.Value) < 0)
                    throw TFrontendError(syntax.Position, "negative window offset");
                const auto amount = node.Type.IsSigned() ? static_cast<std::uint64_t>(std::get<std::int64_t>(node.Value)) : std::get<std::uint64_t>(node.Value);
                if (amount > INT32_MAX) throw TUnsupportedFeature(EFeature::Window, syntax.Position, "native window offset exceeds Int32");
                return amount;
            };
            frame.BeginOffset = offset(spec.BeginOffset); frame.EndOffset = offset(spec.EndOffset);
            if (frame.Begin == B::UnboundedPreceding && frame.End == B::UnboundedFollowing) frame.Type = F::Rows;
            if (frame.Type == F::Groups) throw TUnsupportedFeature(EFeature::Window, syntax.Position, "GROUPS window frame");
            if (frame.Type == F::Range && window.SortKeys.empty())
                throw TUnsupportedFeature(EFeature::Window, syntax.Position, "RANGE without window ordering");
            if (frame.Type == F::Range && (spec.BeginOffset != NoSyntax || spec.EndOffset != NoSyntax)) {
                if (window.SortKeys.size() != 1) throw TFrontendError(syntax.Position, "RANGE offsets require one order key");
                const auto type = context.InputTypes.at(window.SortKeys[0].Column);
                if (!type.IsIntegral() && type.Kind != ETypeKind::Float && type.Kind != ETypeKind::Double)
                    throw TUnsupportedFeature(EFeature::Window, syntax.Position, "RANGE offsets require an integer or floating order key");
            }
            window.WindowFrame = frame;
            context.Groups.push_back(std::move(window));
        }
        auto& window = context.Groups[group->second];
        const auto& frame = window.WindowFrame;
        const bool mayBeEmpty = (frame.Begin == EWindowBound::Following && frame.BeginOffset > 0) ||
            (frame.End == EWindowBound::Preceding && frame.EndOffset > 0);
        const auto type = rank ? TType{ETypeKind::Uint64} : AggregateType(name, context.InputTypes.at(*input), mayBeEmpty, syntax.Position);
        const auto output = Column(name + std::to_string(Program.Columns.size()), {}, type, syntax.Position);
        window.WindowFunctions.push_back({output, input, name});
        const auto value = Program.Expressions.Column(output, type, syntax.Position);
        context.Calls.emplace(Canonical[id], value);
        return value;
    }

    TType TypeConstructor(TSyntaxId id) {
        const auto& syntax = Syntax.Expressions[id];
        TDepth depth(*this, syntax.Position);
        if (syntax.Kind == ESyntaxKind::Column) return ParseType(syntax.Text, syntax.Position);
        if (syntax.Kind == ESyntaxKind::Call && syntax.Arguments.size() == 1) {
            const auto name = Lower(syntax.Text);
            if (name == "optionaltype") {
                auto type = TypeConstructor(syntax.Arguments[0]);
                if (type.OptionalDepth == 64) throw TResourceLimitError(syntax.Position, "Optional type nesting exceeds 64");
                ++type.OptionalDepth;
                return type;
            }
            const auto& argument = Syntax.Expressions[syntax.Arguments[0]];
            if (name == "datatype" && IsStringLiteral(argument.Kind))
                return ParseType(argument.Text, argument.Position);
        }
        throw TUnsupportedFeature(EFeature::Type, syntax.Position, "unsupported native type constructor");
    }

    TExprId Integer(const TSyntaxExpr& syntax, bool negative = false, bool checked = false) {
        std::string_view value = syntax.Text;
        int radix = 10;
        if (value.size() > 2 && value[0] == '0') {
            if (value[1] == 'x' || value[1] == 'X') radix = 16;
            if (value[1] == 'o' || value[1] == 'O') radix = 8;
            if (value[1] == 'b' || value[1] == 'B') radix = 2;
            if (radix != 10) value.remove_prefix(2);
        }
        std::uint64_t number = 0;
        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), number, radix);
        if (error != std::errc()) throw TFrontendError(syntax.Position, "integer literal is out of range or malformed");
        auto suffix = Lower(std::string_view(end, value.data() + value.size() - end));
        const bool unsignedValue = suffix.starts_with('u');
        if (unsignedValue) suffix.erase(0, 1);
        ETypeKind kind;
        if (suffix.empty()) kind = unsignedValue ? ETypeKind::Uint32 :
            number <= std::uint64_t{INT32_MAX} + negative ? ETypeKind::Int32 :
            number <= std::uint64_t{INT64_MAX} + negative ? ETypeKind::Int64 : ETypeKind::Uint64;
        else if (suffix == "l") kind = unsignedValue ? ETypeKind::Uint64 : ETypeKind::Int64;
        else if (suffix == "s") kind = unsignedValue ? ETypeKind::Uint16 : ETypeKind::Int16;
        else if (suffix == "t") kind = unsignedValue ? ETypeKind::Uint8 : ETypeKind::Int8;
        else throw TUnsupportedFeature(EFeature::Syntax, syntax.Position, "unsupported integer suffix");
        const TType type{kind};
        if (type.IsSigned()) {
            if (number > std::uint64_t{INT64_MAX} + negative) throw TFrontendError(syntax.Position, "signed integer literal is out of range");
            const auto value = negative ? static_cast<std::int64_t>(-static_cast<__int128>(number)) : static_cast<std::int64_t>(number);
            return Program.Expressions.Literal(type, value, syntax.Position);
        }
        if (negative && !unsignedValue) throw TFrontendError(syntax.Position, "negative integer literal is out of range");
        auto literal = Program.Expressions.Literal(type, number, syntax.Position);
        return !negative ? literal : checked ? Program.Expressions.CheckedNegate(literal, syntax.Position) :
            Program.Expressions.Unary(EOpcode::Negate, literal, syntax.Position);
    }

    TExprId Expr(TSyntaxId id, TExprContext& context) {
        if (auto it = context.Cache.find(id); it != context.Cache.end()) return it->second;
        const auto& syntax = Syntax.Expressions.at(id);
        TDepth depth(*this, syntax.Position);
        CheckSize(syntax.Position);
        if (syntax.Kind == ESyntaxKind::Column && context.ProjectionAliases) {
            const auto separators = Syntax.NameSeparators(syntax);
            const auto base = syntax.Text.substr(0, separators.empty() ? syntax.Text.size() : separators.front());
            if (const auto found = context.ProjectionAliases->find(base); found != context.ProjectionAliases->end()) {
                CheckColumnProfile(base, syntax.Position);
                if (found->second.HasWindow) throw TFrontendError(syntax.Position, "window expressions are not allowed in HAVING");
                const auto result = AccessMembers(found->second.Expression, syntax, 0);
                context.Cache.emplace(id, result);
                return result;
            }
        }
        if (syntax.Kind == ESyntaxKind::Column && context.Aliases) {
            const auto path = context.Aliases->FindPath(syntax.Text, Syntax.NameSeparators(syntax), syntax.Position);
            if (path.Column) {
                const auto result = AccessMembers(Access(*path.Column, syntax.Position), syntax, path.Members);
                context.Cache.emplace(id, result);
                return result;
            }
        }
        if (context.Aggregation) if (auto it = context.Aggregation->Groups.find(context.Aggregation->Identity->Get(id)); it != context.Aggregation->Groups.end()) {
            auto result = Access(it->second, syntax.Position); context.Cache.emplace(id, result); return result;
        }
        auto& arena = Program.Expressions;
        TExprId result;
        switch (syntax.Kind) {
            case ESyntaxKind::Null: result = arena.Null(syntax.Position); break;
            case ESyntaxKind::Boolean: result = arena.Literal({ETypeKind::Bool}, syntax.Text == "true", syntax.Position); break;
            case ESyntaxKind::Integer: result = Integer(syntax); break;
            case ESyntaxKind::Real: {
                std::string_view literal = syntax.Text;
                const bool single = literal.ends_with('f') || literal.ends_with('F');
                if (single) literal.remove_suffix(1);
                double number;
                std::istringstream stream{std::string(literal)};
                stream.imbue(std::locale::classic());
                stream >> std::noskipws >> number;
                if (!stream || !stream.eof() || !std::isfinite(number)) throw TFrontendError(syntax.Position, "invalid floating point literal");
                result = arena.Literal({single ? ETypeKind::Float : ETypeKind::Double}, number, syntax.Position); break;
            }
            case ESyntaxKind::String: case ESyntaxKind::ByteString: case ESyntaxKind::Utf8String: {
                const bool unicode = syntax.Kind == ESyntaxKind::Utf8String ||
                    (syntax.Kind == ESyntaxKind::String && Bindings.Uses(id, TGlobalBindings::UnicodeLiterals));
                result = arena.Literal({unicode ? ETypeKind::Utf8 : ETypeKind::String}, syntax.Text, syntax.Position);
                break;
            }
            case ESyntaxKind::JsonString: case ESyntaxKind::YsonString:
                result = arena.Literal({syntax.Kind == ESyntaxKind::JsonString ? ETypeKind::Json : ETypeKind::Yson}, syntax.Text, syntax.Position);
                break;
            case ESyntaxKind::Column: {
                const auto column = ResolveColumn(context, syntax);
                if (!column) {
                    throw TFrontendError(syntax.Position, "unknown or ungrouped column: " + syntax.Text);
                }
                result = AccessMembers(Access(column->Column, syntax.Position), syntax, column->Members); break;
            }
            case ESyntaxKind::Member:
                result = arena.Member(Expr(syntax.Arguments.at(0), context), syntax.Text, syntax.Position); break;
            case ESyntaxKind::Named:
                throw TFrontendError(syntax.Position, "named arguments require AsStruct");
            case ESyntaxKind::Variable: {
                const auto binding = Bindings[id];
                if (binding == TGlobalBindings::Parameter) {
                    const auto& type = Program.Parameters.at(syntax.Text);
                    if (const auto* scalar = type.Scalar()) result = arena.Parameter(syntax.Text, *scalar, syntax.Position);
                    else if (const auto* list = type.ScalarList()) result = arena.Parameter(syntax.Text, arena.ListType(list->Item, syntax.Position), syntax.Position);
                    else throw TUnsupportedFeature(EFeature::Expression, syntax.Position, "row-list parameters are relational inputs; use AS_TABLE");
                } else result = ScalarValue(LookupBinding(binding, context, syntax.Position), syntax.Position);
                break;
            }
            case ESyntaxKind::Unary: {
                if (syntax.Text == "-" && Syntax.Expressions[syntax.Arguments[0]].Kind == ESyntaxKind::Integer) {
                    result = Integer(Syntax.Expressions[syntax.Arguments[0]], true, Bindings.Uses(id, TGlobalBindings::CheckedOps));
                    break;
                }
                auto arg = Expr(syntax.Arguments[0], context);
                if (syntax.Text == "+") { if (!arena[arg].Type.IsNumeric()) throw TFrontendError(syntax.Position, "unary + expects a number"); result = arg; }
                else if (syntax.Text == "-") result = Bindings.Uses(id, TGlobalBindings::CheckedOps)
                    ? arena.CheckedNegate(arg, syntax.Position) : arena.Unary(EOpcode::Negate, arg, syntax.Position);
                else if (syntax.Text == "~") result = arena.Unary(EOpcode::BitNot, arg, syntax.Position);
                else if (syntax.Text == "NOT") result = arena.Unary(EOpcode::Not, arg, syntax.Position);
                else if (syntax.Text == "IS NULL") result = arena.Unary(EOpcode::IsNull, arg, syntax.Position);
                else if (syntax.Text == "IS NOT NULL") result = arena.Unary(EOpcode::IsNotNull, arg, syntax.Position);
                else throw TUnsupportedFeature(EFeature::Expression, syntax.Position, "unsupported unary operator: " + syntax.Text);
                break;
            }
            case ESyntaxKind::Binary: {
                auto left = Expr(syntax.Arguments[0], context), right = Expr(syntax.Arguments[1], context);
                static const std::unordered_map<std::string, EOpcode> ops{
                    {"+", EOpcode::Add}, {"-", EOpcode::Subtract}, {"*", EOpcode::Multiply}, {"/", EOpcode::Divide}, {"%", EOpcode::Modulo},
                    {"=", EOpcode::Equal}, {"==", EOpcode::Equal}, {"!=", EOpcode::NotEqual}, {"<>", EOpcode::NotEqual},
                    {"<", EOpcode::Less}, {"<=", EOpcode::LessEqual}, {">", EOpcode::Greater}, {">=", EOpcode::GreaterEqual},
                    {"AND", EOpcode::And}, {"OR", EOpcode::Or}, {"||", EOpcode::Concat},
                    {"&", EOpcode::BitAnd}, {"|", EOpcode::BitOr}, {"^", EOpcode::BitXor},
                    {"<<", EOpcode::ShiftLeft}, {">>", EOpcode::ShiftRight},
                    {"IS NOT DISTINCT FROM", EOpcode::IsNotDistinct}};
                if (syntax.Text == "LIKE" || syntax.Text == "ILIKE") result = NDetail::BuildLike(arena, left, right,
                    syntax.Arguments.size() == 3 ? std::optional{Expr(syntax.Arguments[2], context)} : std::nullopt, syntax.Position, syntax.Text == "LIKE");
                else if (syntax.Text == "IS DISTINCT FROM")
                    result = arena.Unary(EOpcode::Not, arena.Binary(EOpcode::IsNotDistinct, left, right, syntax.Position), syntax.Position);
                else if (syntax.Text == "XOR") {
                    if (arena[left].Type.Kind != ETypeKind::Bool || arena[right].Type.Kind != ETypeKind::Bool) {
                        throw TFrontendError(syntax.Position, "XOR requires Boolean operands");
                    }
                    result = arena.Binary(EOpcode::NotEqual, left, right, syntax.Position);
                }
                else if (syntax.Text == "??") { const TExprId args[]{left, right}; result = arena.Coalesce(args, syntax.Position); }
                else if (auto it = ops.find(syntax.Text); it != ops.end()) {
                    const auto op = it->second;
                    if (op == EOpcode::Divide && !Bindings.Uses(id, TGlobalBindings::ClassicDivision))
                        left = arena.Cast(left, {ETypeKind::Double}, syntax.Position);
                    const bool arithmetic = op == EOpcode::Add || op == EOpcode::Subtract || op == EOpcode::Multiply ||
                        op == EOpcode::Divide || op == EOpcode::Modulo;
                    result = arithmetic && Bindings.Uses(id, TGlobalBindings::CheckedOps)
                        ? arena.CheckedBinary(op, left, right, syntax.Position) : arena.Binary(op, left, right, syntax.Position);
                }
                else throw TUnsupportedFeature(EFeature::Expression, syntax.Position, "unsupported binary operator: " + syntax.Text);
                break;
            }
            case ESyntaxKind::BitCast:
                result = arena.BitCast(Expr(syntax.Arguments[0], context), arena.ParseTypeName(syntax.Text, syntax.Position), syntax.Position);
                break;
            case ESyntaxKind::Cast: {
                const auto type = arena.ParseTypeName(syntax.Text, syntax.Position);
                const auto target = type.Kind == ETypeKind::List ? arena.ListItem(type) : type;
                if (!target.IsIntegral() && !target.IsString() && target.Kind != ETypeKind::Bool &&
                    target.Kind != ETypeKind::Float && target.Kind != ETypeKind::Double && target.Kind != ETypeKind::Decimal && target.Kind != ETypeKind::Tuple &&
                    !target.IsDate() && !target.IsInterval() && target.Kind != ETypeKind::Uuid)
                    throw TUnsupportedFeature(EFeature::Expression, syntax.Position, "explicit cast target is outside the native scalar profile");
                result = arena.Cast(Expr(syntax.Arguments[0], context), type, syntax.Position);
                break;
            }
            case ESyntaxKind::JsonValue: result = arena.JsonValue(Expr(syntax.Arguments[0], context), syntax.Text, syntax.Position); break;
            case ESyntaxKind::Case: {
                result = Expr(syntax.Arguments.back(), context);
                for (std::size_t i = syntax.Arguments.size() - 1; i > 0; i -= 2) {
                    auto condition = Expr(syntax.Arguments[i - 2], context), value = Expr(syntax.Arguments[i - 1], context);
                    result = arena.If(condition, value, result, syntax.Position);
                }
                break;
            }
            case ESyntaxKind::Between: {
                auto value = Expr(syntax.Arguments[0], context), low = Expr(syntax.Arguments[1], context), high = Expr(syntax.Arguments[2], context);
                // NOT BETWEEN uses the opposite comparisons, preserving the
                // SQL translator's behavior for unordered floating-point values.
                const bool negate = syntax.Text.starts_with("NOT ");
                const auto lower = negate ? EOpcode::Less : EOpcode::GreaterEqual;
                const auto upper = negate ? EOpcode::Greater : EOpcode::LessEqual;
                const auto combine = negate ? EOpcode::Or : EOpcode::And;
                auto a = arena.Binary(lower, value, low, syntax.Position), b = arena.Binary(upper, value, high, syntax.Position);
                result = arena.Binary(combine, a, b, syntax.Position);
                if (syntax.Text.ends_with(" SYMMETRIC")) {
                    a = arena.Binary(lower, value, high, syntax.Position);
                    b = arena.Binary(upper, value, low, syntax.Position);
                    result = arena.Binary(negate ? EOpcode::And : EOpcode::Or, result,
                        arena.Binary(combine, a, b, syntax.Position), syntax.Position);
                }
                break;
            }
            case ESyntaxKind::In: {
                const auto namedQuery = syntax.Text == "collection" ? Bindings.QueryReference(syntax.Arguments.at(1)) : nullptr;
                if ((syntax.Query || namedQuery) && !context.AllowSubqueries)
                    throw TUnsupportedFeature(EFeature::Subquery, syntax.Position, "subqueries in this expression context are outside the native profile");
                auto value = Expr(syntax.Arguments[0], context);
                if (syntax.Query || namedQuery) {
                    auto query = syntax.Query ? Subquery(*syntax.Query, context)
                        : ReuseQuery(BindQuery(*namedQuery, nullptr, true, syntax.Position), {}, syntax.Position);
                    if (query.Columns.size() != 1) throw TFrontendError(syntax.Position, "IN subquery must return exactly one column");
                    const auto output = query.Columns.front();
                    if (arena[value].Type.OptionalDepth > 1 || output.Type.OptionalDepth > 1)
                        throw TUnsupportedFeature(EFeature::Type, syntax.Position, "nested optional IN keys require an explicit Unwrap");
                    auto common = CommonType(arena[value].Type, output.Type, syntax.Position);
                    // Both sides may be bare NULL. Use an ordinary nullable key
                    // type so the empty-set case still has a Boolean result.
                    if (common.Kind == ETypeKind::Null) common = {ETypeKind::Bool, true};
                    const TType type{ETypeKind::Bool, common.OptionalDepth};
                    common.OptionalDepth = arena[value].Type.OptionalDepth || arena[value].Type.Kind == ETypeKind::Null;
                    value = arena.Convert(value, common, syntax.Position);
                    const auto lookup = Materialize(value, context.PendingDefinitions, syntax.Position);
                    common.OptionalDepth = output.Type.OptionalDepth || output.Type.Kind == ETypeKind::Null;
                    const auto item = arena.Convert(arena.Column(output.Column, output.Type, syntax.Position), common, syntax.Position);
                    std::vector<TDefinition> conversions;
                    const auto resultColumn = Materialize(item, conversions, syntax.Position);
                    const auto root = Map(query.Root, std::move(conversions), syntax.Position);
                    const auto binding = Column("in", {}, type, syntax.Position);
                    Program.Subplans.emplace(binding, TSubplan{root, resultColumn, ESubquery::In, lookup});
                    result = arena.InResult(Access(ScopeColumn(lookup), syntax.Position), Access(ScopeColumn(binding), syntax.Position), Bindings.Uses(id, TGlobalBindings::AnsiIn), syntax.Position);
                    break;
                }
                if (syntax.Text == "collection") {
                    result = arena.InCollection(value, Expr(syntax.Arguments.at(1), context), Bindings.Uses(id, TGlobalBindings::AnsiIn), syntax.Position);
                    break;
                }
                if (syntax.Text == "list") {
                    std::vector<TExprId> items;
                    items.reserve(syntax.Arguments.size() - 1);
                    for (std::size_t i = 1; i < syntax.Arguments.size(); ++i) items.push_back(Expr(syntax.Arguments[i], context));
                    result = arena.InCollection(value, arena.Collection(items, true, syntax.Position), Bindings.Uses(id, TGlobalBindings::AnsiIn), syntax.Position);
                    break;
                }
                result = arena.Literal({ETypeKind::Bool}, false, syntax.Position);
                for (std::size_t i = 1; i < syntax.Arguments.size(); ++i) {
                    const auto item = Expr(syntax.Arguments[i], context);
                    const auto equal = arena.Binary(EOpcode::Equal, value, item, syntax.Position);
                    result = arena.Binary(EOpcode::Or, result, equal, syntax.Position);
                }
                result = arena.InResult(value, result, Bindings.Uses(id, TGlobalBindings::AnsiIn), syntax.Position);
                break;
            }
            case ESyntaxKind::Lambda:
                throw TFrontendError(syntax.Position, "lambda is not a scalar result");
            case ESyntaxKind::Invoke:
                result = ScalarValue(InvokeBinding(id, context), syntax.Position);
                break;
            case ESyntaxKind::Index: {
                const auto list = Expr(syntax.Arguments[0], context);
                const auto index = Expr(syntax.Arguments[1], context);
                result = arena.ListElement(list, index, syntax.Position);
                break;
            }
            case ESyntaxKind::Call: {
                if (syntax.Text.starts_with('$')) {
                    result = ScalarValue(InvokeBinding(id, context), syntax.Position);
                    break;
                }
                const auto name = Lower(syntax.Text);
                if (syntax.Window) {
                    if (!context.Windows) throw TUnsupportedFeature(EFeature::Window, syntax.Position, "window function is not allowed in this expression context");
                    result = Window(id, *context.Windows); break;
                }
                if (IsAggregate(name)) {
                    if (!context.Aggregation) throw TFrontendError(syntax.Position, "aggregate is not allowed in this expression");
                    result = Aggregate(syntax, context.Aggregation->Identity->Get(id), *context.Aggregation); break;
                }
                if (name == "grouping") { result = Grouping(syntax, context); break; }
                if (syntax.Distinct) throw TFrontendError(syntax.Position, "DISTINCT requires an aggregate function");
                if (name == "asstruct") {
                    std::vector<std::pair<std::string, TExprId>> fields;
                    fields.reserve(syntax.Arguments.size());
                    for (const auto arg : syntax.Arguments) {
                        const auto& field = Syntax.Expressions[arg];
                        if (field.Kind != ESyntaxKind::Named)
                            throw TFrontendError(field.Position, "AsStruct requires named arguments");
                        fields.emplace_back(field.Text, Expr(field.Arguments.at(0), context));
                    }
                    result = arena.Struct(fields, syntax.Position);
                    break;
                }
                if (name == "nothing") {
                    if (syntax.Arguments.size() != 1)
                        throw TUnsupportedFeature(EFeature::Type, syntax.Position, "Nothing requires an explicit optional type");
                    const auto type = TypeConstructor(syntax.Arguments[0]);
                    if (!type.OptionalDepth) throw TFrontendError(syntax.Position, "Nothing requires an optional type");
                    result = arena.Literal(type, {}, syntax.Position);
                    break;
                }
                if (name == "yql::sqlin") {
                    if (syntax.Arguments.size() != 3) throw TFrontendError(syntax.Position, "SqlIn requires collection, lookup and options");
                    const auto& options = Syntax.Expressions[syntax.Arguments[2]];
                    if (options.Kind != ESyntaxKind::Call || Lower(options.Text) != "astuple")
                        throw TUnsupportedFeature(EFeature::Expression, syntax.Position, "SqlIn options require a literal tuple");
                    bool ansi = false;
                    for (auto optionId : options.Arguments) {
                        const auto& option = Syntax.Expressions[optionId];
                        if (option.Kind != ESyntaxKind::Call || Lower(option.Text) != "astuple" || option.Arguments.size() != 1)
                            throw TFrontendError(option.Position, "SqlIn option requires a one-item tuple");
                        const auto& atom = Syntax.Expressions[option.Arguments[0]];
                        if (atom.Kind != ESyntaxKind::Call || Lower(atom.Text) != "asatom" || atom.Arguments.size() != 1)
                            throw TUnsupportedFeature(EFeature::Expression, atom.Position, "SqlIn option requires a literal atom");
                        const auto& text = Syntax.Expressions[atom.Arguments[0]];
                        if (!IsStringLiteral(text.Kind) || text.Text != "ansi")
                            throw TUnsupportedFeature(EFeature::Expression, text.Position, "unsupported SqlIn option");
                        ansi = true;
                    }
                    const auto collection = Expr(syntax.Arguments[0], context), lookup = Expr(syntax.Arguments[1], context);
                    result = arena.InCollection(lookup, collection, ansi, syntax.Position);
                    break;
                }
                std::vector<TExprId> args;
                for (auto arg : syntax.Arguments) args.push_back(Expr(arg, context));
                if (name == "aslist" || name == "astuple") result = arena.Collection(args, name == "aslist", syntax.Position);
                else if ((name == "randomuuid" || name == "randomnumber") && args.empty())
                    throw TFrontendError(syntax.Position, syntax.Text + " requires at least one dependency argument");
                else if (name == "yql::randomuuid" || name == "yql::randomnumber") {
                    if (!args.empty()) throw TUnsupportedFeature(EFeature::Function, syntax.Position,
                        "raw random dependencies require SQL " + syntax.Text.substr(5) + "(arguments)");
                    result = arena.Call(std::string_view(name).substr(5), {}, syntax.Position);
                } else result = CallScalar(syntax.Text, args, syntax.Position);
                break;
            }
            case ESyntaxKind::Star: throw TFrontendError(syntax.Position, "star is only valid in projection or COUNT(*)");
            case ESyntaxKind::Exists:
            case ESyntaxKind::ScalarSubquery: {
                if (!context.AllowSubqueries)
                    throw TUnsupportedFeature(EFeature::Subquery, syntax.Position, "subqueries in this expression context are outside the native profile");
                auto query = Subquery(*syntax.Query, context);
                const bool exists = syntax.Kind == ESyntaxKind::Exists;
                if (!exists && query.Columns.size() != 1) throw TFrontendError(syntax.Position, "scalar subquery must return exactly one column");
                const auto& output = query.Columns.front();
                const auto binding = Column(exists ? "exists" : "scalar", {}, exists ? TType{ETypeKind::Bool} : output.Type.Optional(), syntax.Position);
                Program.Subplans.emplace(binding, TSubplan{query.Root, output.Column, exists ? ESubquery::Exists : ESubquery::Scalar, std::nullopt});
                result = Access(ScopeColumn(binding), syntax.Position);
                break;
            }
        }
        context.Cache.emplace(id, result); return result;
    }

    TExprId ScalarValue(const TBindingValue& value, TSourcePosition pos) const {
        if (const auto* scalar = std::get_if<TExprId>(&value)) return *scalar;
        throw TFrontendError(pos, "callable is not a scalar value");
    }

    TExprId CallScalar(std::string_view name, std::span<const TExprId> arguments, TSourcePosition pos) {
        if (Settings.ExternalBindings && name.find("::") != std::string_view::npos) {
            const auto found = Settings.ExternalBindings->Callables.find(std::string(name));
            if (found != Settings.ExternalBindings->Callables.end()) {
                const auto result = found->second.Apply(Program.Expressions, arguments, pos);
                CheckSize(pos);
                return result;
            }
        }
        return Program.Expressions.Call(name, arguments, pos);
    }

    TBindingValue LookupBinding(std::uint32_t binding, const TExprContext& context, TSourcePosition pos) {
        for (auto frame = context.BindingFrame; frame != NoBindingFrame; frame = BindingFrames[frame].Parent) {
            const auto& values = BindingFrames[frame].Values;
            if (const auto found = values.find(binding); found != values.end()) return found->second;
        }
        if (binding < Values.size() && Values[binding]) {
            if (const auto* external = std::get_if<TGlobalBindings::TExternal>(&*Values[binding]); external && !external->Callable) {
                Values[binding] = external->Body->Apply(Program.Expressions, {}, pos);
                CheckSize(pos);
            }
            return *Values[binding];
        }
        throw TUnsupportedFeature(EFeature::Expression, pos, "binding is not a scalar or callable value");
    }

    TBindingValue BindingValue(TSyntaxId id, TExprContext& context) {
        const auto& syntax = Syntax.Expressions.at(id);
        if (syntax.Kind == ESyntaxKind::Lambda) return TBoundLambda{id, context.BindingFrame};
        if (syntax.Kind == ESyntaxKind::Variable && Bindings[id] != TGlobalBindings::Parameter)
            return LookupBinding(Bindings[id], context, syntax.Position);
        if (syntax.Kind == ESyntaxKind::Invoke || (syntax.Kind == ESyntaxKind::Call && syntax.Text.starts_with('$')))
            return InvokeBinding(id, context);
        if (const auto name = CallableName(id)) return std::string(*name);
        if (syntax.Kind == ESyntaxKind::Call) {
            const auto name = Lower(syntax.Text);
            if (name == "re2::match" || name == "re2::grep") {
                if (syntax.Arguments.empty() || syntax.Arguments.size() > 2 || syntax.Distinct || syntax.Window)
                    throw TFrontendError(syntax.Position, "regular expression matcher requires a pattern and optional options");
                if (syntax.Arguments.size() == 2 && Program.Expressions[Expr(syntax.Arguments[1], context)].Type.Kind != ETypeKind::Null)
                    throw TUnsupportedFeature(EFeature::Function, syntax.Position, "native regular expression options currently require NULL");
                auto pattern = Expr(syntax.Arguments[0], context);
                const auto type = Program.Expressions[pattern].Type;
                if (!type.IsString() || type.OptionalDepth)
                    throw TFrontendError(syntax.Position, "regular expression pattern requires a non-null string");
                pattern = Program.Expressions.Convert(pattern, {ETypeKind::String}, syntax.Position);
                return TBoundRegex{pattern, name == "re2::match"};
            }
        }
        return Expr(id, context);
    }

    TBindingValue InvokeBinding(TSyntaxId id, TExprContext& context) {
        const auto& syntax = Syntax.Expressions.at(id);
        TDepth depth(*this, syntax.Position);
        if (syntax.Distinct || syntax.Window)
            throw TFrontendError(syntax.Position, "scalar callable invocation cannot use DISTINCT or OVER");
        const bool named = syntax.Kind == ESyntaxKind::Call;
        const auto callable = named ? LookupBinding(Bindings[id], context, syntax.Position)
            : BindingValue(syntax.Arguments.at(0), context);
        std::vector<TBindingValue> arguments;
        for (std::size_t i = named ? 0 : 1; i < syntax.Arguments.size(); ++i)
            arguments.push_back(BindingValue(syntax.Arguments[i], context));
        if (const auto* external = std::get_if<TGlobalBindings::TExternal>(&callable); external && external->Callable) {
            std::vector<TExprId> scalars;
            for (const auto& argument : arguments) scalars.push_back(ScalarValue(argument, syntax.Position));
            const auto result = external->Body->Apply(Program.Expressions, scalars, syntax.Position);
            CheckSize(syntax.Position);
            return result;
        }
        if (const auto* name = std::get_if<std::string>(&callable)) {
            std::vector<TExprId> scalars;
            for (const auto& argument : arguments) scalars.push_back(ScalarValue(argument, syntax.Position));
            return CallScalar(*name, scalars, syntax.Position);
        }
        if (const auto* regex = std::get_if<TBoundRegex>(&callable)) {
            if (arguments.size() != 1) throw TFrontendError(syntax.Position, "regular expression matcher requires one input");
            return Program.Expressions.Regex(ScalarValue(arguments.front(), syntax.Position), regex->Pattern, regex->FullMatch, syntax.Position);
        }
        const auto* closure = std::get_if<TBoundLambda>(&callable);
        if (!closure) throw TFrontendError(syntax.Position, "invocation requires a callable value");
        const auto& lambda = Syntax.Expressions[closure->Syntax];
        const auto& names = *lambda.Lambda;
        if (names.Parameters.size() != arguments.size())
            throw TFrontendError(syntax.Position, "lambda argument count mismatch");
        const auto& bindings = Bindings.LambdaBindings(closure->Syntax);
        if (bindings.size() > Settings.MaxNodes - std::min(BoundLocalValues, Settings.MaxNodes) ||
            BindingFrames.size() >= Settings.MaxNodes)
            throw TResourceLimitError(syntax.Position, "lambda expansion limit exceeded");
        BoundLocalValues += bindings.size();
        const auto frame = BindingFrames.size();
        BindingFrames.push_back({closure->Frame, {}});
        BindingFrames[frame].Values.reserve(bindings.size());
        for (std::size_t i = 0; i < arguments.size(); ++i)
            BindingFrames[frame].Values.emplace(bindings[i], std::move(arguments[i]));
        const TScope empty;
        TExprContext body{empty};
        body.AllowSubqueries = false;
        body.BindingFrame = frame;
        for (std::size_t i = 0; i < names.Locals.size(); ++i) {
            // Binding a nested call may append frames. Reacquire by index.
            auto value = BindingValue(lambda.Arguments[i], body);
            BindingFrames[frame].Values.emplace(bindings[names.Parameters.size() + i], std::move(value));
        }
        return BindingValue(lambda.Arguments.back(), body);
    }

    // Callable values bind to native function names at declaration time. The
    // scalar factory owns signatures and semantics; operator translation never
    // inspects SQL syntax or resolves a backend UDF.
    std::optional<std::string_view> CallableName(TSyntaxId id) const {
        const auto& syntax = Syntax.Expressions.at(id);
        if (syntax.Kind != ESyntaxKind::Call || Lower(syntax.Text) != "udf") return {};
        if (syntax.Arguments.size() != 1 || syntax.Distinct || syntax.Window)
            throw TUnsupportedFeature(EFeature::Function, syntax.Position, "native Udf requires one qualified function name");
        const auto& name = Syntax.Expressions[syntax.Arguments[0]];
        if (name.Kind != ESyntaxKind::Column || name.Text.find("::") == std::string::npos)
            throw TFrontendError(name.Position, "Udf requires a qualified function name");
        CheckFunctionProfile(Lower(name.Text), name.Position);
        return name.Text;
    }

    std::unordered_map<const TCatalogTable*, TGeneratedValues> Generated;
    std::size_t GeneratedNodes = 0;
    const TParsedQuery& Syntax;
    const TTableResolver& Catalog;
    const TBindSettings& Settings;
    // One catalog snapshot per resolved path; source aliases own their
    // bindings, while repeated sources share the immutable column metadata.
    std::unordered_map<TTableAddress, TCatalogSnapshot, TTableAddressHash> Tables;
    const TGlobalBindings& Bindings;
    TBoundProgram Program;
    std::vector<TColumnId> ColumnOrigins;
    std::vector<std::uint32_t> Canonical;
    std::vector<std::uint32_t> CanonicalWindows;
    std::vector<bool> ContainsAggregate;
    std::vector<bool> ContainsWindow;
    std::vector<std::optional<TBindingValue>> Values;
    std::vector<TBindingFrame> BindingFrames;
    std::size_t BoundLocalValues = 0;
    std::unordered_map<const TSelect*, TQueryResult> ReusableQueries;
    std::unordered_map<const TCteBinding*, TExprContext*> CteContexts;
    std::unordered_set<std::string> Declared;
    std::unordered_set<const TSelect*> ActiveQueries;
    std::size_t Depth = 0;
};

} // namespace

TBoundProgram NDetail::Bind(const TParsedQuery& syntax, const TTableResolver& catalog, const TBindSettings& settings) {
    const TGlobalBindings bindings(syntax, settings);
    return TBinder(syntax, catalog, settings, bindings).Run();
}

TBoundProgram NDetail::Compile(std::string_view sql, const TTableResolver& catalog, const TBindSettings& bindSettings, const TParseSettings& parseSettings) {
    return Bind(Parse(sql, parseSettings), catalog, bindSettings);
}

namespace {

void CheckSyntax(const TParsedQuery& syntax) {
    for (const auto& node : syntax.Expressions) {
        if (node.NamePath) {
            const auto offset = node.NamePath;
            if (node.Kind != ESyntaxKind::Column || offset > syntax.NamePaths.size() ||
                syntax.NamePaths[offset - 1] > syntax.NamePaths.size() - offset)
                throw TFrontendError(node.Position, "invalid parsed name path");
            std::size_t previous = 0;
            for (const auto separator : syntax.NameSeparators(node)) {
                if (separator < previous || separator >= node.Text.size() || node.Text[separator] != '.')
                    throw TFrontendError(node.Position, "invalid parsed name separator");
                previous = separator + 1;
            }
        }
        if (node.Kind == ESyntaxKind::Case && node.Text == "implicit_else")
            throw TFrontendError(node.Position, "CASE requires explicit ELSE");
        if (node.Kind == ESyntaxKind::Real && (node.Text.ends_with('p') || node.Text.ends_with('P')))
            throw TUnsupportedFeature(EFeature::Type, node.Position, "PostgreSQL numeric literals are outside native-read-v1");
        if (node.Kind == ESyntaxKind::Binary &&
            (node.Text == "+" || node.Text == "-" || node.Text == "*" || node.Text == "/" || node.Text == "%" || node.Text == "||")) {
            for (auto arg : node.Arguments) if (syntax.Expressions[arg].Kind == ESyntaxKind::Null)
                throw TUnsupportedFeature(EFeature::Expression, node.Position, "bare NULL arithmetic is outside native-read-v1");
        }
        if (node.Kind == ESyntaxKind::Call) {
            const auto name = Lower(node.Text);
            CheckFunctionProfile(name, node.Position);
        }

    }
}

void CheckProgram(const TBoundProgram& program) {
    for (const auto& column : program.Columns) if (column.Type.Kind == ETypeKind::Calendar)
        throw TUnsupportedFeature(EFeature::Type, {}, "native calendar values must remain inside scalar expressions; use DateTime::MakeDate/MakeDatetime/MakeTimestamp before a relational boundary");
    for (const auto& node : program.Expressions.Nodes()) {
        if ((node.Op >= EOpcode::Add && node.Op <= EOpcode::Modulo) || node.Op == EOpcode::Concat ||
            (node.Op == EOpcode::Coalesce && node.Type.Kind == ETypeKind::Decimal)) {
            const bool integerOperand = (node.Op == EOpcode::Multiply || node.Op == EOpcode::Divide) &&
                program.Expressions[node.Arguments[1]].Type.IsIntegral();
            for (auto id : node.Arguments) {
                while (program.Expressions[id].Op == EOpcode::Convert) id = program.Expressions[id].Arguments[0];
                const auto from = program.Expressions[id].Type;
                if (from.Kind == ETypeKind::Null && node.Op != EOpcode::Coalesce)
                    throw TUnsupportedFeature(EFeature::Expression, node.Position, "bare NULL arithmetic is outside native-read-v1");
                const bool coalesceWidening = node.Op == EOpcode::Coalesce && from.Kind == ETypeKind::Decimal &&
                    from.Scale == node.Type.Scale && from.Precision <= node.Type.Precision;
                if (node.Type.Kind == ETypeKind::Decimal && !integerOperand && !coalesceWidening && from.Kind != ETypeKind::Null && from.Required() != node.Type.Required())
                    throw TUnsupportedFeature(EFeature::Type, node.Position,
                        "native Decimal arithmetic requires matching types except integer factors/divisors; COALESCE additionally allows same-scale precision widening");
            }
        }
        if (node.Op == EOpcode::Cast) {
            auto from = program.Expressions[node.Arguments[0]].Type;
            auto target = node.Type;
            if (from.Kind == ETypeKind::List && target.Kind == ETypeKind::List) {
                from = program.Expressions.ListItem(from);
                target = program.Expressions.ListItem(target);
            }
            const bool temporal = (from.IsDate() || from.IsInterval()) &&
                (target.IsDate() || target.IsInterval() || target.IsNumeric());
            if (!from.IsIntegral() && !from.IsString() && from.Kind != ETypeKind::Null &&
                from.Kind != ETypeKind::Bool && from.Kind != ETypeKind::Float && from.Kind != ETypeKind::Double && from.Kind != ETypeKind::Decimal && from.Kind != ETypeKind::Uuid && !temporal)
                throw TUnsupportedFeature(EFeature::Expression, node.Position, "runtime cast input is outside the native scalar profile");
            if (target.IsString()) {
                if (!from.IsString() && !from.IsIntegral() && from.Kind != ETypeKind::Bool && from.Kind != ETypeKind::Decimal && from.Kind != ETypeKind::Uuid && from.Kind != ETypeKind::Null) {
                    throw TUnsupportedFeature(EFeature::Expression, node.Position,
                        "native-read-v1 does not implement this type's YQL text formatting");
                }
            }
        }
        if ((node.Op == EOpcode::Lower || node.Op == EOpcode::Upper) && node.Type.Kind != ETypeKind::String)
            throw TUnsupportedFeature(EFeature::Function, node.Position, "native ASCII case functions require String input");
    }
    for (const auto& relation : program.Relations) {
        const auto documentKey = [&](TColumnId id) {
            if (program.Columns.at(id).Type.IsDocument())
                throw TUnsupportedFeature(EFeature::Type, relation.Position, "JSON/YSON grouping and ordering are outside the native scalar profile");
        };
        for (auto key : relation.GroupKeys) documentKey(key);
        for (const auto& key : relation.SortKeys) documentKey(key.Column);
        for (const auto& capture : relation.Captures)
            if (program.Columns.at(capture.Local).Type.IsDocument())
                throw TUnsupportedFeature(EFeature::Subquery, relation.Position, "correlated JSON/YSON captures are outside the native scalar profile");
        for (const auto& aggregate : relation.Aggregates)
            if (program.Columns.at(aggregate.Input).Type.IsDocument() && (aggregate.Function != "count" || aggregate.Distinct))
                throw TUnsupportedFeature(EFeature::Function, relation.Position, "native JSON/YSON aggregation supports only COUNT");
        for (const auto& function : relation.WindowFunctions)
            if (function.Input && program.Columns.at(*function.Input).Type.IsDocument() && function.Function != "count")
                throw TUnsupportedFeature(EFeature::Function, relation.Position, "native JSON/YSON windows support only COUNT");
        if (relation.Kind == ERelation::Join && (relation.JoinKind == EJoinKind::Full || relation.JoinKind == EJoinKind::Exclusion) && relation.Predicate) {
            // The existing full-join backend cannot execute residual filters.
            // Only admit predicates that become column equality keys; leave
            // unsupported forms to whole-query legacy-optimizer delegation.
            const auto& arena = program.Expressions;
            auto stripLift = [&](TExprId id) {
                while (arena[id].Op == EOpcode::Convert && arena[id].Type.Required() == arena[arena[id].Arguments[0]].Type.Required())
                    id = arena[id].Arguments[0];
                return id;
            };
            const auto& leftColumns = program.Relations[relation.Inputs[0]].Columns;
            const auto& rightColumns = program.Relations[relation.Inputs[1]].Columns;
            const std::unordered_set<TColumnId> left(leftColumns.begin(), leftColumns.end()), right(rightColumns.begin(), rightColumns.end());
            std::vector<TExprId> pending{*relation.Predicate};
            std::unordered_set<TExprId> visited;
            while (!pending.empty()) {
                const auto id = stripLift(pending.back()); pending.pop_back();
                if (!visited.insert(id).second) continue;
                const auto& node = arena[id];
                if (node.Op == EOpcode::And) {
                    pending.insert(pending.end(), node.Arguments.begin(), node.Arguments.end());
                    continue;
                }
                if (node.Op == EOpcode::Equal) {
                    const auto& a = arena[stripLift(node.Arguments[0])];
                    const auto& b = arena[stripLift(node.Arguments[1])];
                    if (a.Op == EOpcode::Column && b.Op == EOpcode::Column &&
                        ((left.contains(a.ColumnId) && right.contains(b.ColumnId)) || (right.contains(a.ColumnId) && left.contains(b.ColumnId)))) continue;
                }
                throw TUnsupportedFeature(EFeature::Backend, relation.Position,
                    "FULL JOIN requires only column equality keys in the native backend profile");
            }
        }
        if (relation.Kind == ERelation::Aggregate && relation.Aggregates.size() > 1) {
            const bool distinct = std::any_of(relation.Aggregates.begin(), relation.Aggregates.end(), [](const auto& agg) { return agg.Distinct; });
            if (distinct && std::any_of(relation.Aggregates.begin(), relation.Aggregates.end(), [](const auto& agg) { return agg.Function == "stddev_samp"; }))
                throw TUnsupportedFeature(EFeature::AggregateCombination, relation.Position,
                    "mixed DISTINCT aggregates with sample standard deviation are outside the native backend profile");

        }
    }
}

class TTableCollector {
public:
    TTableCollector(const TParsedQuery& syntax, const TBindSettings& settings, const TGlobalBindings& bindings)
        : Syntax(syntax), Settings(settings), Bindings(bindings) {}
    std::vector<TTableRequest> Run() {
        for (const auto& statement : Syntax.Statements) {
            if (statement.Kind == EStatementKind::Query) Select(*statement.Query);
            if (statement.Write) {
                const auto& write = *statement.Write;
                Source(write.Target);
                if (write.Input) Select(*write.Input);
                for (const auto& row : write.Rows) for (auto value : row) Expression(value);
                Expression(write.Where);
                for (const auto& assignment : write.Assignments) Expression(assignment.Expression);
            }
        }
        return std::move(Tables);
    }
private:
    void Expression(TSyntaxId root) {
        if (root == NoSyntax) return;
        const auto& expression = Syntax.Expressions[root];
        if (expression.Arguments.empty() && !expression.Query && !expression.Window && expression.Filter == NoSyntax &&
            Bindings.Definition(Bindings[root]) == NoSyntax) return;
        std::vector<TSyntaxId> pending{root};
        std::unordered_set<TSyntaxId> seen;
        while (!pending.empty()) {
            const auto id = pending.back(); pending.pop_back();
            if (!seen.insert(id).second) continue;
            const auto& node = Syntax.Expressions[id];
            if (++Nodes > Settings.MaxNodes) throw TResourceLimitError(node.Position, "table discovery expansion limit exceeded");
            pending.insert(pending.end(), node.Arguments.begin(), node.Arguments.end());
            if (node.Filter != NoSyntax) pending.push_back(node.Filter);
            if (node.Window) WindowChildren(Bindings.Window(*node.Window), pending);
            if (node.Query) Select(*node.Query);
            if (const auto query = Bindings.QueryReference(id)) {
                NamedQuery(*query, node.Position);
            } else if (const auto expression = Bindings.Definition(Bindings[id]); expression != NoSyntax) {
                pending.push_back(expression);
            }
        }
    }
    void NamedQuery(const TSelect& query, TSourcePosition pos) {
        if (!Active.insert(&query).second) throw TFrontendError(pos, "recursive query binding");
        Select(query);
        Active.erase(&query);
    }
    void Source(const TSource& source) {
        if (source.ParameterTable) return;
        auto query = Bindings.Query(source);
        if (query) {
            NamedQuery(*query, source.Position);
        } else if (!source.NameIsBinding) {
            const auto& address = Bindings.PhysicalAddress(source);
            if (Seen.insert(address).second) Tables.push_back({address, source.Position});
        }
    }
    void Select(const TSelect& query) {
        if (++Depth > Settings.MaxExpansionDepth || ++Nodes > Settings.MaxNodes)
            throw TResourceLimitError(query.Position, "table discovery expansion limit exceeded");
        if (query.Source) Source(*query.Source);
        for (const auto& join : query.Joins) { Source(join.Source); Expression(join.Predicate); }
        for (const auto& item : query.Projection) Expression(item.Expression);
        Expression(query.Where);
        for (const auto& item : query.GroupBy) Expression(item.Expression);
        Expression(query.Having);
        for (const auto& item : query.OrderBy) Expression(item.Expression);
        Expression(query.Limit);
        Expression(query.Offset);
        for (const auto& branch : query.SetInputs) Select(*branch);
        --Depth;
    }
    const TParsedQuery& Syntax;
    const TBindSettings& Settings;
    const TGlobalBindings& Bindings;
    std::unordered_set<const TSelect*> Active;
    std::unordered_set<TTableAddress, TTableAddressHash> Seen;
    std::vector<TTableRequest> Tables;
    std::size_t Depth = 0, Nodes = 0;
};

} // namespace

TParsedQuery ParseNative(std::string_view sql, const TCompilerSettings& settings) {
    auto parsing = settings.Parsing;
    // Translation settings apply only to the leading directive-comment lines.
    // Ordinary comments or the first SQL token end this prefix. A string value
    // containing "--!" never changes language settings.
    auto prefix = sql;
    std::uint32_t line = 1;
    while (!prefix.empty()) {
        const auto end = prefix.find_first_of("\r\n");
        auto text = prefix.substr(0, end);
        while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) text.remove_prefix(1);
        while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) text.remove_suffix(1);
        if (!text.empty()) {
            if (!text.starts_with("--!")) break;
            text.remove_prefix(3);
            const TSourcePosition pos{sql.size() - prefix.size(), line, 1};
            if (text.find_first_of(" \t") != text.npos) throw TFrontendError(pos, "bad translation settings format");
            if (text == "ansi_lexer") parsing.AnsiQuotes = true;
            else if (text.empty() || text == "syntax_v1" || text == "antlr4_parser") {}
            else if (text.starts_with("syntax_"))
                throw TUnsupportedFeature(EFeature::LanguageVersion, pos, "native frontend supports SQL v1");
            else throw TFrontendError(pos, "unknown SQL translation setting: " + std::string(text));
        }
        if (end == prefix.npos) break;
        prefix.remove_prefix(end + 1);
        ++line;
    }
    if (settings.Parser == EParser::Handwritten) return Parse(sql, parsing);
    if (settings.Parser == EParser::Fast) {
        try {
            return Parse(sql, parsing);
        } catch (const TResourceLimitError&) {
            throw;
        } catch (const TFrontendError&) {
            // This is parser dispatch, not optimizer fallback. The grammar
            // parser supplies authoritative rejection/unsupported diagnostics.
            return ParseGrammar(sql, parsing);
        }
    }
    auto grammar = ParseGrammar(sql, parsing);
    if (settings.Parser == EParser::Verify) {
        auto handwritten = Parse(sql, parsing);
        if (SyntaxFingerprint(grammar) != SyntaxFingerprint(handwritten))
            throw std::logic_error("native parser differential mismatch");
        return handwritten;
    }
    return grammar;
}

std::vector<TTableRequest> CollectTables(const TParsedQuery& syntax, const TBindSettings& settings) {
    const TGlobalBindings bindings(syntax, settings);
    return TTableCollector(syntax, settings, bindings).Run();
}

struct TDiscoveredQuery::TImpl {
    TImpl(TParsedQuery syntax, TBindSettings settings)
        : Syntax(std::move(syntax)), Settings(std::move(settings)), Bindings(Syntax, Settings),
          Tables(TTableCollector(Syntax, Settings, Bindings).Run()) {}

    const TParsedQuery Syntax;
    const TBindSettings Settings;
    const TGlobalBindings Bindings;
    const std::vector<TTableRequest> Tables;
};

TDiscoveredQuery DiscoverNative(TParsedQuery syntax, TBindSettings settings) {
    CheckSyntax(syntax);
    return TDiscoveredQuery(std::make_shared<TDiscoveredQuery::TImpl>(std::move(syntax), std::move(settings)));
}

std::span<const TTableRequest> TDiscoveredQuery::Tables() const { return Impl->Tables; }
bool TDiscoveredQuery::UsedGeneratedParser() const { return Impl->Syntax.UsedGeneratedParser; }

TCompiledProgram BindNative(const TDiscoveredQuery& query, const TTableResolver& catalog) {
    const auto& data = *query.Impl;
    auto program = TBinder(data.Syntax, catalog, data.Settings, data.Bindings).Run();
    CheckProgram(program);
    return TCompiledProgram(std::move(program));
}

TRowExpression CompileRowExpression(std::string_view sql, std::span<const TCatalogColumn> columns,
    TType target, const TParseSettings& settings) {
    auto parsed = [&] {
        try { return ParseExpression(sql, settings); }
        catch (const TResourceLimitError&) { throw; }
        catch (const TFrontendError&) { return ParseGrammarExpression(sql, settings); }
    }();
    TParsedQuery syntax{std::move(parsed.Expressions), {}};
    syntax.NamePaths = std::move(parsed.NamePaths);
    CheckSyntax(syntax);
    for (const auto& node : syntax.Expressions) {
        if (node.Kind == ESyntaxKind::Variable || (node.Kind == ESyntaxKind::Call && node.Text.starts_with('$')))
            throw TFrontendError(node.Position, "row expressions cannot reference query bindings");
    }
    TBindSettings binding;
    binding.MaxNodes = settings.MaxNodes;
    const TTableResolver noTables = [](const TTableAddress&, TSourcePosition pos) -> TCatalogTable {
        throw TFrontendError(pos, "row expressions cannot read tables");
    };
    const TGlobalBindings bindings(syntax, binding);
    auto [program, root] = TBinder(syntax, noTables, binding, bindings).RunExpression(parsed.Root, columns, target);
    CheckProgram(program);
    root = program.Expressions.FoldConstants(std::span<const TExprId>(&root, 1)).front();
    TRowExpression result{std::move(program.Expressions), root, {}, {}};
    result.Columns.reserve(columns.size());
    for (const auto& column : columns) result.Columns.push_back(column.Name);
    // The arena is append-only: folded-away branches still have nodes, but
    // must not become row dependencies or trigger stored-value lookups.
    std::vector<bool> visited(result.Expressions.Size());
    std::vector<bool> used(columns.size());
    std::vector<TExprId> pending{root};
    visited[root] = true;
    while (!pending.empty()) {
        const auto id = pending.back(); pending.pop_back();
        const auto& node = result.Expressions[id];
        if (node.Op == EOpcode::Column) used.at(node.ColumnId) = true;
        for (const auto child : node.Arguments) if (!visited[child]) {
            visited[child] = true;
            pending.push_back(child);
        }
    }
    for (TColumnId id = 0; id < used.size(); ++id) if (used[id]) result.Dependencies.push_back(id);
    return result;
}

TCompiledProgram BindNative(const TParsedQuery& syntax, const TTableResolver& catalog, const TBindSettings& settings) {
    CheckSyntax(syntax);
    auto program = NDetail::Bind(syntax, catalog, settings);
    CheckProgram(program);
    return TCompiledProgram(std::move(program));
}

TCompileResult CompileNative(std::string_view sql, const TTableResolver& catalog, const TCompilerSettings& settings) {
    if (settings.Mode == ECompilerMode::Legacy) return {};
    try {
        return {BindNative(ParseNative(sql, settings), catalog, settings.Binding), {}};
    } catch (const TUnsupportedFeature& error) {
        if (settings.Mode == ECompilerMode::NativeRequired) throw;
        return {{}, TUnsupported{error.Feature, error.Position, error.what()}};
    }
}

std::string_view SupportManifest() {
    return R"manifest({"profile":"native-read-v1","default_parser":"fast","parser_dispatch":"handwritten subset; shared-grammar-antlr4 for unhandled syntax",
"statements":["select","declare","scalar-binding","query-binding","scalar-lambda","insert","upsert","replace","update","delete"],
"predicates":["BETWEEN and NOT BETWEEN, including SYMMETRIC/ASYMMETRIC with three-valued and unordered comparison semantics; IN COMPACT uses native membership evaluation"],
"scalar_extrema":["MinOf/MaxOf and Least/Greatest over comparable native scalars except extended temporal values; common types, NULL propagation and scalar NaN handling"],
"scalar_collections":["fixed tuple/list constructors and declared or supplied scalar List parameters for IN; checked dynamic key conversion and ANSI/legacy NULL rules; integral ListFromRange, flat scalar list casts using scalar conversion rules, String/Utf8 ListConcat, ListLength and ListHasItems; bounded constant materialization and lazy runtime ranges; not relational columns"],
"row_parameters":["AS_TABLE of declared or supplied List<Struct<scalar fields>> parameters; nullable scalar fields and empty structs; no nested containers or optional row/list containers"],
"index_views":["ready synchronous row indexes, including unique indexes; covering reads and nullable-primary-key lookups; shared named view producers materialize once; no async/specialized indexes or VIEW PRIMARY KEY"],
"relations":["read","project","filter","on-join","using-join","aggregate","window","sort","limit","union-all","union","intersect","except","intersect-all","except-all","scalar-subquery","correlated-scalar-subquery","exists-subquery","in-subquery","named-query-in","effect"],
"types":["Bool","Int8","Int16","Int32","Int64","Uint8","Uint16","Uint32","Uint64","Float","Double","String","Utf8","Date","Datetime","Timestamp","Interval","Date32","Datetime64","Timestamp64","Interval64","Uuid","Decimal","Json","JsonDocument","Yson","Null","scalar optionals, including explicit nested optional casts"],
"aggregates":["count","count_if","sum","avg","min","max","min_by","max_by","some","stddev","stddev_sample","stddev_samp","stddev_s","stddevsample","stddevsamp","stddevs"],
"effects":["VALUES and SELECT inputs; predicate UPDATE/DELETE and explicit input rows; ordered RETURNING; source-order dependencies and shared named producers; literal and sequence defaults followed by native stored generated expressions at emission; INSERT OR REVERT checks supplied values for primary-key and unique-index conflicts before optimization; virtual generated columns, generated writes requiring stored dependency lookup, INSERT OR REVERT with defaults or generated columns, and batch writes are unsupported"],
"volatile_operations":["RandomUuid(dependencies)","RandomNumber(dependencies)","YQL::RandomUuid() and YQL::RandomNumber() syntax for query-scoped internal inputs"],
"math_operations":["Math::IsNaN on typed integer/Float/Double inputs","Math::Abs with Double input conversion and nullable AutoMap semantics"],
"grouping":["GROUPING SETS","ROLLUP","CUBE","GROUPING bit mask"],
"scalar_checks":["Unwrap with an optional non-null String/Utf8 error message"],
"json_operations":["Validated j/y string literals and Json/Yson literal constructors; JSON_VALUE on typed Json literals, Json/JsonDocument columns or parameters, default Utf8? result and NULL ON EMPTY/ERROR; selector paths with optional lax/strict mode, members, wildcards and nonnegative Uint32 indexes; no escaped member names, path expressions, PASSING, RETURNING, explicit handlers or JsonDocument constructors; JSON/YSON grouping/order/comparison, DISTINCT, tuple states, non-COUNT aggregates/windows and correlated JSON/YSON captures are unsupported"],
"unicode_operations":["Unicode::ToUpper","Unicode::ToLower"],
"aggregate_filters":["FILTER (WHERE predicate) for supported ordinary and window aggregates; native conditional inputs preserve excluded-row laziness, distinct identities and NULL predicates"],
"windows":["inline OVER","named WINDOW definitions without inheritance","count","sum","avg","min","max","rank","denserank","rownumber","ROWS and RANGE with nonnegative Int32 literal offsets"],
"calendar_operations":["DateTime::Split","DateTime::GetYear","DateTime::MakeDate","DateTime::MakeDatetime","DateTime::MakeTimestamp","DateTime::ShiftMonths","DateTime::ShiftYears","DateTime::IntervalFromDays","DateTime::IntervalFromHours","DateTime::IntervalFromMinutes"],
"pragmas":["warning(disable, issue-code)","OrderedColumns","DisableOrderedColumns","TablePathPrefix","YqlSelect=force","AnsiImplicitCrossJoin","AnsiInForEmptyOrNullableItemsCollections","DisableAnsiInForEmptyOrNullableItemsCollections","AnsiCurrentRow","DisableAnsiCurrentRow","UnicodeLiterals","DisableUnicodeLiterals","ClassicDivision","CheckedOps","YqlSelectAllowUnnamedGroupByExpr","DisableYqlSelectAllowUnnamedGroupByExpr"],
"planner_pragmas":{"prefixes":["ydb","kikimr"],
"boolean":["OptDisableAutoIndexSelection","EnableAutoIndexSelectionForIndexLookupJoin","OptDisableTopSort","OptEnablePredicateExtract","OptEnableInplaceUpdate","OptShuffleElimination","OptShuffleEliminationWithMap","OptShuffleEliminationForAggregation","EnableNewRBOPhysicalStagePeephole","WindowFunctionsV2","OptEnableOlapPushdown","OptEnableOlapPushdownAggregate","OptEnableOlapPushdownProjections","OptEnableOlapPushdownRegexp","DqHashOperatorsUseBlocks","UseBlockHashJoin","DisableBlockExecution"],
"uint32":["CostBasedOptimizationLevel","DqChannelVersion"],
"string":["CostBasedOptimization=on/off/auto","HashJoinMode","OptimizerHints"],
"false_only":["OptFallbackToLegacyOptimizer"],
"rules":["static query settings applied in source order; every occurrence is validated","case-insensitive names; up to four single internal or trailing underscores","empty Boolean values mean true","HashJoinMode and OptimizerHints are validated at the RBO handoff","unknown settings delegate; malformed known values are errors"]},
"restrictions":["sample standard deviation supports integer/Float/Double inputs, ordinary aggregates and sole DISTINCT aggregates; Decimal, windows and mixed DISTINCT combinations delegate","computed GROUP BY keys require aliases or YqlSelectAllowUnnamedGroupByExpr; bare NULL grouping-set keys require a scalar cast; GROUP BY modifiers and hopping windows delegate","CASE requires ELSE","join-star projections require unique output names; duplicate names needing coalescing delegate","FULL JOIN requires only column equality keys; residual predicates delegate to the legacy optimizer","constant LIKE pattern with literal text and % wildcards","casts among integers, Bool, Float/Double, Decimal and String/Utf8; casts to and between Date/Datetime/Timestamp; no Float to 32/64-bit integer; dynamic text parsing to integers, Bool, Float/Double, Decimal and Date","unsigned dynamic LIMIT/OFFSET","integer comparisons preserve signed/unsigned operand types; other numeric operations use SQL rank and wrapping integer conversion","no bare NULL arithmetic; dynamic Substring offsets require Uint8/16/32","Unwrap requires a typed scalar; bare NULL would produce an unsupported Void result","set operands align by name and require unique result names","no correlated INTERSECT ALL/EXCEPT ALL; the backend cannot decorrelate their numbering windows","no subqueries in JOIN ON, GROUP BY keys, ORDER BY expressions, LIMIT/OFFSET, scalar bindings or lambda bodies","outer references after aggregation require explicit grouping","no inherited/DISTINCT windows, correlated window subqueries, GROUPS frames except whole partitions, or frame exclusions; no tuple IN subqueries, named USING keys, DDL, arbitrary UDFs, general containers or PostgreSQL","UTC narrow calendar operations only; calendar intermediates cannot cross relational boundaries; shifts and interval constructors require Int32 or fitting integer literals","no floating or temporal text formatting","Decimal multiplication allows integer factors and division allows integer divisors; COALESCE allows same-scale precision widening; other Decimal arithmetic requires matching types or explicit casts; Decimal constructors require canonical literal digits or inf/nan; text casts preserve runtime exponent parsing and rounding","no system columns or string suffixes other than s/u/j/y"],
"fallback":"whole query to legacy YQL optimizer, only on structured unsupported feature",
"status":"experimental; grammar recognition exceeds execution coverage"})manifest";
}

} // namespace NKikimr::NKqp::NFrontend
