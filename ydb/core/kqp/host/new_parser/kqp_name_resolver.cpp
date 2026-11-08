#include "kqp_name_resolver.h"
#include "kqp_new_parser.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>

#include <type_traits>

namespace NKikimr::NKqp::NNewYql {
namespace {

struct TResolutionError {
    NYql::TIssue Issue;
};

struct TVisibleColumn {
    TColumnId Id;
    TString Qualifier;
};

struct TScope {
    TVector<TVisibleColumn> Columns;
    THashSet<TString> Relations;
    const TScope* Parent = nullptr;
};

using TCtes = THashMap<TString, TVector<TColumnId>>;
using TWindows = THashMap<TString, const TWindowDefinition*>;

class TResolver {
public:
    TResolver(const TScript& script, TNameResolutionSettings settings, const TTableLookup& lookup)
        : Script(script), Settings(std::move(settings)), Lookup(lookup)
    {}

    std::unique_ptr<TResolvedNames> Run() {
        for (const auto& statement : Script.Statements) {
            if (const auto* pragma = std::get_if<TPragmaStatement>(&statement.Value)) {
                Pragma(*pragma);
            } else {
                Query(*std::get<TQueryStatement>(statement.Value).Query, nullptr, {});
            }
        }
        return std::make_unique<TResolvedNames>(std::move(Result));
    }

private:
    [[noreturn]] void Error(const TSourceRange& range, const TString& message) const {
        throw TResolutionError{NYql::TIssue(NYql::TPosition(range.Column + 1, range.Line, Script.File),
            TStringBuilder() << "New YQL name resolution: " << message)};
    }

    [[noreturn]] void Unsupported(const TSourceRange& range, TStringBuf feature) const {
        ythrow TNewYqlParserFallback() << "New YQL name resolution does not support " << feature
            << " at " << Script.File << ":" << range.Line << ":" << range.Column + 1;
    }

    TString Decode(const TString& text, const TSourceRange& range) const {
        if (text.empty() || (text[0] != '`' && text[0] != '\'' && text[0] != '"')) {
            return text;
        }
        const char quote = text[0];
        if (text.size() < 2 || text.back() != quote) {
            Unsupported(range, "suffixed quoted names");
        }
        TString result;
        if (Script.AnsiLexer && quote != '`') {
            for (size_t i = 1; i + 1 < text.size(); ++i) {
                result += text[i];
                if (text[i] == quote && i + 2 < text.size() && text[i + 1] == quote) {
                    ++i;
                }
            }
        } else {
            TStringOutput output(result);
            size_t read = 0;
            const TStringBuf atom(text.data() + 1, text.size() - 1);
            if (NYql::UnescapeArbitraryAtom(atom, quote, &output, &read) != NYql::EUnescapeResult::OK
                || read != atom.size()) {
                Error(range, "Invalid escape in identifier");
            }
        }
        return result;
    }

    TString Name(const TIdentifier& identifier) const {
        return Decode(identifier.Spelling, identifier.Range);
    }

    void Pragma(const TPragmaStatement& pragma) {
        // This controls the legacy translator's representation, not SQL names.
        // The original frontend still validates its value during host import.
        if (!pragma.Namespace && AsciiEqualsIgnoreCase(Name(pragma.Name), "YqlSelect")) {
            return;
        }
        if (pragma.Namespace || !AsciiEqualsIgnoreCase(Name(pragma.Name), "TablePathPrefix")
            || pragma.Values.size() != 1) {
            Unsupported(pragma.Name.Range, "this pragma's binding rules");
        }
        const auto* value = std::get_if<TLiteral>(&pragma.Values[0].Value);
        if (!value || value->Kind != ELiteralKind::String) {
            Unsupported(pragma.Name.Range, "dynamic table path prefixes");
        }
        Settings.TablePathPrefix = Decode(value->Spelling, pragma.Values[0].Range);
    }

    TColumnId AddColumn(TResolvedColumn column) {
        const auto id = Result.Columns.size();
        Result.Columns.push_back(std::move(column));
        return id;
    }

    TColumnId FindColumn(const TScope& scope, const TString& name, const TString& qualifier,
        const TSourceRange& range) const
    {
        for (const TScope* current = &scope; current; current = current->Parent) {
            std::optional<TColumnId> match;
            for (const auto& column : current->Columns) {
                if (Result.Columns[column.Id].Name == name && (qualifier.empty() || column.Qualifier == qualifier)) {
                    if (match) {
                        Error(range, TStringBuilder() << "Ambiguous column '" << name << "'");
                    }
                    match = column.Id;
                }
            }
            if (match) {
                return *match;
            }
            // A local relation shadows an outer relation even if this column
            // does not exist locally. Never bind it to the outer table.
            if (!qualifier.empty() && current->Relations.contains(qualifier)) {
                Error(range, TStringBuilder() << "Unknown column '" << qualifier << "." << name << "'");
            }
        }
        Error(range, TStringBuilder() << "Unknown column '" << (qualifier.empty() ? TString() : qualifier + ".") << name << "'");
    }

    bool HasRelation(const TScope& scope, const TString& name) const {
        for (const TScope* current = &scope; current; current = current->Parent) {
            if (current->Relations.contains(name)) {
                return true;
            }
        }
        return false;
    }

    std::optional<TString> ExpressionName(const TExpression& expression) const {
        if (const auto* name = std::get_if<TName>(&expression.Value); name && !name->Module) {
            return Name(name->Name);
        }
        // The grammar shares STRING_VALUE between strings and ANSI quoted
        // identifiers; their interpretation is a semantic binding decision.
        if (const auto* literal = std::get_if<TLiteral>(&expression.Value);
            literal && literal->Kind == ELiteralKind::String && Script.AnsiLexer && literal->Spelling.StartsWith('"')) {
            return Decode(literal->Spelling, expression.Range);
        }
        return {};
    }

    void Window(const TWindowSpecification& window, const TScope& scope, const TCtes& ctes,
        const TWindows& windows)
    {
        if (window.BaseWindow && !windows.contains(Name(*window.BaseWindow))) {
            Error(window.BaseWindow->Range, TStringBuilder() << "Unknown window '" << Name(*window.BaseWindow) << "'");
        }
        for (const auto& partition : window.PartitionBy) {
            if (partition.Alias) {
                Unsupported(partition.Alias->Range, "window partition aliases");
            }
            Expression(*partition.Expression, scope, ctes, windows);
        }
        for (const auto& sort : window.OrderBy) {
            Expression(*sort.Expression, scope, ctes, windows);
        }
        if (window.Frame) {
            const TScope empty;
            if (window.Frame->Begin.Offset) {
                Expression(*window.Frame->Begin.Offset, empty, ctes, windows);
            }
            if (window.Frame->End && window.Frame->End->Offset) {
                Expression(*window.Frame->End->Offset, empty, ctes, windows);
            }
        }
    }

    void Expression(const TExpression& expression, const TScope& scope, const TCtes& ctes,
        const TWindows& windows)
    {
        const auto visit = [&](const TExpressionPtr& child) {
            if (child) {
                Expression(*child, scope, ctes, windows);
            }
        };
        std::visit([&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TName>) {
                if (value.Module) {
                    Unsupported(expression.Range, "module-valued expressions");
                }
                Result.ColumnReferences[&expression] = FindColumn(scope, Name(value.Name), {}, expression.Range);
            } else if constexpr (std::is_same_v<T, TMember>) {
                const auto object = ExpressionName(*value.Object);
                if (object && HasRelation(scope, *object)) {
                    Result.ColumnReferences[&expression] = FindColumn(scope, Name(value.Member), *object, expression.Range);
                } else {
                    visit(value.Object);
                    Unsupported(expression.Range, "typed struct member resolution");
                }
            } else if constexpr (std::is_same_v<T, TCall>) {
                if (const auto* name = std::get_if<TName>(&value.Callable->Value); name && !name->Module) {
                    static const THashSet<TString> typeConstructors = {"datatype", "optionaltype", "listtype",
                        "dicttype", "tupletype", "structtype", "varianttype", "callabletype", "resourcetype",
                        "taggedtype", "streamtype", "flowtype", "voidtype", "nulltype", "emptylisttype", "emptydicttype"};
                    if (typeConstructors.contains(to_lower(Name(name->Name)))) {
                        // These arguments belong to the type namespace. Until
                        // type-valued expressions are lowered, use the legacy
                        // frontend instead of reporting their names as columns.
                        Unsupported(expression.Range, "type-valued expressions");
                    }
                }
                // Function names are not column references. Function overload
                // and parameter type resolution belong to expression lowering.
                if (!std::holds_alternative<TName>(value.Callable->Value)
                    && !std::holds_alternative<TParameter>(value.Callable->Value) && !ExpressionName(*value.Callable)) {
                    Unsupported(expression.Range, "dynamic call targets");
                }
                for (const auto& argument : value.Arguments) {
                    visit(argument.Expression);
                }
                visit(value.Filter);
                if (value.Over) {
                    if (const auto* name = std::get_if<TIdentifier>(&value.Over->Value)) {
                        const auto it = windows.find(Name(*name));
                        if (it == windows.end()) {
                            Error(name->Range, TStringBuilder() << "Unknown window '" << Name(*name) << "'");
                        }
                        Result.WindowReferences[&*value.Over] = it->second;
                    } else {
                        Window(std::get<TWindowSpecification>(value.Over->Value), scope, ctes, windows);
                    }
                }
            } else if constexpr (std::is_same_v<T, TUnary>) {
                visit(value.Operand);
            } else if constexpr (std::is_same_v<T, TBinary>) {
                visit(value.Left); visit(value.Right);
            } else if constexpr (std::is_same_v<T, TIndex>) {
                visit(value.Object); visit(value.Index);
            } else if constexpr (std::is_same_v<T, TTuple>) {
                for (const auto& item : value.Elements) { visit(item.Expression); }
            } else if constexpr (std::is_same_v<T, TList>) {
                for (const auto& item : value.Elements) { visit(item); }
            } else if constexpr (std::is_same_v<T, TCase>) {
                visit(value.Operand); visit(value.Else);
                for (const auto& branch : value.Branches) { visit(branch.When); visit(branch.Then); }
            } else if constexpr (std::is_same_v<T, TCast>) {
                visit(value.Value);
            } else if constexpr (std::is_same_v<T, TSubquery> || std::is_same_v<T, TExists>) {
                Query(*value.Query, &scope, ctes);
            } else if constexpr (std::is_same_v<T, TBetween>) {
                visit(value.Value); visit(value.Lower); visit(value.Upper);
            } else if constexpr (std::is_same_v<T, TIn>) {
                visit(value.Value); visit(value.Collection);
            } else if constexpr (std::is_same_v<T, TPatternMatch>) {
                visit(value.Value); visit(value.Pattern); visit(value.Escape);
            } else if constexpr (std::is_same_v<T, TStar>) {
                Unsupported(expression.Range, "stars outside projections");
            } else if constexpr (std::is_same_v<T, TLiteral>) {
                if (auto name = ExpressionName(expression)) {
                    Result.ColumnReferences[&expression] = FindColumn(scope, *name, {}, expression.Range);
                }
            } else {
                static_assert(std::is_same_v<T, TParameter>);
            }
        }, expression.Value);
    }

    TScope Source(const TSource& source, const TCtes& ctes, const TScope* parent) {
        if (!source.Hints.empty()) {
            Unsupported(source.Range, "table hints that may change the schema");
        }
        TScope scope;
        scope.Parent = parent;
        if (const auto* join = std::get_if<TJoin>(&source.Value)) {
            if (join->Natural || !join->Using.empty()) {
                Unsupported(source.Range, "NATURAL and USING column merging");
            }
            auto left = Source(*join->Left, ctes, parent);
            auto right = Source(*join->Right, ctes, parent);
            scope = left;
            for (const auto& relation : right.Relations) {
                if (!scope.Relations.insert(relation).second) {
                    Error(source.Range, TStringBuilder() << "Duplicate table alias '" << relation << "'");
                }
            }
            scope.Columns.insert(scope.Columns.end(), right.Columns.begin(), right.Columns.end());
            if (join->On) {
                Expression(*join->On, scope, ctes, {});
            }
            if (join->Kind == EJoinKind::LeftSemi || join->Kind == EJoinKind::LeftOnly) {
                scope = std::move(left);
            } else if (join->Kind == EJoinKind::RightSemi || join->Kind == EJoinKind::RightOnly) {
                scope = std::move(right);
            }
            return scope;
        }

        TString qualifier;
        TVector<TColumnId> inputs;
        if (const auto* table = std::get_if<TTable>(&source.Value)) {
            if (table->Binding || table->Anonymous || table->Provider) {
                Unsupported(source.Range, "bound, anonymous or provider-qualified tables");
            }
            const auto name = Name(table->Name);
            qualifier = name;
            const auto cte = ctes.find(name);
            if (!table->Cluster && cte != ctes.end()) {
                inputs = cte->second;
            } else {
                const auto cluster = table->Cluster ? Name(*table->Cluster) : Settings.DefaultCluster;
                TString path = name;
                if (!path.StartsWith('/')) {
                    const auto prefix = Settings.ClusterPathPrefixes.FindPtr(cluster);
                    const TString& base = prefix ? *prefix : Settings.TablePathPrefix;
                    path = base.empty() ? path : base + (base.EndsWith('/') ? "" : "/") + path;
                }
                // Path aliases and normalization must be resolved by the catalog.
                auto metadata = Lookup(cluster, path);
                if (!metadata) {
                    Error(table->Name.Range, TStringBuilder() << "Table '" << cluster << "." << path << "' does not exist");
                }
                Result.Tables[&source] = metadata;
                for (const auto& column : metadata->Columns) {
                    inputs.push_back(AddColumn({column.Name, &source, metadata, column.Id, nullptr, {}}));
                }
            }
        } else {
            inputs = Query(*std::get<TDerivedTable>(source.Value).Query, nullptr, ctes);
        }
        if (source.Alias) {
            qualifier = Name(*source.Alias);
        }
        if (!qualifier.empty()) {
            scope.Relations.insert(qualifier);
        }
        if (!source.ColumnAliases.empty() && source.ColumnAliases.size() != inputs.size()) {
            Error(source.Range, "Column alias count does not match source column count");
        }
        for (size_t i = 0; i < inputs.size(); ++i) {
            auto id = inputs[i];
            const auto& input = Result.Columns[id];
            const TString name = source.ColumnAliases.empty() ? input.Name : Name(source.ColumnAliases[i]);
            if (input.Source != &source || !source.ColumnAliases.empty()) {
                id = AddColumn({name, &source, {}, {}, nullptr, id});
            }
            scope.Columns.push_back({id, qualifier});
        }
        if (source.Sampling) {
            const TScope empty;
            Expression(*source.Sampling->Rate, empty, ctes, {});
            if (source.Sampling->Seed) {
                Expression(*source.Sampling->Seed, empty, ctes, {});
            }
        }
        return scope;
    }

    void Group(const TGroupingElement& group, const TScope& scope, const TCtes& ctes) {
        if (group.Expression.Alias) {
            Unsupported(group.Range, "GROUP BY aliases");
        }
        if (group.Expression.Expression) {
            Expression(*group.Expression.Expression, scope, ctes, {});
        }
        for (const auto& item : group.Elements) {
            Group(item, scope, ctes);
        }
    }

    TVector<TColumnId> Query(const TQuery& query, const TScope* parent, TCtes ctes) {
        THashSet<TString> localCtes;
        for (const auto& cte : query.With) {
            if (cte.Recursive) {
                Unsupported(cte.Name.Range, "recursive CTEs");
            }
            const auto name = Name(cte.Name);
            if (!localCtes.insert(name).second) {
                Error(cte.Name.Range, TStringBuilder() << "Duplicate CTE '" << name << "'");
            }
            auto columns = Query(*cte.Query, nullptr, ctes);
            if (!cte.Columns.empty()) {
                if (columns.size() != cte.Columns.size()) {
                    Error(cte.Name.Range, "CTE column alias count does not match projection");
                }
                for (size_t i = 0; i < columns.size(); ++i) {
                    columns[i] = AddColumn({Name(cte.Columns[i]), nullptr, {}, {}, nullptr, columns[i]});
                }
            }
            ctes[name] = std::move(columns);
        }
        const auto* select = std::get_if<TSelect>(&query.Body);
        if (!select) {
            Unsupported(query.Range, "set operation column alignment");
        }
        TScope scope = select->From ? Source(*select->From, ctes, parent) : TScope{};
        scope.Parent = parent;
        if (select->Where) {
            Expression(*select->Where, scope, ctes, {});
        }
        for (const auto& group : select->GroupBy) {
            Group(group, scope, ctes);
        }
        TWindows windows;
        for (const auto& window : select->Windows) {
            if (!windows.emplace(Name(window.Name), &window).second) {
                Error(window.Range, TStringBuilder() << "Duplicate window '" << Name(window.Name) << "'");
            }
        }
        for (const auto& window : select->Windows) {
            Window(window.Specification, scope, ctes, windows);
        }
        TVector<TColumnId> outputs;
        for (const auto& projection : select->Columns) {
            const auto& expression = *projection.Expression;
            if (const auto* star = std::get_if<TStar>(&expression.Value)) {
                if (scope.Relations.size() > 1) {
                    Unsupported(expression.Range, "join star output naming");
                }
                const auto qualifier = star->Qualifier ? Name(*star->Qualifier) : TString();
                if (star->Qualifier && !scope.Relations.contains(qualifier)) {
                    Error(expression.Range, TStringBuilder() << "Unknown table alias '" << qualifier << "'");
                }
                if (!select->From) {
                    Error(expression.Range, "SELECT * requires a source");
                }
                auto& expansion = Result.Stars[&expression];
                for (const auto& column : scope.Columns) {
                    if (qualifier.empty() || qualifier == column.Qualifier) {
                        expansion.push_back(column.Id);
                        outputs.push_back(AddColumn({Result.Columns[column.Id].Name, nullptr, {}, {}, &expression, column.Id}));
                    }
                }
            } else {
                Expression(expression, scope, ctes, windows);
                const auto* input = Result.ColumnReferences.FindPtr(&expression);
                const TString name = projection.Alias ? Name(*projection.Alias)
                    : input ? Result.Columns[*input].Name : TStringBuilder() << "column" << outputs.size();
                outputs.push_back(AddColumn({name, nullptr, {}, {}, &expression,
                    input ? std::optional(*input) : std::nullopt}));
            }
        }
        if (select->Having) {
            // SELECT-alias visibility in HAVING is dialect dependent. Defer
            // until its binding rules are implemented instead of misbinding it.
            Unsupported(select->Having->Range, "HAVING alias visibility");
        }
        TScope orderScope;
        orderScope.Parent = &scope;
        for (auto id : outputs) {
            orderScope.Columns.push_back({id, {}});
        }
        for (const auto& sort : select->OrderBy) {
            Expression(*sort.Expression, orderScope, ctes, windows);
        }
        const TScope empty;
        if (query.Limit) { Expression(*query.Limit, empty, ctes, {}); }
        if (query.Offset) { Expression(*query.Offset, empty, ctes, {}); }
        Result.QueryOutputs[&query] = outputs;
        return outputs;
    }

    const TScript& Script;
    TNameResolutionSettings Settings;
    const TTableLookup& Lookup;
    TResolvedNames Result;
};

} // namespace

TNameResolutionResult ResolveNames(const TScript& script, TNameResolutionSettings settings, const TTableLookup& lookup) {
    TNameResolutionResult result;
    try {
        result.Names = TResolver(script, std::move(settings), lookup).Run();
    } catch (const TResolutionError& error) {
        result.Issues.AddIssue(error.Issue);
    }
    return result;
}

} // namespace NKikimr::NKqp::NNewYql
