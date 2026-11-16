#include "kqp_name_resolver.h"
#include "kqp_new_parser.h"
#include "kqp_ast_visit.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/sql_types/normalize_name.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
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

// Columns retain source order for star expansion. Relations are tracked
// separately so a local alias shadows an outer relation even when the requested
// column is absent locally. Parent is borrowed only for the recursive visit.
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

    // Apply pragmas in statement order, but start each top-level query with no
    // outer row scope or inherited CTEs. Publish names only after the whole script
    // has resolved successfully.
    std::unique_ptr<TResolvedNames> Run() {
        bool declarationsAllowed = true;
        for (const auto& statement : Script.Statements) {
            if (const auto* pragma = std::get_if<TPragmaStatement>(&statement.Value)) {
                Pragma(*pragma);
            } else if (const auto* binding = std::get_if<TNamedBinding>(&statement.Value)) {
                declarationsAllowed = false;
                Define(*binding);
            } else if (const auto* declaration = std::get_if<TDeclareStatement>(&statement.Value)) {
                const auto name = ParameterName(declaration->Name, statement.Range);
                if (!declarationsAllowed) { Unsupported(statement.Range, "DECLARE outside the script heading"); }
                if (name == "$_") { Error(statement.Range, "Cannot use anonymous name in DECLARE statement"); }
                if (DeclaredTypes.contains(name)) {
                    // The legacy frontend retains the first declaration and
                    // applies the configured duplicate-declaration warning policy.
                    Unsupported(statement.Range, "duplicate declaration warning policy");
                }
                DeclaredTypes[name] = &declaration->Type;
                Result.DeclarationNames[declaration] = name;
            } else if (const auto* write = std::get_if<TWriteStatement>(&statement.Value)) {
                declarationsAllowed = false;
                auto scope = Source(*write->Target, {}, nullptr);
                auto& columns = Result.WriteColumns[write];
                for (const auto& column : write->Columns) { columns.push_back(Name(column)); }
                if (write->Input) { Query(*write->Input, nullptr, {}); }
                for (const auto& row : write->Rows) {
                    for (const auto& value : row) { Expression(*value, {}, {}, {}); }
                }
                auto& returning = Result.ReturningColumns[write];
                for (const auto& column : write->Returning) { returning.push_back(Name(column)); }
                if (write->ReturningAll) {
                    for (const auto& column : Result.Tables.at(write->Target.get())->Columns) { returning.push_back(column.Name); }
                }
                if (write->Where) { Expression(*write->Where, scope, {}, {}); }
                for (const auto& assignment : write->Assignments) {
                    columns.push_back(Name(*assignment.Alias));
                    Expression(*assignment.Expression, scope, {}, {});
                }
            } else {
                declarationsAllowed = false;
                Query(*std::get<TQueryStatement>(statement.Value).Query, nullptr, {});
            }
        }
        return std::make_unique<TResolvedNames>(std::move(Result));
    }

    TVector<std::pair<TString, TString>> CollectTables() {
        for (const auto& statement : Script.Statements) {
            if (const auto* pragma = std::get_if<TPragmaStatement>(&statement.Value)) {
                Pragma(*pragma);
            } else if (const auto* binding = std::get_if<TNamedBinding>(&statement.Value)) {
                CollectExpression(*binding->Value, {});
            } else if (const auto* write = std::get_if<TWriteStatement>(&statement.Value)) {
                CollectSource(*write->Target, {});
                if (write->Input) { CollectQuery(*write->Input, {}); }
                for (const auto& row : write->Rows) {
                    for (const auto& value : row) { CollectExpression(*value, {}); }
                }
                if (write->Where) { CollectExpression(*write->Where, {}); }
                for (const auto& assignment : write->Assignments) { CollectExpression(*assignment.Expression, {}); }
            } else if (std::holds_alternative<TDeclareStatement>(statement.Value)) {
                continue;
            } else {
                CollectQuery(*std::get<TQueryStatement>(statement.Value).Query, {});
            }
        }
        return std::move(TableReferences);
    }

private:
    std::pair<TString, TString> TablePath(const TTable& table) const {
        const auto cluster = table.Cluster ? Name(*table.Cluster) : Settings.DefaultCluster;
        TString path = Name(table.Name);
        if (!path.StartsWith('/')) {
            const auto prefix = Settings.ClusterPathPrefixes.FindPtr(cluster);
            const TString& base = prefix ? *prefix : Settings.TablePathPrefix;
            path = base.empty() ? path : base + (base.EndsWith('/') ? "" : "/") + path;
        }
        return {cluster, path};
    }

    void CollectExpression(const TExpression& expression, const THashSet<TString>& ctes) {
        VisitScalar(expression, [&](const TExpression& node) {
            if (const auto* subquery = std::get_if<TSubquery>(&node.Value)) { CollectQuery(*subquery->Query, ctes); }
            if (const auto* exists = std::get_if<TExists>(&node.Value)) { CollectQuery(*exists->Query, ctes); }
            // Scalar traversal omits callable targets and frame bounds. Lambdas
            // cannot contain native relational queries and are checked by resolution.
            if (const auto* call = std::get_if<TCall>(&node.Value)) {
                CollectExpression(*call->Callable, ctes);
                if (call->Over) {
                    if (const auto* window = std::get_if<TWindowSpecification>(&call->Over->Value)) {
                        CollectFrame(*window, ctes);
                    }
                }
            }
            return true;
        });
    }

    void CollectFrame(const TWindowSpecification& window, const THashSet<TString>& ctes) {
        if (!window.Frame) { return; }
        if (window.Frame->Begin.Offset) { CollectExpression(*window.Frame->Begin.Offset, ctes); }
        if (window.Frame->End && window.Frame->End->Offset) { CollectExpression(*window.Frame->End->Offset, ctes); }
    }

    void CollectSource(const TSource& source, const THashSet<TString>& ctes) {
        if (!source.Hints.empty()) { Unsupported(source.Range, "table hints that may change the schema"); }
        if (const auto* table = std::get_if<TTable>(&source.Value)) {
            if (table->Anonymous || table->Provider || (table->Binding && table->Cluster)) {
                Unsupported(source.Range, "anonymous, provider-qualified or cluster-qualified bound tables");
            }
            // Named queries were visited at their definition, before subsequent
            // path pragmas. A qualified table name never denotes a local CTE.
            if (!table->Binding && (table->Cluster || !ctes.contains(Name(table->Name)))) {
                auto key = TablePath(*table);
                if (SeenTables.insert(key).second) { TableReferences.push_back(std::move(key)); }
            }
        } else if (const auto* join = std::get_if<TJoin>(&source.Value)) {
            if (join->Natural || !join->Using.empty()) { Unsupported(source.Range, "NATURAL and USING column merging"); }
            CollectSource(*join->Left, ctes);
            CollectSource(*join->Right, ctes);
            if (join->On) { CollectExpression(*join->On, ctes); }
        } else if (const auto* expression = std::get_if<TExpressionSource>(&source.Value)) {
            CollectExpression(*expression->Expression, ctes);
        } else {
            CollectQuery(*std::get<TDerivedTable>(source.Value).Query, ctes);
        }
        if (source.Sampling) {
            CollectExpression(*source.Sampling->Rate, ctes);
            if (source.Sampling->Seed) { CollectExpression(*source.Sampling->Seed, ctes); }
        }
    }

    void CollectGroup(const TGroupingElement& group, const THashSet<TString>& ctes) {
        if (group.Expression.Expression) { CollectExpression(*group.Expression.Expression, ctes); }
        for (const auto& child : group.Elements) { CollectGroup(child, ctes); }
    }

    void CollectQuery(const TQuery& query, THashSet<TString> ctes) {
        for (const auto& cte : query.With) {
            if (cte.Recursive) { Unsupported(cte.Name.Range, "recursive CTEs"); }
            CollectQuery(*cte.Query, ctes);
            ctes.insert(Name(cte.Name));
        }
        if (const auto* select = std::get_if<TSelect>(&query.Body)) {
            if (select->From) { CollectSource(*select->From, ctes); }
            if (select->Where) { CollectExpression(*select->Where, ctes); }
            for (const auto& group : select->GroupBy) { CollectGroup(group, ctes); }
            for (const auto& window : select->Windows) {
                for (const auto& key : window.Specification.PartitionBy) { CollectExpression(*key.Expression, ctes); }
                for (const auto& key : window.Specification.OrderBy) { CollectExpression(*key.Expression, ctes); }
                CollectFrame(window.Specification, ctes);
            }
            for (const auto& column : select->Columns) { CollectExpression(*column.Expression, ctes); }
            if (select->Having) { CollectExpression(*select->Having, ctes); }
            for (const auto& sort : select->OrderBy) { CollectExpression(*sort.Expression, ctes); }
        } else {
            const auto& set = std::get<TSetOperation>(query.Body);
            CollectQuery(*set.Left, ctes);
            CollectQuery(*set.Right, ctes);
        }
        for (const auto& sort : query.OrderBy) { CollectExpression(*sort.Expression, ctes); }
        if (query.Limit) { CollectExpression(*query.Limit, ctes); }
        if (query.Offset) { CollectExpression(*query.Offset, ctes); }
    }

    TString ParameterName(const TParameter& parameter, const TSourceRange& range) const {
        return "$" + Decode(parameter.Spelling.substr(1), range);
    }

    void Define(const TNamedBinding& binding) {
        // Resolve the RHS before publishing the name. A reassignment captures
        // the preceding definition; it cannot create a recursive binding.
        Expression(*binding.Value, {}, {}, {});
        Result.Definitions[&binding.Name] = binding.Value.get();
        if (const auto* query = std::get_if<TSubquery>(&binding.Value->Value)) {
            Result.NamedQueries[&binding.Name] = query->Query.get();
        } else if (const auto* reference = Result.BindingReferences.FindPtr(binding.Value.get())) {
            if (const auto* query = Result.NamedQueries.FindPtr(*reference)) {
                Result.NamedQueries[&binding.Name] = *query;
            }
        }
        NamedValues[ParameterName(binding.Name, binding.Range)] = &binding.Name;
    }

    [[noreturn]] void Error(const TSourceRange& range, const TString& message) const {
        throw TResolutionError{NYql::TIssue(NYql::TPosition(range.Column + 1, range.Line, Script.File),
            TStringBuilder() << "New YQL name resolution: " << message)};
    }

    [[noreturn]] void Unsupported(const TSourceRange& range, TStringBuf feature) const {
        ythrow TNewYqlParserFallback() << "New YQL name resolution does not support " << feature
            << " at " << Script.File << ":" << range.Line << ":" << range.Column + 1;
    }

    // Decode quoted spelling using the selected dialect without case folding.
    // Identifier case remains significant after escaping has been removed.
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

    // Only pragmas whose binding effects are understood may pass this stage.
    // An unknown setting could change table paths or name visibility, so ignoring
    // it would be less safe than falling back to the existing translator.
    void Pragma(const TPragmaStatement& pragma) {
        if (pragma.Namespace && (AsciiEqualsIgnoreCase(Name(*pragma.Namespace), "ydb")
            || AsciiEqualsIgnoreCase(Name(*pragma.Namespace), "kikimr"))) {
            TString name = Name(pragma.Name);
            if (const auto error = NYql::NormalizeName(
                NYql::TPosition(pragma.Name.Range.Column + 1, pragma.Name.Range.Line, Script.File), name)) {
                Error(pragma.Name.Range, error->GetMessage());
            }
            if (name == "costbasedoptimizationlevel") {
                if (pragma.Values.size() > 1) {
                    Error(pragma.Name.Range, "Expected at most one value in the pragma");
                }
                TMaybe<TString> value = TString(); // A bare pragma passes an empty string, not DEFAULT.
                if (!pragma.Values.empty()) {
                    const auto& argument = pragma.Values.front();
                    if (std::holds_alternative<TParameter>(argument.Value)) {
                        Unsupported(argument.Range, "dynamic CostBasedOptimizationLevel values");
                    } else if (const auto* literal = std::get_if<TLiteral>(&argument.Value);
                        literal && literal->Kind == ELiteralKind::String) {
                        value = Decode(literal->Spelling, argument.Range);
                    } else if (const auto* identifier = std::get_if<TIdentifier>(&argument.Value);
                        identifier && AsciiEqualsIgnoreCase(identifier->Spelling, "default")) {
                        value.Clear();
                    } else {
                        Error(argument.Range, "Expected string, named parameter or 'default' keyword as pragma value for pragma: " + Name(pragma.Name));
                    }
                }
                Result.CostBasedOptimizationPragmas.emplace(&pragma, std::move(value));
                return;
            }
        }
        if (!pragma.Namespace && AsciiEqualsIgnoreCase(Name(pragma.Name), "YqlSelect")) {
            const auto* value = pragma.Values.size() == 1 ? std::get_if<TLiteral>(&pragma.Values[0].Value) : nullptr;
            if (!value || value->Kind != ELiteralKind::String) {
                Unsupported(pragma.Name.Range, "non-literal YqlSelect pragma");
            }
            const auto mode = Decode(value->Spelling, pragma.Values[0].Range);
            if (mode != "force" && mode != "auto") {
                // Explicit disable and malformed values retain legacy behavior.
                Unsupported(pragma.Name.Range, "this YqlSelect mode");
            }
            return;
        }
        if (!pragma.Namespace && AsciiEqualsIgnoreCase(Name(pragma.Name), "YqlSelectAllowUnnamedGroupByExpr")
            && pragma.ValueForm == EPragmaValueForm::None) {
            return;
        }
        // Benchmark preambles explicitly enable comma joins. The AST already
        // represents these as Cross joins, which have the same column scope as
        // explicit CROSS JOIN. Other forms/settings must still request fallback.
        if (!pragma.Namespace && AsciiEqualsIgnoreCase(Name(pragma.Name), "AnsiImplicitCrossJoin")
            && pragma.ValueForm == EPragmaValueForm::None) {
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

    // Append-only allocation keeps IDs stable as nested scopes add columns.
    // Never use a catalog column ID here: the same stored column can occur twice.
    TColumnId AddColumn(TResolvedColumn column) {
        const auto id = Result.Columns.size();
        Result.Columns.push_back(std::move(column));
        return id;
    }

    // Search one scope at a time: ambiguity is local to the nearest matching
    // scope. Qualified lookup must stop at a local relation declaration, even
    // when that relation lacks the field, to prevent accidental outer binding.
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

    // The ANSI lexer can deliver a double-quoted identifier through the literal
    // grammar alternative. Classify it as a name before ordinary literal handling.
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

    // Resolve partition/order expressions in the row scope, but frame offsets
    // in an empty row scope. Binding a window reference here does not implement
    // window evaluation; the type/lowering phases still gate that support.
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

    // Record bindings against the complete AST expression. In particular a.b
    // can be one qualified column reference, not two independently bound names.
    // Subqueries receive this scope for correlation; callable names do not.
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
                if (!std::holds_alternative<TName>(value.Callable->Value) && !ExpressionName(*value.Callable)) {
                    // A named or inline lambda is a value in the lexical scope,
                    // so its target must be bound just like an ordinary argument.
                    visit(value.Callable);
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
                if (LambdaDepth) { Unsupported(expression.Range, "relational queries inside scalar lambdas"); }
                Query(*value.Query, &scope, ctes);
            } else if constexpr (std::is_same_v<T, TLambda>) {
                auto outer = NamedValues;
                ++LambdaDepth;
                Y_DEFER { NamedValues = std::move(outer); --LambdaDepth; };
                THashSet<TString> parameters;
                for (const auto& parameter : value.Parameters) {
                    const auto name = ParameterName(parameter, expression.Range);
                    if (!parameters.insert(name).second) {
                        Error(expression.Range, TStringBuilder() << "Duplicate lambda parameter '" << name << "'");
                    }
                    NamedValues[name] = &parameter;
                }
                // Lambda locals have the same sequential binding rules as the
                // preamble. No implicit row scope is captured by a scalar lambda.
                for (const auto& binding : value.Bindings) { Define(binding); }
                Expression(*value.Body, {}, {}, {});
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
                if (const auto* binding = NamedValues.FindPtr(ParameterName(value, expression.Range))) {
                    Result.BindingReferences[&expression] = *binding;
                }
            }
        }, expression.Value);
    }

    // Resolve both inputs before ON so the condition can reference either side.
    // Only after binding ON is the visible scope narrowed for SEMI/ONLY joins.
    // NATURAL/USING need different column-merging rules and cannot reuse this path.
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
            if (table->Anonymous || table->Provider || (table->Binding && table->Cluster)) {
                Unsupported(source.Range, "anonymous, provider-qualified or cluster-qualified bound tables");
            }
            const auto name = Name(table->Name);
            qualifier = name;
            const auto cte = ctes.find(name);
            if (table->Binding) {
                const auto key = ParameterName(TParameter{table->Name.Spelling}, source.Range);
                const auto* binding = NamedValues.FindPtr(key);
                if (!binding) { Error(source.Range, TStringBuilder() << "Unknown named source '" << key << "'"); }
                const auto* query = Result.NamedQueries.FindPtr(*binding);
                if (!query) { Unsupported(source.Range, "non-query named sources"); }
                // The producer was resolved at its definition, with that point's
                // settings and lexical captures. Each source still gets fresh
                // visible IDs below, just like a WITH CTE or derived table.
                Result.NamedSources[&source] = *query;
                inputs = Result.QueryOutputs.at(*query);
                qualifier = key.substr(1);
            } else if (!table->Cluster && cte != ctes.end()) {
                inputs = cte->second;
            } else {
                const auto [cluster, path] = TablePath(*table);
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
        } else if (const auto* expression = std::get_if<TExpressionSource>(&source.Value)) {
            Expression(*expression->Expression, {}, ctes, {});
            const auto* parameter = std::get_if<TParameter>(&expression->Expression->Value);
            if (!parameter || Result.BindingReferences.contains(expression->Expression.get())) {
                Unsupported(source.Range, "AS_TABLE schema inference without DECLARE");
            }
            const auto* declared = DeclaredTypes.FindPtr(ParameterName(*parameter, expression->Expression->Range));
            if (!declared || !AsciiEqualsIgnoreCase((*declared)->Name.Spelling, "List")
                || (*declared)->OptionalDepth || (*declared)->Arguments.size() != 1) {
                Unsupported(source.Range, "AS_TABLE without a declared list type");
            }
            const auto& row = (*declared)->Arguments.front();
            if (!AsciiEqualsIgnoreCase(row.Name.Spelling, "Struct") || row.OptionalDepth) {
                Error(source.Range, "AS_TABLE expects a list of structs");
            }
            for (const auto& member : row.Members) {
                Y_ENSURE(member.Name);
                const auto& tag = *member.Name;
                TString name;
                if (const auto* id = std::get_if<TIdentifier>(&tag.Value)) { name = Name(*id); }
                else if (const auto* literal = std::get_if<TLiteral>(&tag.Value)) { name = Decode(literal->Spelling, tag.Range); }
                else { Unsupported(tag.Range, "bound struct field names"); }
                inputs.push_back(AddColumn({name, &source, {}, {}, nullptr, {}}));
            }
        } else {
            // Derived tables are not LATERAL: they inherit CTEs but no enclosing row
            // scope. Scalar subqueries use a different path that permits correlation.
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
            // Each derived/CTE use gets fresh visible IDs. Keep Input provenance so
            // types and later RBO bindings can follow the rename without conflating
            // independent references to the same producer.
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

    void Group(const TGroupingElement& group, TScope& scope, const TCtes& ctes) {
        if (group.Expression.Expression) {
            Expression(*group.Expression.Expression, scope, ctes, {});
            if (group.Expression.Alias) {
                const auto id = AddColumn({Name(*group.Expression.Alias), nullptr, {}, {}, group.Expression.Expression.get(), {}});
                Result.GroupAliases[group.Expression.Expression.get()] = id;
                scope.Columns.push_back({id, {}});
            }
        }
        for (const auto& item : group.Elements) {
            Group(item, scope, ctes);
        }
    }

    // Copy the CTE environment so nested WITH declarations do not leak outward.
    // Publish each CTE after resolving its body; later declarations can use it,
    // but a declaration does not bind itself recursively through this path.
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
            // Set operations align by ordinal; output names come from the left
            // operand while fresh identities keep branch bindings independent.
            const auto& set = std::get<TSetOperation>(query.Body);
            const auto left = Query(*set.Left, parent, ctes);
            const auto right = Query(*set.Right, parent, ctes);
            if (left.size() != right.size()) { Error(query.Range, "Set operands have different column counts"); }
            TVector<TColumnId> outputs;
            for (const auto id : left) { outputs.push_back(AddColumn({Result.Columns[id].Name, nullptr, {}, {}, nullptr, {}})); }
            Result.QueryOutputs[&query] = outputs;
            TScope orderScope;
            for (const auto id : outputs) { orderScope.Columns.push_back({id, {}}); }
            for (const auto& sort : query.OrderBy) { Expression(*sort.Expression, orderScope, ctes, {}); }
            if (query.Limit) { Expression(*query.Limit, {}, ctes, {}); }
            if (query.Offset) { Expression(*query.Offset, {}, ctes, {}); }
            return outputs;
        }
        TScope scope = select->From ? Source(*select->From, ctes, parent) : TScope{};
        scope.Parent = parent;
        if (select->Where) {
            Expression(*select->Where, scope, ctes, {});
        }
        for (const auto& group : select->GroupBy) {
            Group(group, scope, ctes);
        }
        // Collect names before visiting definitions, allowing window references
        // to be resolved independently of definition order.
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
        THashSet<TString> usedLabels;
        for (const auto& projection : select->Columns) {
            if (projection.Alias) { usedLabels.insert(Name(*projection.Alias)); }
        }
        size_t projectionIndex = 0;
        for (const auto& projection : select->Columns) {
            const auto ordinal = projectionIndex++;
            const auto& expression = *projection.Expression;
            if (const auto* star = std::get_if<TStar>(&expression.Value)) {
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
                        // An unqualified star over a join exposes qualified
                        // field names, matching SQL v1's derived-row contract.
                        auto name = Result.Columns[column.Id].Name;
                        if (scope.Relations.size() > 1 && !star->Qualifier && !column.Qualifier.empty()) {
                            name = column.Qualifier + "." + name;
                        }
                        outputs.push_back(AddColumn({name, nullptr, {}, {}, &expression, column.Id}));
                    }
                }
            } else {
                Expression(expression, scope, ctes, windows);
                const auto* input = Result.ColumnReferences.FindPtr(&expression);
                TString name;
                if (projection.Alias) {
                    name = Name(*projection.Alias);
                } else if (input) {
                    name = Result.Columns[*input].Name;
                    // SQL v1 retains explicit relation qualification in the
                    // result of a join, even when the field name is unique.
                    if (scope.Relations.size() > 1) {
                        if (const auto* member = std::get_if<TMember>(&expression.Value)) {
                            if (const auto qualifier = ExpressionName(*member->Object)) {
                                name = *qualifier + "." + name;
                            }
                        }
                    }
                } else {
                    auto index = ordinal;
                    do { name = TStringBuilder() << "column" << index++; } while (usedLabels.contains(name));
                }
                usedLabels.insert(name);
                outputs.push_back(AddColumn({name, nullptr, {}, {}, &expression,
                    input ? std::optional(*input) : std::nullopt}));
            }
        }
        THashSet<TString> outputNames;
        for (const auto id : outputs) {
            if (!outputNames.insert(Result.Columns[id].Name).second) {
                Error(query.Range, TStringBuilder() << "Duplicate output column '" << Result.Columns[id].Name << "'");
            }
        }
        // Projection aliases form the nearest ORDER BY scope. Its parent keeps
        // unprojected input columns available for hidden sort dependencies; WHERE
        // was resolved earlier and therefore cannot see these aliases.
        TScope orderScope;
        orderScope.Parent = &scope;
        for (auto id : outputs) {
            orderScope.Columns.push_back({id, {}});
        }
        if (select->Having) { Expression(*select->Having, orderScope, ctes, windows); }
        for (const auto& sort : select->OrderBy) {
            Expression(*sort.Expression, orderScope, ctes, windows);
        }
        const TScope empty;
        // LIMIT/OFFSET use the empty scope: parameters and constants remain legal,
        // but a column name must not bind to the rows of this SELECT.
        if (query.Limit) { Expression(*query.Limit, empty, ctes, {}); }
        if (query.Offset) { Expression(*query.Offset, empty, ctes, {}); }
        Result.QueryOutputs[&query] = outputs;
        return outputs;
    }

    const TScript& Script;
    TNameResolutionSettings Settings;
    const TTableLookup& Lookup;
    TResolvedNames Result;
    THashMap<TString, const TParameter*> NamedValues;
    THashMap<TString, const TTypeName*> DeclaredTypes;
    size_t LambdaDepth = 0;
    THashSet<std::pair<TString, TString>> SeenTables;
    TVector<std::pair<TString, TString>> TableReferences;
};

} // namespace

TTableReferencesResult CollectTableReferences(const TScript& script, TNameResolutionSettings settings) {
    TTableReferencesResult result;
    const TTableLookup lookup;
    try {
        result.Tables = TResolver(script, std::move(settings), lookup).CollectTables();
    } catch (const TResolutionError& error) {
        result.Issues.AddIssue(error.Issue);
    }
    return result;
}

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
