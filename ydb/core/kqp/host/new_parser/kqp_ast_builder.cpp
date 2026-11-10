#include "kqp_ast_builder.h"
#include "kqp_new_parser.h"

#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Parser.h>
#include <ydb/core/kqp/host/new_parser/grammar/ansi/SQLv1Antlr4Parser.h>

#include <util/generic/hash.h>
#include <util/string/ascii.h>

#include <algorithm>
#include <type_traits>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;
using TContext = antlr4::ParserRuleContext;
using TTree = antlr4::tree::ParseTree;
using TTerminal = antlr4::tree::TerminalNode;

// ANTLR stop indices are inclusive, while our ranges are half-open. EOF has
// no source character, and empty grammar rules must not produce End < Begin.
TSourceRange Range(TTree* tree) {
    auto* context = dynamic_cast<TContext*>(tree);
    auto* start = context ? context->getStart() : dynamic_cast<TTerminal*>(tree)->getSymbol();
    auto* stop = context ? context->getStop() : start;
    const auto begin = start->getStartIndex();
    const auto end = !stop ? begin : stop->getType() == antlr4::Token::EOF
        ? stop->getStartIndex() : stop->getStopIndex() + 1;
    return {begin, std::max(begin, end), ui32(start->getLine()), ui32(start->getCharPositionInLine())};
}

TString Upper(TTree* tree) {
    TString result;
    for (char c : tree->getText()) {
        result.push_back(AsciiToUpper(c));
    }
    return result;
}

TIdentifier Identifier(TTree* tree) {
    return {TString(tree->getText()), Range(tree)};
}

// These helpers inspect direct children only. A recursive search could pick
// a nested SELECT clause or operand instead of the current grammar production.
// Children preserves token order, including repetitions of the same rule.
TContext* Child(TContext* context, size_t rule) {
    for (auto* tree : context->children) {
        if (auto* child = dynamic_cast<TContext*>(tree); child && child->getRuleIndex() == rule) {
            return child;
        }
    }
    return nullptr;
}

TVector<TContext*> Children(TContext* context, size_t rule) {
    TVector<TContext*> result;
    for (auto* tree : context->children) {
        if (auto* child = dynamic_cast<TContext*>(tree); child && child->getRuleIndex() == rule) {
            result.push_back(child);
        }
    }
    return result;
}

TTerminal* Token(TContext* context, size_t type) {
    for (auto* tree : context->children) {
        if (auto* token = dynamic_cast<TTerminal*>(tree); token && token->getSymbol()->getType() == type) {
            return token;
        }
    }
    return nullptr;
}

// A suffix context covers only the suffix, not its base. Extend composite
// ranges to include both operands or the original callee so diagnostics point
// to the whole expression after intermediate CST wrappers are discarded.
template <typename T>
TExpressionPtr MakeExpression(TTree* tree, T value) {
    auto range = Range(tree);
    if constexpr (std::is_same_v<T, TBinary>) {
        range = value.Left->Range;
        range.End = value.Right->Range.End;
    } else if constexpr (std::is_same_v<T, TMember> || std::is_same_v<T, TIndex>) {
        const auto end = range.End;
        range = value.Object->Range;
        range.End = end;
    } else if constexpr (std::is_same_v<T, TCall>) {
        const auto end = range.End;
        range = value.Callable->Range;
        range.End = end;
    }
    return std::make_unique<TExpression>(TExpression{range, std::move(value)});
}

// Both dialects are generated from the same parser rules. Use generated rule
// constants to walk direct children, retaining source order (including mixed
// operators and optional clauses), instead of depending on protobuf field order.
template <typename P>
class TAstBuilder {
public:
    explicit TAstBuilder(const TString& file)
        : File(file)
    {}

    // Only statements with an explicit AST representation may enter the result.
    // Unsupported declarations/effects abort the whole build: dropping a statement
    // could change the meaning of every query that follows it.
    std::unique_ptr<TScript> Build(TContext* root, bool ansiLexer) {
        auto result = std::make_unique<TScript>();
        result->File = File;
        result->AnsiLexer = ansiLexer;
        auto* list = Child(root, P::RuleSql_stmt_list);
        Require(list, root, "PRAGMA ANSI");
        for (auto* statement : Children(list, P::RuleSql_stmt)) {
            auto* core = Child(statement, P::RuleSql_stmt_core);
            TStatement item;
            item.Range = Range(statement);
            if (Token(statement, P::TOKEN_EXPLAIN)) {
                item.Explain = Token(statement, P::TOKEN_PLAN) ? EExplainMode::QueryPlan : EExplainMode::Explain;
            }
            if (auto* select = Child(core, P::RuleSelect_stmt)) {
                item.Value = TQueryStatement{Query(select)};
            } else if (auto* pragma = Child(core, P::RulePragma_stmt)) {
                item.Value = Pragma(pragma);
            } else if (auto* binding = Child(core, P::RuleNamed_nodes_stmt)) {
                Require(item.Explain == EExplainMode::None, statement, "EXPLAIN on a named definition");
                item.Value = NamedBinding(binding);
            } else if (auto* declaration = Child(core, P::RuleDeclare_stmt)) {
                Require(item.Explain == EExplainMode::None && !Token(declaration, P::TOKEN_EQUALS),
                    declaration, "EXPLAIN or default values on DECLARE");
                item.Value = TDeclareStatement{{TString(Child(declaration, P::RuleBind_parameter)->getText())},
                    Type(Child(declaration, P::RuleType_name))};
            } else if (auto* insert = Child(core, P::RuleInto_table_stmt)) {
                item.Value = Write(insert, EWriteKind::Insert);
            } else if (auto* update = Child(core, P::RuleUpdate_stmt)) {
                item.Value = Write(update, EWriteKind::Update);
            } else if (auto* remove = Child(core, P::RuleDelete_stmt)) {
                item.Value = Write(remove, EWriteKind::Delete);
            } else {
                Unsupported(core, "this statement");
            }
            result->Statements.push_back(std::move(item));
        }
        return result;
    }

private:
    TWriteStatement Write(TContext* context, EWriteKind kind) {
        Require(!Token(context, P::TOKEN_BATCH), context, "BATCH writes");
        auto* simple = Child(context, P::RuleSimple_table_ref);
        const bool intoTable = kind == EWriteKind::Insert;
        if (kind == EWriteKind::Insert) {
            Require(!Token(context, P::TOKEN_IGNORE), context, "INSERT OR IGNORE");
            if (Token(context, P::TOKEN_UPSERT)) { kind = EWriteKind::Upsert; }
            if (Token(context, P::TOKEN_REPLACE)) { kind = EWriteKind::Replace; }
            auto* into = Child(context, P::RuleInto_simple_table_ref);
            Require(!Token(into, P::TOKEN_ERASE), into, "ERASE BY");
            simple = Child(into, P::RuleSimple_table_ref);
        }
        Require(!Child(simple, P::RuleTable_hints), simple, "write table hints");
        auto* core = Child(simple, P::RuleSimple_table_ref_core);
        auto* object = Child(core, P::RuleObject_ref);
        Require(object && !Child(object, P::RuleCluster_expr), core, "qualified or dynamic write targets");
        auto* id = Child(object, P::RuleId_or_at);
        Require(!Token(id, P::TOKEN_COMMAT), id, "anonymous write targets");
        auto target = std::make_unique<TSource>();
        target->Range = Range(simple);
        TTable table;
        table.Name = Identifier(id);
        target->Value = std::move(table);
        TWriteStatement result{};
        result.Kind = kind;
        result.Target = std::move(target);
        if (Token(context, P::TOKEN_REVERT)) { result.OnConflict = "revert"; }
        if (auto* returning = Child(context, P::RuleReturning_columns_list)) {
            result.ReturningAll = Token(returning, P::TOKEN_ASTERISK);
            for (auto* column : Children(returning, P::RuleAn_id)) { result.Returning.push_back(Identifier(column)); }
        }
        if (intoTable || Token(context, P::TOKEN_ON)) {
            auto* values = Child(context, P::RuleInto_values_source);
            auto* source = Child(values, P::RuleValues_source);
            auto* select = source ? Child(source, P::RuleSelect_stmt) : nullptr;
            if (auto* columns = Child(values, P::RulePure_column_list)) {
                for (auto* column : Children(columns, P::RuleAn_id)) { result.Columns.push_back(Identifier(column)); }
            }
            if (select) {
                result.Input = Query(select);
            } else {
                auto* rows = source ? Child(source, P::RuleValues_stmt) : nullptr;
                Require(rows && !result.Columns.empty(), values, "DEFAULT VALUES or VALUES without column names");
                for (auto* row : Children(Child(rows, P::RuleValues_source_row_list), P::RuleValues_source_row)) {
                    auto& output = result.Rows.emplace_back();
                    for (auto* expression : Children(Child(row, P::RuleExpr_list), P::RuleExpr)) {
                        output.push_back(Expression(expression));
                    }
                }
            }
        } else {
            Require(!Token(context, P::TOKEN_ON), context, "UPDATE/DELETE ON");
            if (auto* where = Child(context, P::RuleExpr)) { result.Where = Expression(where); }
            if (kind == EWriteKind::Update) {
                auto* clauses = Child(Child(context, P::RuleSet_clause_choice), P::RuleSet_clause_list);
                Require(clauses, context, "multiple-column UPDATE assignments");
                for (auto* clause : Children(clauses, P::RuleSet_clause)) {
                    result.Assignments.push_back({Expression(Child(clause, P::RuleExpr)),
                        Identifier(Child(Child(clause, P::RuleSet_target), P::RuleColumn_name))});
                }
            }
        }
        return result;
    }

    // Preserve the definition rather than substituting text: captures, shadowing
    // and repeated query references are resolved by identity in the next phase.
    TNamedBinding NamedBinding(TContext* context) {
        const auto names = Children(Child(context, P::RuleBind_parameter_list), P::RuleBind_parameter);
        Require(names.size() == 1, context, "tuple destructuring in named definitions");
        TNamedBinding result{Range(context), {TString(names.front()->getText())}, {}};
        if (auto* query = Child(context, P::RuleSelect_unparenthesized_stmt)) {
            result.Value = MakeExpression(query, TSubquery{Query(query)});
        } else {
            result.Value = Expression(Child(context, P::RuleExpr));
        }
        return result;
    }

    TExpressionPtr Lambda(TContext* context) {
        TLambda result;
        // The grammar shares parenthesized expressions with argument lists.
        // Reuse that parser, but accept only parameter names in this position.
        auto parameters = Parenthesis(Child(context, P::RuleSmart_parenthesis));
        const auto addParameter = [&](const TExpression& expression) {
            const auto* parameter = std::get_if<TParameter>(&expression.Value);
            Require(parameter, context, "non-parameter lambda arguments");
            result.Parameters.push_back(*parameter);
        };
        if (const auto* tuple = std::get_if<TTuple>(&parameters->Value)) {
            for (const auto& item : tuple->Elements) {
                Require(!item.Alias, context, "aliased lambda arguments");
                addParameter(*item.Expression);
            }
        } else {
            addParameter(*parameters);
        }
        if (auto* body = Child(context, P::RuleLambda_body)) {
            for (auto* statement : Children(body, P::RuleLambda_stmt)) {
                auto* binding = Child(statement, P::RuleNamed_nodes_stmt);
                Require(binding, statement, "lambda imports");
                result.Bindings.push_back(NamedBinding(binding));
            }
            result.Body = Expression(Child(body, P::RuleExpr));
        } else {
            result.Body = Expression(Child(context, P::RuleExpr));
        }
        return MakeExpression(context, std::move(result));
    }

    [[noreturn]] void Unsupported(TTree* tree, TStringBuf feature) const {
        const auto range = Range(tree);
        ythrow TNewYqlParserFallback() << "New AST does not support " << feature
            << " at " << File << ":" << range.Line << ":" << range.Column + 1;
    }

    // Use this guard for valid grammar constructs whose semantics are not yet
    // represented. Syntax errors have already been reported by ANTLR; this path
    // requests a complete restart through the legacy frontend.
    void Require(bool supported, TTree* tree, TStringBuf feature) const {
        if (!supported) {
            Unsupported(tree, feature);
        }
    }

    // Preserve namespace, value form and quoting. A flag pragma, assignment and
    // list are different syntax even if they currently carry the same values.
    TPragmaStatement Pragma(TContext* context) {
        TPragmaStatement result;
        auto* prefix = Child(context, P::RuleOpt_id_prefix_or_type);
        if (auto* name = Child(prefix, P::RuleAn_id_or_type)) {
            result.Namespace = Identifier(name);
        }
        result.Name = Identifier(Child(context, P::RuleAn_id));
        result.ValueForm = Token(context, P::TOKEN_LPAREN) ? EPragmaValueForm::List
            : Token(context, P::TOKEN_EQUALS) ? EPragmaValueForm::Assignment : EPragmaValueForm::None;
        for (auto* value : Children(context, P::RulePragma_value)) {
            TPragmaValue item;
            item.Range = Range(value);
            if (auto* number = Child(value, P::RuleSigned_number)) {
                item.Value = TLiteral{Child(number, P::RuleReal) ? ELiteralKind::Real : ELiteralKind::Integer,
                    TString(number->getText())};
            } else if (auto* boolean = Child(value, P::RuleBool_value)) {
                item.Value = TLiteral{ELiteralKind::Boolean, TString(boolean->getText())};
            } else if (auto* string = Token(value, P::TOKEN_STRING_VALUE)) {
                item.Value = TLiteral{ELiteralKind::String, TString(string->getText())};
            } else if (auto* binding = Child(value, P::RuleBind_parameter)) {
                item.Value = TParameter{TString(binding->getText())};
            } else {
                item.Value = Identifier(Child(value, P::RuleId));
            }
            result.Values.push_back(std::move(item));
        }
        return result;
    }

    ESetQuantifier Quantifier(TContext* context) const {
        if (!context) {
            return ESetQuantifier::Default;
        }
        if (Token(context, P::TOKEN_DISTINCT)) {
            return ESetQuantifier::Distinct;
        }
        return Token(context, P::TOKEN_ALL) ? ESetQuantifier::All : ESetQuantifier::Default;
    }

    // Normalize operator aliases only; precedence and associativity still come
    // from the surrounding grammar rule and the folding strategy below.
    EBinaryOp BinaryOp(TTree* tree) const {
        static const THashMap<TString, EBinaryOp> operators = {
            {"OR", EBinaryOp::Or}, {"AND", EBinaryOp::And}, {"XOR", EBinaryOp::Xor},
            {"=", EBinaryOp::Equal}, {"==", EBinaryOp::Equal},
            {"!=", EBinaryOp::NotEqual}, {"<>", EBinaryOp::NotEqual},
            {"<", EBinaryOp::Less}, {"<=", EBinaryOp::LessOrEqual},
            {">", EBinaryOp::Greater}, {">=", EBinaryOp::GreaterOrEqual},
            {"ISDISTINCTFROM", EBinaryOp::IsDistinctFrom}, {"ISNOTDISTINCTFROM", EBinaryOp::IsNotDistinctFrom},
            {"+", EBinaryOp::Add}, {"-", EBinaryOp::Subtract}, {"*", EBinaryOp::Multiply},
            {"/", EBinaryOp::Divide}, {"%", EBinaryOp::Modulo}, {"&", EBinaryOp::BitAnd},
            {"|", EBinaryOp::BitOr}, {"^", EBinaryOp::BitXor}, {"<<", EBinaryOp::ShiftLeft},
            {">>", EBinaryOp::ShiftRight}, {"|<<", EBinaryOp::RotateLeft}, {">>|", EBinaryOp::RotateRight},
            {"||", EBinaryOp::Concat}, {"??", EBinaryOp::Coalesce},
        };
        auto it = operators.find(Upper(tree));
        Require(it != operators.end(), tree, "this binary operator");
        return it->second;
    }

    // Left-fold one precedence level in source order. Recursing into each operand
    // handles tighter precedence; this helper must not flatten those subtrees.
    // Operators with special associativity (notably ??) use a separate path.
    TExpressionPtr Fold(TContext* context, size_t operandRule) {
        TExpressionPtr result;
        TTree* op = nullptr;
        for (auto* tree : context->children) {
            auto* child = dynamic_cast<TContext*>(tree);
            if (child && child->getRuleIndex() == operandRule) {
                auto right = Expression(child);
                if (result) {
                    Y_ENSURE(op);
                    result = MakeExpression(context, TBinary{BinaryOp(op), std::move(result), std::move(right)});
                } else {
                    result = std::move(right);
                }
                op = nullptr;
            } else {
                Require(!op, context, "this operator sequence");
                op = tree;
            }
        }
        Require(result && !op, context, "this expression form");
        return result;
    }

    TNamedExpression Named(TContext* context) {
        TNamedExpression result{Expression(Child(context, P::RuleExpr)), {}};
        if (auto* alias = Child(context, P::RuleAn_id_or_type)) {
            result.Alias = Identifier(alias);
        }
        return result;
    }

    // Keep the original spelling, including suffixes and escapes. Numeric range
    // checks and string decoding need semantic settings and occur during lowering.
    TExpressionPtr Literal(TContext* context) {
        ELiteralKind kind;
        if (Child(context, P::RuleInteger)) {
            kind = ELiteralKind::Integer;
        } else if (Child(context, P::RuleReal)) {
            kind = ELiteralKind::Real;
        } else if (Child(context, P::RuleBool_value)) {
            kind = ELiteralKind::Boolean;
        } else if (Token(context, P::TOKEN_STRING_VALUE)) {
            kind = ELiteralKind::String;
        } else if (Token(context, P::TOKEN_NULL_)) {
            kind = ELiteralKind::Null;
        } else if (Token(context, P::TOKEN_BLOB)) {
            kind = ELiteralKind::Blob;
        } else {
            Unsupported(context, "this literal");
        }
        return MakeExpression(context, TLiteral{kind, TString(context->getText())});
    }

    // SQL predicates carry extra operands/modifiers that a binary operator cannot
    // represent. Retain NOT, ESCAPE and SYMMETRIC rather than rewriting predicates
    // before type and NULL semantics are available.
    TExpressionPtr Condition(TContext* context, TExpressionPtr left) {
        const bool negated = Token(context, P::TOKEN_NOT);
        auto values = Children(context, P::RuleEq_subexpr);
        if (auto* match = Child(context, P::RuleMatch_op)) {
            const auto text = Upper(match);
            EPatternOp op = text == "LIKE" ? EPatternOp::Like : text == "ILIKE" ? EPatternOp::ILike
                : text == "GLOB" ? EPatternOp::Glob : text == "REGEXP" ? EPatternOp::Regexp
                : text == "RLIKE" ? EPatternOp::RLike : EPatternOp::Match;
            return MakeExpression(context, TPatternMatch{op, std::move(left), Expression(values[0]),
                values.size() == 2 ? Expression(values[1]) : nullptr, negated});
        }
        if (auto* in = Child(context, P::RuleIn_expr)) {
            return MakeExpression(context, TIn{std::move(left), Expression(in), negated,
                bool(Token(context, P::TOKEN_COMPACT))});
        }
        if (Token(context, P::TOKEN_BETWEEN)) {
            return MakeExpression(context, TBetween{std::move(left), Expression(values[0]), Expression(values[1]),
                negated, bool(Token(context, P::TOKEN_SYMMETRIC))});
        }
        if (values.empty()) {
            Require(Token(context, P::TOKEN_NULL_) || Token(context, P::TOKEN_ISNULL)
                || Token(context, P::TOKEN_NOTNULL), context, "this predicate");
            return MakeExpression(context, TUnary{negated || Token(context, P::TOKEN_NOTNULL)
                ? EUnaryOp::IsNotNull : EUnaryOp::IsNull, std::move(left)});
        }
        Require(values.size() == 1, context, "chained equality comparisons");
        return MakeExpression(context, TBinary{BinaryOp(context->children.front()), std::move(left), Expression(values[0])});
    }

    // Type tags can inhabit different namespaces: a literal, identifier and
    // parameter must remain distinguishable even when their text looks similar.
    TTypeTag TypeTag(TContext* context) {
        TTypeTag result;
        result.Range = Range(context);
        if (auto* binding = Child(context, P::RuleBind_parameter)) {
            result.Value = TParameter{TString(binding->getText())};
        } else if (auto* literal = Token(context, P::TOKEN_STRING_VALUE)) {
            result.Value = TLiteral{ELiteralKind::String, TString(literal->getText())};
        } else {
            result.Value = Identifier(Child(context, P::RuleId));
        }
        return result;
    }

    // Unwrap grammar scaffolding while preserving recursive type structure.
    // Do not resolve aliases or validate Decimal bounds here; existing YQL type
    // annotation will do that for the subset accepted by expression lowering.
    TTypeName Type(TContext* context) {
        const auto rule = context->getRuleIndex();
        if (rule == P::RuleType_name_or_bind) {
            auto* type = Child(context, P::RuleType_name);
            if (!type) {
                TTypeName result;
                result.Name = Identifier(Child(context, P::RuleBind_parameter));
                result.Binding = true;
                return result;
            }
            return Type(type);
        }
        if (rule == P::RuleType_name || rule == P::RuleType_name_composite) {
            auto* child = dynamic_cast<TContext*>(context->children.front());
            Require(child, context, "this type");
            auto result = Type(child);
            for (auto* tree : context->children) {
                if (auto* token = dynamic_cast<TTerminal*>(tree); token && token->getSymbol()->getType() == P::TOKEN_QUESTION) {
                    ++result.OptionalDepth;
                }
            }
            return result;
        }
        TTypeName result;
        if (rule == P::RuleType_name_simple || rule == P::RuleType_name_null) {
            result.Name = Identifier(context);
            return result;
        }
        if (rule == P::RuleType_name_decimal) {
            result.Name = Identifier(context->children.front());
            for (auto* parameter : Children(context, P::RuleInteger_or_bind)) {
                Require(!Child(parameter, P::RuleBind_parameter), parameter, "bound Decimal parameters");
                result.Parameters.emplace_back(parameter->getText());
            }
            return result;
        }
        Require(rule == P::RuleType_name_optional || rule == P::RuleType_name_list || rule == P::RuleType_name_tuple
            || rule == P::RuleType_name_dict || rule == P::RuleType_name_set || rule == P::RuleType_name_struct
            || rule == P::RuleType_name_variant || rule == P::RuleType_name_enum || rule == P::RuleType_name_resource
            || rule == P::RuleType_name_tagged || rule == P::RuleType_name_stream || rule == P::RuleType_name_flow
            || rule == P::RuleType_name_linear || rule == P::RuleType_name_dynamiclinear, context, "this composite type");
        result.Name = Identifier(context->children.front());
        for (auto* argument : Children(context, P::RuleType_name_or_bind)) {
            result.Arguments.push_back(Type(argument));
        }
        for (auto* member : Children(context, rule == P::RuleType_name_struct ? P::RuleStruct_arg : P::RuleVariant_arg)) {
            TTypeMember item;
            if (auto* name = Child(member, P::RuleType_name_tag)) {
                item.Name = TypeTag(name);
            }
            item.Type = Type(Child(member, P::RuleType_name_or_bind));
            result.Members.push_back(std::move(item));
        }
        for (auto* tag : Children(context, P::RuleType_name_tag)) {
            result.Tags.push_back(TypeTag(tag));
        }
        return result;
    }

    // Preserve an omitted direction separately from explicit ASC. Expressions
    // are syntax only here; projection-alias precedence is resolved later.
    TVector<TSortSpecification> OrderBy(TContext* context) {
        TVector<TSortSpecification> result;
        auto* list = Child(context, P::RuleSort_specification_list);
        for (auto* sort : Children(list, P::RuleSort_specification)) {
            result.push_back({Expression(Child(sort, P::RuleExpr)),
                Token(sort, P::TOKEN_DESC) ? ESortDirection::Descending
                    : Token(sort, P::TOKEN_ASC) ? ESortDirection::Ascending : ESortDirection::Default});
        }
        return result;
    }

    // An unbounded frame endpoint has no offset expression. Preserve this
    // distinction so later validation cannot confuse it with a numeric bound.
    TWindowBound WindowBound(TContext* context) {
        TWindowBound result;
        result.Range = Range(context);
        if (!Token(context, P::TOKEN_CURRENT)) {
            const bool preceding = Token(context, P::TOKEN_PRECEDING);
            if (Token(context, P::TOKEN_UNBOUNDED)) {
                result.Kind = preceding ? EWindowBoundKind::UnboundedPreceding : EWindowBoundKind::UnboundedFollowing;
            } else {
                result.Kind = preceding ? EWindowBoundKind::Preceding : EWindowBoundKind::Following;
                result.Offset = Expression(Child(context, P::RuleExpr));
            }
        }
        return result;
    }

    // Copy the optional base window and each clause independently. In particular
    // a missing frame end is not replaced by an explicit CURRENT ROW at parse time.
    TWindowSpecification WindowSpecification(TContext* context) {
        TWindowSpecification result;
        result.Range = Range(context);
        auto* details = Child(context, P::RuleWindow_specification_details);
        if (auto* base = Child(details, P::RuleExisting_window_name)) {
            result.BaseWindow = Identifier(Child(base, P::RuleWindow_name));
        }
        if (auto* partition = Child(details, P::RuleWindow_partition_clause)) {
            result.CompactPartition = Token(partition, P::TOKEN_COMPACT);
            for (auto* item : Children(Child(partition, P::RuleNamed_expr_list), P::RuleNamed_expr)) {
                result.PartitionBy.push_back(Named(item));
            }
        }
        if (auto* order = Child(details, P::RuleWindow_order_clause)) {
            result.OrderBy = OrderBy(Child(order, P::RuleOrder_by_clause));
        }
        if (auto* frame = Child(details, P::RuleWindow_frame_clause)) {
            TWindowFrame value;
            value.Range = Range(frame);
            auto* units = Child(frame, P::RuleWindow_frame_units);
            value.Units = Token(units, P::TOKEN_RANGE) ? EWindowFrameUnits::Range
                : Token(units, P::TOKEN_GROUPS) ? EWindowFrameUnits::Groups : EWindowFrameUnits::Rows;
            auto* extent = Child(frame, P::RuleWindow_frame_extent);
            if (auto* between = Child(extent, P::RuleWindow_frame_between)) {
                auto bounds = Children(between, P::RuleWindow_frame_bound);
                value.Begin = WindowBound(bounds[0]);
                value.End = WindowBound(bounds[1]);
            } else {
                value.Begin = WindowBound(Child(extent, P::RuleWindow_frame_bound));
            }
            if (auto* exclusion = Child(frame, P::RuleWindow_frame_exclusion)) {
                value.Exclusion = Token(exclusion, P::TOKEN_CURRENT) ? EWindowExclusion::CurrentRow
                    : Token(exclusion, P::TOKEN_GROUP) ? EWindowExclusion::Group
                    : Token(exclusion, P::TOKEN_TIES) ? EWindowExclusion::Ties : EWindowExclusion::NoOthers;
            }
            result.Frame = std::move(value);
        }
        return result;
    }

    // Hints can change schema as well as execution. Keep their structured values
    // so a later phase can reject unsupported hints instead of silently losing them.
    TTableHint TableHint(TContext* context) {
        TTableHint result;
        result.Range = Range(context);
        if (auto* name = Child(context, P::RuleAn_id_hint)) {
            TGenericTableHint hint;
            hint.Name = Identifier(name);
            hint.ValueForm = Token(context, P::TOKEN_LPAREN) ? EHintValueForm::List
                : Token(context, P::TOKEN_EQUALS) ? EHintValueForm::Scalar : EHintValueForm::None;
            for (auto* value : Children(context, P::RuleType_name_tag)) {
                hint.Values.push_back(TypeTag(value));
            }
            result.Value = std::move(hint);
        } else if (Token(context, P::TOKEN_WATERMARK)) {
            result.Value = TWatermarkHint{Expression(Child(context, P::RuleExpr))};
        } else {
            TSchemaHint hint;
            hint.Kind = Token(context, P::TOKEN_COLUMNS) ? ESchemaHintKind::Columns : ESchemaHintKind::Schema;
            if (auto* type = Child(context, P::RuleType_name_or_bind)) {
                hint.Value = Type(type);
            } else {
                TVector<TSchemaColumn> columns;
                for (auto* column : Children(context, P::RuleStruct_arg_positional)) {
                    TSchemaColumn item;
                    item.Range = Range(column);
                    item.Name = TypeTag(Child(column, P::RuleType_name_tag));
                    item.Type = Type(Child(column, P::RuleType_name_or_bind));
                    item.Nullability = Token(column, P::TOKEN_NOT) ? EColumnNullability::NotNull
                        : Token(column, P::TOKEN_NULL_) ? EColumnNullability::Nullable : EColumnNullability::Default;
                    columns.push_back(std::move(item));
                }
                hint.Value = std::move(columns);
            }
            result.Value = std::move(hint);
        }
        return result;
    }

    // The same grammar wrapper covers grouping, tuples and relational subqueries.
    // Use its actual alternatives and comma/alias syntax to distinguish them;
    // Parentheses around one scalar normally do not create a tuple. The RHS
    // of IN is the exception: (x) denotes a one-element membership collection.
    TExpressionPtr Parenthesis(TContext* context, bool expectTuple = false) {
        auto* subexpr = Child(context, P::RuleSelect_subexpr);
        if (!subexpr) {
            return MakeExpression(context, TTuple{});
        }
        auto* core = Child(subexpr, P::RuleSelect_subexpr_core);
        auto intersections = Children(core, P::RuleSelect_subexpr_intersect);
        auto terms = Children(intersections.front(), P::RuleSelect_or_expr);
        if (auto* tuple = Child(terms.front(), P::RuleTuple_or_expr)) {
            Require(!Child(subexpr, P::RuleCte_with_clause) && intersections.size() == 1 && terms.size() == 1,
                context, "set operations on scalar expressions");
            auto elements = Children(tuple, P::RuleNamed_expr);
            auto* alias = Child(tuple, P::RuleAn_id_or_type);
            auto first = Expression(Child(tuple, P::RuleExpr));
            if ((!expectTuple || std::holds_alternative<TSubquery>(first->Value))
                && elements.empty() && !Token(tuple, P::TOKEN_COMMA) && !alias) {
                first->Range = Range(context);
                return first;
            }
            TTuple result;
            result.Elements.push_back({std::move(first), alias ? std::optional(Identifier(alias)) : std::nullopt});
            for (auto* element : elements) {
                result.Elements.push_back(Named(element));
            }
            return MakeExpression(context, std::move(result));
        }
        return MakeExpression(context, TSubquery{Query(subexpr)});
    }

    TExpressionPtr Atom(TContext* context) {
        if (Token(context, P::TOKEN_NAMESPACE)) {
            return MakeExpression(context, TName{Identifier(context->children[2]), Identifier(context->children[0])});
        }
        auto* child = dynamic_cast<TContext*>(context->children.front());
        Require(child, context, "this atom");
        return Expression(child);
    }

    // Apply member, index and call suffixes to the result built so far. This
    // preserves chains such as f(x).field[i] without treating every suffix as
    // attached directly to the original atom.
    TExpressionPtr Suffix(TContext* context, TExpressionPtr result) {
        Require(!Token(context, P::TOKEN_COLLATE), context, "COLLATE");
        for (auto* tree : context->children) {
            if (auto* token = dynamic_cast<TTerminal*>(tree)) {
                if (token->getSymbol()->getType() == P::TOKEN_DOT) {
                    continue;
                }
                Require(token->getSymbol()->getType() == P::TOKEN_DIGITS, tree, "this member access");
                result = MakeExpression(token, TMember{std::move(result), Identifier(token)});
                continue;
            }
            auto* child = dynamic_cast<TContext*>(tree);
            switch (child->getRuleIndex()) {
                case P::RuleAn_id_or_type:
                    result = MakeExpression(child, TMember{std::move(result), Identifier(child)});
                    break;
                case P::RuleKey_expr:
                    result = MakeExpression(child, TIndex{std::move(result), Expression(Child(child, P::RuleExpr))});
                    break;
                case P::RuleInvoke_expr: {
                    auto* tail = Child(child, P::RuleInvoke_expr_tail);
                    TCall call;
                    call.Callable = std::move(result);
                    call.StarArgument = Token(child, P::TOKEN_ASTERISK);
                    call.Quantifier = Quantifier(Child(child, P::RuleOpt_set_quantifier));
                    if (auto* args = Child(child, P::RuleNamed_expr_list)) {
                        for (auto* arg : Children(args, P::RuleNamed_expr)) {
                            call.Arguments.push_back(Named(arg));
                        }
                    }
                    if (auto* treatment = Child(tail, P::RuleNull_treatment)) {
                        call.NullTreatment = Token(treatment, P::TOKEN_IGNORE) ? ENullTreatment::Ignore : ENullTreatment::Respect;
                    }
                    if (auto* filter = Child(tail, P::RuleFilter_clause)) {
                        call.Filter = Expression(Child(filter, P::RuleExpr));
                    }
                    if (auto* over = Child(tail, P::RuleWindow_name_or_specification)) {
                        TWindowReference reference;
                        reference.Range = Range(over);
                        if (auto* name = Child(over, P::RuleWindow_name)) {
                            reference.Value = Identifier(name);
                        } else {
                            reference.Value = WindowSpecification(Child(over, P::RuleWindow_specification));
                        }
                        call.Over = std::move(reference);
                    }
                    result = MakeExpression(child, std::move(call));
                    break;
                }
                default:
                    Unsupported(child, "dynamic member access");
            }
        }
        return result;
    }

    // Dispatch on generated grammar rules rather than reparsing token text.
    // Every accepted alternative must preserve all of its semantic modifiers;
    // otherwise Require/Unsupported hands the complete script to the old frontend.
    TExpressionPtr Expression(TContext* context) {
        Y_ENSURE(context);
        TExpressionPtr result;
        switch (context->getRuleIndex()) {
            case P::RuleExpr:
                Require(!Child(context, P::RuleType_name_composite), context, "type-valued expressions");
                return Fold(context, P::RuleOr_subexpr);
            case P::RuleOr_subexpr: return Fold(context, P::RuleAnd_subexpr);
            case P::RuleAnd_subexpr: return Fold(context, P::RuleXor_subexpr);
            case P::RuleXor_subexpr: {
                result = Expression(Child(context, P::RuleEq_subexpr));
                if (auto* condition = Child(context, P::RuleCond_expr)) {
                    result = Condition(condition, std::move(result));
                    result->Range = Range(context);
                }
                return result;
            }
            case P::RuleEq_subexpr:
                Require(Children(context, P::RuleNeq_subexpr).size() <= 2, context, "chained ordered comparisons");
                return Fold(context, P::RuleNeq_subexpr);
            case P::RuleNeq_subexpr: {
                Require(!Token(context, P::TOKEN_QUESTION), context, "optional type expressions");
                if (auto* right = Child(context, P::RuleNeq_subexpr)) {
                    // '??' is right associative. Fold only its left operand's
                    // bitwise chain, leaving the CST untouched.
                    TExpressionPtr left;
                    TTree* op = nullptr;
                    for (auto* tree : context->children) {
                        auto* child = dynamic_cast<TContext*>(tree);
                        if (child && child->getRuleIndex() == P::RuleDouble_question) {
                            break;
                        }
                        if (child && child->getRuleIndex() == P::RuleBit_subexpr) {
                            auto value = Expression(child);
                            left = left ? MakeExpression(context, TBinary{BinaryOp(op), std::move(left), std::move(value)}) : std::move(value);
                        } else {
                            op = tree;
                        }
                    }
                    return MakeExpression(context, TBinary{EBinaryOp::Coalesce, std::move(left), Expression(right)});
                }
                return Fold(context, P::RuleBit_subexpr);
            }
            case P::RuleBit_subexpr: return Fold(context, P::RuleAdd_subexpr);
            case P::RuleAdd_subexpr: return Fold(context, P::RuleMul_subexpr);
            case P::RuleMul_subexpr: return Fold(context, P::RuleCon_subexpr);
            case P::RuleCon_subexpr: {
                result = Expression(Child(context, P::RuleUnary_subexpr));
                if (auto* op = Child(context, P::RuleUnary_op)) {
                    const auto text = Upper(op);
                    return MakeExpression(context, TUnary{text == "+" ? EUnaryOp::Plus : text == "-" ? EUnaryOp::Minus
                        : text == "~" ? EUnaryOp::BitNot : EUnaryOp::Not, std::move(result)});
                }
                return result;
            }
            case P::RuleIn_expr:
            case P::RuleUnary_subexpr:
            case P::RuleIn_unary_subexpr:
                return Expression(dynamic_cast<TContext*>(context->children.front()));
            case P::RuleUnary_casual_subexpr:
            case P::RuleIn_unary_casual_subexpr:
                result = Suffix(Child(context, P::RuleUnary_subexpr_suffix),
                    Expression(dynamic_cast<TContext*>(context->children.front())));
                result->Range = Range(context);
                return result;
            case P::RuleId_expr:
            case P::RuleId_expr_in:
                return MakeExpression(context, TName{Identifier(context), {}});
            case P::RuleIn_atom_expr:
                if (auto* lambda = Child(context, P::RuleLambda); lambda && !Token(lambda, P::TOKEN_ARROW)) {
                    return Parenthesis(Child(lambda, P::RuleSmart_parenthesis), true);
                }
                return Atom(context);
            case P::RuleAtom_expr:
                return Atom(context);
            case P::RuleLiteral_value:
                return Literal(context);
            case P::RuleBind_parameter:
                return MakeExpression(context, TParameter{TString(context->getText())});
            case P::RuleLambda:
                if (Token(context, P::TOKEN_ARROW)) { return Lambda(context); }
                return Parenthesis(Child(context, P::RuleSmart_parenthesis));
            case P::RuleList_literal: {
                TList list;
                if (auto* values = Child(context, P::RuleExpr_list)) {
                    for (auto* value : Children(values, P::RuleExpr)) {
                        list.Elements.push_back(Expression(value));
                    }
                }
                return MakeExpression(context, std::move(list));
            }
            case P::RuleCast_expr:
            case P::RuleBitcast_expr: {
                const bool bitcast = context->getRuleIndex() == P::RuleBitcast_expr;
                return MakeExpression(context, TCast{Expression(Child(context, P::RuleExpr)),
                    Type(Child(context, bitcast ? P::RuleType_name_simple : P::RuleType_name_or_bind)), bitcast});
            }
            case P::RuleCase_expr: {
                TCase value;
                auto expressions = Children(context, P::RuleExpr);
                const bool hasElse = Token(context, P::TOKEN_ELSE);
                if (expressions.size() > size_t(hasElse)) {
                    value.Operand = Expression(expressions.front());
                }
                for (auto* branch : Children(context, P::RuleWhen_expr)) {
                    auto parts = Children(branch, P::RuleExpr);
                    value.Branches.push_back({Expression(parts[0]), Expression(parts[1])});
                }
                if (hasElse) {
                    value.Else = Expression(expressions.back());
                }
                return MakeExpression(context, std::move(value));
            }
            case P::RuleExists_expr: {
                auto* query = Child(context, P::RuleSelect_stmt);
                Require(query, context, "EXISTS VALUES");
                return MakeExpression(context, TExists{Query(query)});
            }
            default:
                Unsupported(context, "this expression");
        }
    }

    TQueryPtr Query(TContext* context);
    TSourcePtr Source(TContext* context);
    TSelect Select(TContext* context);
    TGroupingElement Grouping(TContext* context);

    const TString& File;
};

// Fold UNION/EXCEPT and INTERSECT at their separate grammar levels, retaining
// INTERSECT precedence and left associativity within a level. WITH and count
// clauses decorate the resulting query rather than an arbitrary leaf SELECT.
template <typename P>
TQueryPtr TAstBuilder<P>::Query(TContext* context) {
    const auto rule = context->getRuleIndex();
    TQueryPtr result;
    if (rule == P::RuleSelect_stmt || rule == P::RuleSelect_subexpr || rule == P::RuleSelect_unparenthesized_stmt) {
        result = Query(Child(context, rule == P::RuleSelect_stmt ? P::RuleSelect_stmt_core
            : rule == P::RuleSelect_subexpr ? P::RuleSelect_subexpr_core : P::RuleSelect_unparenthesized_stmt_core));
        if (auto* with = Child(context, P::RuleCte_with_clause)) {
            for (auto* binding : Children(with, P::RuleCte_binding)) {
                auto* key = Child(binding, P::RuleCte_key);
                auto* value = Child(binding, P::RuleCte_value);
                auto* select = Child(value, P::RuleSelect_stmt);
                Require(select, value, "VALUES in common table expressions");
                TCte cte;
                cte.Name = Identifier(Child(key, P::RuleId_table_or_type));
                cte.Recursive = Token(binding, P::TOKEN_RECURSIVE);
                if (auto* columns = Child(key, P::RulePure_column_list)) {
                    for (auto* column : Children(columns, P::RuleAn_id)) {
                        cte.Columns.push_back(Identifier(column));
                    }
                }
                cte.Query = Query(select);
                result->With.push_back(std::move(cte));
            }
        }
    } else if (rule == P::RuleSelect_stmt_core || rule == P::RuleSelect_subexpr_core
        || rule == P::RuleSelect_stmt_intersect || rule == P::RuleSelect_subexpr_intersect
        || rule == P::RuleSelect_unparenthesized_stmt_core || rule == P::RuleSelect_unparenthesized_stmt_intersect)
    {
        TContext* op = nullptr;
        for (auto* tree : context->children) {
            auto* child = dynamic_cast<TContext*>(tree);
            Y_ENSURE(child);
            if (child->getRuleIndex() == P::RuleUnion_op || child->getRuleIndex() == P::RuleIntersect_op) {
                op = child;
                continue;
            }
            auto right = Query(child);
            if (result) {
                Y_ENSURE(op);
                const auto* leftSelect = std::get_if<TSelect>(&result->Body);
                Require(result->Parenthesized || (!result->Limit && result->OrderBy.empty()
                    && (!leftSelect || leftSelect->OrderBy.empty())), context,
                    "ORDER BY/LIMIT before the last unparenthesized set operand");
                TSetOperation operation;
                operation.Op = Token(op, P::TOKEN_UNION) ? ESetOperation::Union
                    : Token(op, P::TOKEN_INTERSECT) ? ESetOperation::Intersect : ESetOperation::Except;
                operation.Quantifier = Quantifier(op);
                operation.Left = std::move(result);
                operation.Right = std::move(right);
                result = std::make_unique<TQuery>();
                result->Range = operation.Left->Range;
                result->Range.End = operation.Right->Range.End;
                // The grammar attaches trailing clauses to the final SELECT,
                // but without parentheses they apply to the entire compound.
                if (!operation.Right->Parenthesized) {
                    if (auto* select = std::get_if<TSelect>(&operation.Right->Body)) {
                        Require(!select->AssumeOrderBy, context, "ASSUME ORDER BY on set operations");
                        result->OrderBy = std::move(select->OrderBy);
                    } else {
                        result->OrderBy = std::move(operation.Right->OrderBy);
                    }
                    result->Limit = std::move(operation.Right->Limit);
                    result->Offset = std::move(operation.Right->Offset);
                }
                result->Body = std::move(operation);
                op = nullptr;
            } else {
                result = std::move(right);
            }
        }
    } else if (rule == P::RuleSelect_kind_parenthesis || rule == P::RuleSelect_or_expr) {
        auto* kind = Child(context, P::RuleSelect_kind_partial);
        Require(kind, context, "scalar operands of set operations");
        result = Query(kind);
        result->Parenthesized = Token(context, P::TOKEN_LPAREN);
    } else if (rule == P::RuleSelect_kind_partial) {
        auto* kind = Child(context, P::RuleSelect_kind);
        auto* core = Child(kind, P::RuleSelect_core);
        Require(core, kind, "PROCESS, REDUCE and COMBINE");
        Require(!Token(kind, P::TOKEN_DISCARD) && !Token(kind, P::TOKEN_INTO), kind, "DISCARD and INTO RESULT");
        result = std::make_unique<TQuery>();
        result->Body = Select(core);
        auto limits = Children(context, P::RuleExpr);
        if (!limits.empty()) {
            result->Limit = Expression(limits[0]);
            if (limits.size() == 2) {
                result->Offset = Expression(limits[1]);
                if (Token(context, P::TOKEN_COMMA)) {
                    std::swap(result->Limit, result->Offset);
                }
            }
        }
    } else {
        Unsupported(context, "this query form");
    }
    Y_ENSURE(result);
    result->Range = Range(context);
    return result;
}

// Preserve the nested grouping specification; expanding it into grouping
// sets is a relational operation and is deliberately deferred.
template <typename P>
TGroupingElement TAstBuilder<P>::Grouping(TContext* context) {
    if (context->getRuleIndex() == P::RuleGrouping_element) {
        return Grouping(dynamic_cast<TContext*>(context->children.front()));
    }
    TGroupingElement result;
    result.Range = Range(context);
    const auto rule = context->getRuleIndex();
    if (rule == P::RuleOrdinary_grouping_set) {
        result.Expression = Named(Child(context, P::RuleNamed_expr));
    } else if (rule == P::RuleRollup_list || rule == P::RuleCube_list) {
        result.Kind = rule == P::RuleRollup_list ? EGroupingKind::Rollup : EGroupingKind::Cube;
        for (auto* item : Children(Child(context, P::RuleOrdinary_grouping_set_list), P::RuleOrdinary_grouping_set)) {
            result.Elements.push_back(Grouping(item));
        }
    } else if (rule == P::RuleGrouping_sets_specification) {
        result.Kind = EGroupingKind::Sets;
        for (auto* item : Children(Child(context, P::RuleGrouping_element_list), P::RuleGrouping_element)) {
            result.Elements.push_back(Grouping(item));
        }
    } else {
        Unsupported(context, "hopping-window grouping");
    }
    return result;
}

// Read each clause from this SELECT only. In particular WHERE and HAVING
// expressions are direct children; descending into projections or FROM would
// accidentally capture expressions from nested queries.
template <typename P>
TSelect TAstBuilder<P>::Select(TContext* context) {
    Require(!Token(context, P::TOKEN_STREAM), context, "SELECT STREAM");
    Require(!Token(context, P::TOKEN_WITHOUT), context, "SELECT WITHOUT");
    TSelect result;
    result.Quantifier = Quantifier(Child(context, P::RuleOpt_set_quantifier));
    for (auto* column : Children(context, P::RuleResult_column)) {
        TNamedExpression item;
        if (Token(column, P::TOKEN_ASTERISK)) {
            TStar star;
            if (auto* qualifier = Child(Child(column, P::RuleOpt_id_prefix), P::RuleAn_id)) {
                star.Qualifier = Identifier(qualifier);
            }
            item.Expression = MakeExpression(column, std::move(star));
        } else {
            item.Expression = Expression(Child(column, P::RuleExpr));
            if (auto* alias = Child(column, P::RuleAn_id_or_type)) {
                item.Alias = Identifier(alias);
            } else if (auto* alias = Child(column, P::RuleAn_id_as_compat)) {
                item.Alias = Identifier(alias);
            }
        }
        result.Columns.push_back(std::move(item));
    }
    auto sources = Children(context, P::RuleJoin_source);
    Require(sources.size() <= 1, context, "two FROM clauses");
    if (!sources.empty()) {
        result.From = Source(sources.front());
    }
    auto predicates = Children(context, P::RuleExpr);
    size_t predicate = 0;
    if (Token(context, P::TOKEN_WHERE)) {
        result.Where = Expression(predicates[predicate++]);
    }
    if (Token(context, P::TOKEN_HAVING)) {
        result.Having = Expression(predicates[predicate++]);
    }
    if (auto* group = Child(context, P::RuleGroup_by_clause)) {
        result.CompactGroupBy = Token(group, P::TOKEN_COMPACT);
        result.GroupQuantifier = Quantifier(Child(group, P::RuleOpt_set_quantifier));
        if (auto* hint = Child(group, P::RuleAn_id)) {
            result.GroupHint = Identifier(hint);
        }
        for (auto* item : Children(Child(group, P::RuleGrouping_element_list), P::RuleGrouping_element)) {
            result.GroupBy.push_back(Grouping(item));
        }
    }
    if (auto* window = Child(context, P::RuleWindow_clause)) {
        auto* list = Child(window, P::RuleWindow_definition_list);
        for (auto* definition : Children(list, P::RuleWindow_definition)) {
            result.Windows.push_back({Range(definition), Identifier(Child(definition, P::RuleNew_window_name)),
                WindowSpecification(Child(definition, P::RuleWindow_specification))});
        }
    }
    if (auto* order = Child(context, P::RuleExt_order_by_clause)) {
        result.AssumeOrderBy = Token(order, P::TOKEN_ASSUME);
        result.OrderBy = OrderBy(Child(order, P::RuleOrder_by_clause));
    }
    return result;
}

// Join chains are left-folded as their inputs arrive. The following ON/USING
// constraint belongs to the most recently constructed join; aliases, hints
// and sampling stay on their own source wrappers.
template <typename P>
TSourcePtr TAstBuilder<P>::Source(TContext* context) {
    const auto rule = context->getRuleIndex();
    TSourcePtr result;
    if (rule == P::RuleJoin_source) {
        TContext* op = nullptr;
        bool any = false;
        bool leftAny = false;
        for (auto* tree : context->children) {
            if (auto* token = dynamic_cast<TTerminal*>(tree)) {
                Y_ENSURE(token->getSymbol()->getType() == P::TOKEN_ANY);
                any = true;
                continue;
            }
            auto* child = dynamic_cast<TContext*>(tree);
            if (child->getRuleIndex() == P::RuleJoin_op) {
                op = child;
            } else if (child->getRuleIndex() == P::RuleFlatten_source) {
                auto right = Source(child);
                if (!result) {
                    result = std::move(right);
                    leftAny = any;
                    any = false;
                    continue;
                }
                Y_ENSURE(op);
                TJoin join;
                join.LeftAny = leftAny;
                join.RightAny = any;
                // Later joins consume the accumulated result, without carrying
                // ANY from an input of an earlier join onto that result.
                leftAny = false;
                any = false;
                Require(!Token(op, P::TOKEN_OUTER) ||
                    ((Token(op, P::TOKEN_LEFT) || Token(op, P::TOKEN_RIGHT) || Token(op, P::TOKEN_FULL)) &&
                        !Token(op, P::TOKEN_ONLY) && !Token(op, P::TOKEN_SEMI)), op, "this OUTER JOIN modifier");
                join.Natural = Token(op, P::TOKEN_NATURAL);
                if (Token(op, P::TOKEN_COMMA) || Token(op, P::TOKEN_CROSS)) {
                    join.Kind = EJoinKind::Cross;
                } else if (Token(op, P::TOKEN_LEFT)) {
                    join.Kind = Token(op, P::TOKEN_ONLY) ? EJoinKind::LeftOnly
                        : Token(op, P::TOKEN_SEMI) ? EJoinKind::LeftSemi : EJoinKind::Left;
                } else if (Token(op, P::TOKEN_RIGHT)) {
                    join.Kind = Token(op, P::TOKEN_ONLY) ? EJoinKind::RightOnly
                        : Token(op, P::TOKEN_SEMI) ? EJoinKind::RightSemi : EJoinKind::Right;
                } else if (Token(op, P::TOKEN_FULL)) {
                    join.Kind = EJoinKind::Full;
                } else if (Token(op, P::TOKEN_EXCLUSION)) {
                    join.Kind = EJoinKind::Exclusion;
                } else {
                    join.Kind = EJoinKind::Inner;
                }
                join.Left = std::move(result);
                join.Right = std::move(right);
                result = std::make_unique<TSource>();
                result->Range = join.Left->Range;
                result->Range.End = join.Right->Range.End;
                result->Value = std::move(join);
                op = nullptr;
            } else if (child->getRuleIndex() == P::RuleJoin_constraint) {
                auto& join = std::get<TJoin>(result->Value);
                result->Range.End = Range(child).End;
                if (auto* on = Child(child, P::RuleExpr)) {
                    join.On = Expression(on);
                } else {
                    for (auto* name : Children(Child(child, P::RulePure_column_or_named_list), P::RulePure_column_or_named)) {
                        auto* id = Child(name, P::RuleAn_id);
                        Require(id, name, "bound JOIN USING columns");
                        join.Using.push_back(Identifier(id));
                    }
                }
            } else {
                Unsupported(child, "this join clause");
            }
        }
        Require(!leftAny, context, "ANY without a JOIN");
    } else if (rule == P::RuleFlatten_source) {
        Require(!Token(context, P::TOKEN_FLATTEN), context, "FLATTEN");
        result = Source(Child(context, P::RuleNamed_single_source));
    } else if (rule == P::RuleNamed_single_source) {
        Require(!Child(context, P::RuleRow_pattern_recognition_clause), context, "MATCH_RECOGNIZE");
        result = Source(Child(context, P::RuleHinted_single_source));
        if (auto* alias = Child(context, P::RuleAn_id)) {
            result->Alias = Identifier(alias);
        } else if (auto* alias = Child(context, P::RuleAn_id_as_compat)) {
            result->Alias = Identifier(alias);
        }
        if (auto* columns = Child(context, P::RulePure_column_list)) {
            for (auto* column : Children(columns, P::RuleAn_id)) {
                result->ColumnAliases.push_back(Identifier(column));
            }
        }
        auto* sample = Child(context, P::RuleSample_clause);
        auto* tableSample = Child(context, P::RuleTablesample_clause);
        if (sample || tableSample) {
            auto* clause = sample ? sample : tableSample;
            TSampling sampling;
            sampling.Range = Range(clause);
            sampling.Rate = Expression(Child(clause, P::RuleExpr));
            if (tableSample) {
                auto* mode = Child(tableSample, P::RuleSampling_mode);
                sampling.Mode = Token(mode, P::TOKEN_BERNOULLI) ? ESamplingMode::Bernoulli : ESamplingMode::System;
                if (auto* repeatable = Child(tableSample, P::RuleRepeatable_clause)) {
                    sampling.Seed = Expression(Child(repeatable, P::RuleExpr));
                }
            }
            result->Sampling = std::move(sampling);
        }
    } else if (rule == P::RuleHinted_single_source) {
        result = Source(Child(context, P::RuleSingle_source));
        if (auto* hints = Child(context, P::RuleTable_hints)) {
            for (auto* hint : Children(hints, P::RuleTable_hint)) {
                result->Hints.push_back(TableHint(hint));
            }
        }
    } else if (rule == P::RuleSingle_source) {
        if (auto* table = Child(context, P::RuleTable_ref)) {
            result = Source(table);
        } else {
            auto* query = Child(context, P::RuleSelect_stmt);
            Require(query, context, "VALUES sources");
            result = std::make_unique<TSource>();
            result->Value = TDerivedTable{Query(query)};
        }
    } else if (rule == P::RuleTable_ref) {
        if (auto* function = Child(context, P::RuleAn_id_expr)) {
            Require(Upper(function) == "AS_TABLE" && !Child(context, P::RuleCluster_expr)
                && !Token(context, P::TOKEN_COMMAT), context, "this table-valued function");
            auto arguments = Children(context, P::RuleTable_arg);
            Require(arguments.size() == 1 && !Token(arguments.front(), P::TOKEN_COMMAT)
                && !Child(arguments.front(), P::RuleView_name), context, "AS_TABLE arguments");
            auto argument = Named(Child(arguments.front(), P::RuleNamed_expr));
            Require(!argument.Alias, context, "aliased AS_TABLE argument");
            result = std::make_unique<TSource>();
            result->Range = Range(context);
            result->Value = TExpressionSource{std::move(argument.Expression)};
            return result;
        }
        TTable table;
        table.Anonymous = Token(context, P::TOKEN_COMMAT);
        if (auto* key = Child(context, P::RuleTable_key)) {
            Require(!Child(key, P::RuleView_name), key, "VIEW reads");
            table.Name = Identifier(Child(key, P::RuleId_table_or_type));
        } else if (auto* binding = Child(context, P::RuleBind_parameter)) {
            Require(!Token(context, P::TOKEN_LPAREN) && !Child(context, P::RuleView_name), context, "bound table calls and VIEW reads");
            table.Binding = true;
            table.Name = Identifier(binding);
        } else {
            Unsupported(context, "table-valued function calls");
        }
        if (auto* cluster = Child(context, P::RuleCluster_expr)) {
            auto* name = Child(cluster, P::RulePure_column_or_named);
            Require(name && !Child(name, P::RuleBind_parameter), cluster, "dynamic clusters");
            table.Cluster = Identifier(Child(name, P::RuleAn_id));
            if (auto* provider = Child(cluster, P::RuleAn_id)) {
                table.Provider = Identifier(provider);
            }
        }
        result = std::make_unique<TSource>();
        result->Value = std::move(table);
    } else {
        Unsupported(context, "this source");
    }
    Y_ENSURE(result);
    result->Range = Range(context);
    return result;
}

} // namespace

std::unique_ptr<TScript> BuildNewYqlAst(antlr4::ParserRuleContext& root, bool ansiLexer, const TString& file) {
    if (ansiLexer) {
        return TAstBuilder<NKqpAnsiAntlr4::SQLv1Antlr4Parser>(file).Build(&root, true);
    }
    return TAstBuilder<NKqpDefaultAntlr4::SQLv1Antlr4Parser>(file).Build(&root, false);
}

} // namespace NKikimr::NKqp
