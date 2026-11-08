#include "kqp_expr_lowering.h"
#include "kqp_new_parser.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/sql_types/normalize_name.h>
#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <util/stream/str.h>
#include <util/generic/scope.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

#include <limits>
#include <type_traits>

namespace NKikimr::NKqp::NNewYql {
namespace {

using namespace NYql;
using TNode = TExprNode::TPtr;

struct TLoweringError {};

// Translate scalar syntax into the existing YQL callable vocabulary. This
// class deliberately leaves coercions and result typing to the shared type
// annotator; unsupported SQL semantics must never be guessed from a name.
class TLowerer {
public:
    TLowerer(const TScript& script, const TExpressionBindings& bindings, TExprContext& ctx,
        const TExpressionLoweringSettings& settings)
        : Script(script), Bindings(bindings), Ctx(ctx), Settings(settings)
    {}

    TNode Expression(const TExpression& expression) {
        const auto pos = Pos(expression.Range);
        if (const auto* replacement = Bindings.Expressions.FindPtr(&expression)) { return *replacement; }
        // Resolved identity takes precedence over the syntax variant, including
        // ANSI quoted identifiers represented as literals and qualified members.
        if (Bindings.Names) {
            if (const auto* id = Bindings.Names->ColumnReferences.FindPtr(&expression)) {
                const auto* binding = Bindings.Columns.FindPtr(*id);
                if (!binding || !*binding) {
                    Error(expression.Range, TStringBuilder() << "Missing expression binding for column " << *id);
                }
                return *binding;
            }
        }
        return std::visit([&](const auto& value) -> TNode {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TLiteral>) {
                return Literal(value, expression.Range);
            } else if constexpr (std::is_same_v<T, TName>) {
                Error(expression.Range, "Unresolved column reference");
            } else if constexpr (std::is_same_v<T, TParameter>) {
                if (Bindings.Names) {
                    if (const auto* reference = Bindings.Names->BindingReferences.FindPtr(&expression)) {
                        if (const auto* argument = Locals.FindPtr(*reference)) { return *argument; }
                        const auto* definition = Bindings.Names->Definitions.FindPtr(*reference);
                        if (!definition) { Error(expression.Range, "Lambda argument is outside its lexical scope"); }
                        // Lower afresh per use: YQL lambdas are polymorphic and
                        // annotation may specialize/rewrite each application.
                        return Expression(**definition);
                    }
                }
                const auto* binding = Bindings.Parameters.FindPtr(value.Spelling);
                if (!binding || !*binding) {
                    Error(expression.Range, TStringBuilder() << "Missing parameter binding for " << value.Spelling);
                }
                return *binding;
            } else if constexpr (std::is_same_v<T, TLambda>) {
                if (!Bindings.Names) { Error(expression.Range, "Lambda lowering requires resolved lexical bindings"); }
                auto outer = Locals;
                Y_DEFER { Locals = std::move(outer); };
                TExprNode::TListType arguments;
                for (const auto& parameter : value.Parameters) {
                    auto argument = Ctx.NewArgument(pos, parameter.Spelling);
                    Locals[&parameter] = argument;
                    arguments.push_back(std::move(argument));
                }
                // Definitions in the body are substituted through the resolver's
                // identity links. Outer argument bindings remain available for
                // nested closures even when an inner parameter has the same name.
                auto body = Expression(*value.Body);
                return Ctx.NewLambda(pos, Ctx.NewArguments(pos, std::move(arguments)), std::move(body));
            } else if constexpr (std::is_same_v<T, TUnary>) {
                if (value.Op == EUnaryOp::Minus) {
                    if (const auto* literal = std::get_if<TLiteral>(&value.Operand->Value);
                        literal && literal->Kind == ELiteralKind::Integer) {
                        // Parse sign together with magnitude so the minimum signed integer
                        // is accepted even though its positive magnitude exceeds signed max.
                        return Integer(literal->Spelling, expression.Range, true);
                    }
                }
                auto operand = Expression(*value.Operand);
                switch (value.Op) {
                    case EUnaryOp::Plus: return Call(pos, "Plus", {operand});
                    case EUnaryOp::Minus: return Call(pos, Settings.CheckedOps ? "CheckedMinus" : "Minus", {operand});
                    case EUnaryOp::BitNot: return Call(pos, "BitNot", {operand});
                    case EUnaryOp::Not: return Call(pos, "Not", {operand});
                    case EUnaryOp::IsNull: return Call(pos, "Not", {Call(pos, "Exists", {operand})});
                    case EUnaryOp::IsNotNull: return Call(pos, "Exists", {operand});
                }
                Y_UNREACHABLE();
            } else if constexpr (std::is_same_v<T, TBinary>) {
                auto left = Expression(*value.Left);
                auto right = Expression(*value.Right);
                if (value.Op == EBinaryOp::Divide && !Settings.ClassicDivision) {
                    left = Call(pos, "SafeCast", {left, Call(pos, "DataType", {Atom(pos, "Double")})});
                }
                return Call(pos, BinaryName(value.Op), {left, right});
            } else if constexpr (std::is_same_v<T, TCall>) {
                return Function(value, expression.Range);
            } else if constexpr (std::is_same_v<T, TMember>) {
                return Call(pos, "SqlAccess", {Atom(pos, "struct"), Expression(*value.Object),
                    Atom(Pos(value.Member.Range), Identifier(value.Member))});
            } else if constexpr (std::is_same_v<T, TIndex>) {
                return Call(pos, "SqlAccess", {Atom(pos, "dict"), Expression(*value.Object), Expression(*value.Index)});
            } else if constexpr (std::is_same_v<T, TTuple>) {
                TExprNode::TListType items;
                for (const auto& item : value.Elements) {
                    if (item.Alias) {
                        Unsupported(expression.Range, "named tuple elements");
                    }
                    items.push_back(Expression(*item.Expression));
                }
                return Ctx.NewList(pos, std::move(items));
            } else if constexpr (std::is_same_v<T, TList>) {
                TExprNode::TListType items;
                for (const auto& item : value.Elements) {
                    items.push_back(Expression(*item));
                }
                return Call(pos, "AsListMayWarn", std::move(items));
            } else if constexpr (std::is_same_v<T, TCase>) {
                // YQL SQL requires ELSE; do not silently add standard SQL NULL.
                if (!value.Else) {
                    Error(expression.Range, "ELSE is required");
                }
                auto result = Expression(*value.Else);
                auto operand = value.Operand ? Expression(*value.Operand) : TNode{};
                // Build from ELSE outward so the first matching WHEN remains the
                // outermost If. Reuse the simple-CASE operand node across comparisons.
                for (auto it = value.Branches.rbegin(); it != value.Branches.rend(); ++it) {
                    auto condition = Expression(*it->When);
                    if (operand) {
                        condition = Call(pos, "==", {operand, condition});
                    }
                    result = Call(pos, "If", {Predicate(pos, condition), Expression(*it->Then), result});
                }
                return result;
            } else if constexpr (std::is_same_v<T, TCast>) {
                return Call(pos, value.Bitcast ? "BitCast" : "SafeCast", {Expression(*value.Value), Type(value.Type)});
            } else if constexpr (std::is_same_v<T, TBetween>) {
                const auto input = Expression(*value.Value);
                const auto lower = Expression(*value.Lower);
                const auto upper = Expression(*value.Upper);
                const auto between = [&](TNode lo, TNode hi) {
                    return Call(pos, "And", {Call(pos, ">=", {input, lo}), Call(pos, "<=", {input, hi})});
                };
                auto result = between(lower, upper);
                if (value.Symmetric) {
                    result = Call(pos, "Or", {result, between(upper, lower)});
                }
                return value.Negated ? Call(pos, "Not", {result}) : result;
            } else if constexpr (std::is_same_v<T, TIn>) {
                TExprNode::TListType options;
                const auto option = [&](TStringBuf name) { options.push_back(Ctx.NewList(pos, {Atom(pos, name)})); };
                if (value.Compact) { option("isCompact"); }
                if (!Settings.AnsiIn) { option("warnNoAnsi"); }
                else if (*Settings.AnsiIn) { option("ansi"); }
                // SqlIn owns SQL three-valued membership semantics and expects the
                // collection first. Expanding into equality/Or here would bypass its
                // NULL handling and ANSI compatibility options.
                auto result = Call(pos, "SqlIn", {Expression(*value.Collection), Expression(*value.Value),
                    Ctx.NewList(pos, std::move(options))});
                return value.Negated ? Call(pos, "Not", {result}) : result;
            } else if constexpr (std::is_same_v<T, TPatternMatch>) {
                // Reuse the same Re2 runtime configuration as SQL v1. Literal
                // prefix/suffix patterns retain simple predicates for RBO/CBO.
                auto input = Expression(*value.Value);
                auto pattern = Expression(*value.Pattern);
                TNode result;
                if (value.Op == EPatternOp::Like && !value.Escape && pattern->IsCallable({"String", "Utf8"})) {
                    const TString text(pattern->Head().Content());
                    if (!text.Contains('_') && !text.Contains('\\')) {
                        auto inner = text;
                        const bool leading = inner.StartsWith('%'), trailing = inner.EndsWith('%');
                        if (leading) { inner = inner.substr(1); }
                        if (trailing && !inner.empty()) { inner.pop_back(); }
                        if (!inner.Contains('%')) {
                            result = Call(pos, leading ? (trailing ? "StringContains" : "EndsWith")
                                : trailing ? "StartsWith" : "==", {input, Call(pos, pattern->Content(), {Atom(pos, inner)})});
                        }
                    }
                }
                if (!result) {
                    if (value.Op != EPatternOp::Like && value.Op != EPatternOp::ILike) { Unsupported(expression.Range, "non-LIKE pattern operators"); }
                    TExprNode::TListType escapeArgs = {Call(pos, "Udf", {Atom(pos, "Re2.PatternFromLike")}), pattern};
                    if (value.Escape) { escapeArgs.push_back(Expression(*value.Escape)); }
                    auto escaped = Call(pos, "Apply", std::move(escapeArgs));
                    auto options = Call(pos, "NamedApply", {Call(pos, "Udf", {Atom(pos, "Re2.Options")}), Ctx.NewList(pos, {}),
                        Call(pos, "AsStruct", {Ctx.NewList(pos, {Atom(pos, "CaseSensitive"),
                            Call(pos, "Bool", {Atom(pos, value.Op == EPatternOp::Like ? "true" : "false")})})})});
                    auto matcher = Call(pos, "AssumeStrict", {Call(pos, "Udf", {Atom(pos, "Re2.Match"), Ctx.NewList(pos, {escaped, options})})});
                    result = Call(pos, "Apply", {matcher, input});
                }
                return value.Negated ? Call(pos, "Not", {result}) : result;
            } else if constexpr (std::is_same_v<T, TSubquery> || std::is_same_v<T, TExists>) {
                Unsupported(expression.Range, "relational subquery lowering");
            } else {
                static_assert(std::is_same_v<T, TStar>);
                Unsupported(expression.Range, "star expansion in scalar expressions");
            }
        }, expression.Value);
    }

private:
    TPositionHandle Pos(const TSourceRange& range) {
        return Ctx.AppendPosition(TPosition(range.Column + 1, range.Line, Script.File));
    }

    [[noreturn]] void Error(const TSourceRange& range, const TString& message) {
        Ctx.AddError(TIssue(Ctx.GetPosition(Pos(range)), TStringBuilder() << "New YQL expression lowering: " << message));
        throw TLoweringError{};
    }

    [[noreturn]] void Unsupported(const TSourceRange& range, TStringBuf feature) const {
        ythrow TNewYqlParserFallback() << "New YQL expression lowering does not support " << feature
            << " at " << Script.File << ":" << range.Line << ":" << range.Column + 1;
    }

    TNode Atom(TPositionHandle pos, TStringBuf value) { return Ctx.NewAtom(pos, value); }
    TNode Call(TPositionHandle pos, TStringBuf name, TExprNode::TListType args) {
        return Ctx.NewCallable(pos, name, std::move(args));
    }

    // YQL If requires a definite Bool. SQL unknown conditions take the false
    // branch, so normalize Optional<Bool> at the conditional boundary.
    TNode Predicate(TPositionHandle pos, TNode expression) {
        return Call(pos, "Coalesce", {expression, Call(pos, "Bool", {Atom(pos, "false")})});
    }

    TString Decode(const TString& text, const TSourceRange& range) {
        if (text.size() < 2 || text.front() != text.back()) {
            Error(range, "Invalid quoted value");
        }
        if (Script.AnsiLexer && text.front() != '`') {
            auto result = text.substr(1, text.size() - 2);
            SubstGlobal(result, TString(2, text.front()), TString(1, text.front()));
            return result;
        }
        TString result;
        TStringOutput output(result);
        size_t read = 0;
        const TStringBuf atom(text.data() + 1, text.size() - 1);
        const auto status = UnescapeArbitraryAtom(atom, text.front(), &output, &read);
        if (status != EUnescapeResult::OK || read != atom.size()) {
            Error(range, TStringBuilder() << "Invalid escape: " << UnescapeResultToString(status));
        }
        return result;
    }

    TString Identifier(const TIdentifier& identifier) {
        const auto& name = identifier.Spelling;
        return !name.empty() && (name.front() == '`' || name.front() == '"' || name.front() == '\'')
            ? Decode(name, identifier.Range) : name;
    }

    // Accumulate an unsigned magnitude with overflow checks before selecting
    // a signed/unsigned width. Keeping the sign separate avoids overflowing
    // when spelling the minimum Int64 or a suffix-constrained negative literal.
    TNode Integer(const TString& spelling, const TSourceRange& range, bool negative = false) {
        const auto pos = Pos(range);
        const auto text = to_lower(spelling);
        size_t end = text.size();
        if (end > 1 && TStringBuf("lstibn").Contains(text[end - 1])) { --end; }
        if (end > 1 && (text[end - 1] == 'u' || text[end - 1] == 'p')) { --end; }
        const auto suffix = text.substr(end);
        if (suffix.Contains('p')) { Unsupported(range, "PostgreSQL numeric literals"); }
        ui64 base = 10;
        size_t begin = 0;
        if (end > 2 && text[0] == '0') {
            if (text[1] == 'x') { base = 16; begin = 2; }
            else if (text[1] == 'o') { base = 8; begin = 2; }
            else if (text[1] == 'b') { base = 2; begin = 2; }
        }
        ui64 magnitude = 0;
        for (size_t i = begin; i < end; ++i) {
            const auto c = text[i];
            const ui64 digit = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : base;
            if (digit >= base || magnitude > (std::numeric_limits<ui64>::max() - digit) / base) {
                Error(range, "Invalid integer literal or integer overflow");
            }
            magnitude = magnitude * base + digit;
        }
        TString type;
        if (suffix.empty()) {
            if (negative && magnitude > (ui64(1) << 63)) { Error(range, "Negative integer overflow"); }
            type = magnitude <= ui64(std::numeric_limits<i32>::max()) + negative ? "Int32"
                : magnitude <= ui64(std::numeric_limits<i64>::max()) + negative ? "Int64" : "Uint64";
        } else {
            static const THashMap<TString, TString> types = {{"u", "Uint32"}, {"ul", "Uint64"},
                {"ut", "Uint8"}, {"t", "Int8"}, {"l", "Int64"}, {"us", "Uint16"}, {"s", "Int16"}};
            const auto* found = types.FindPtr(suffix);
            if (!found) { Error(range, "Invalid integer suffix"); }
            type = *found;
        }
        // A minus applied to an unsigned literal is still an arithmetic operation;
        // it must not silently change the explicit unsigned type to a signed one.
        const bool signedNegative = negative && type.StartsWith("Int");
        auto result = Call(pos, type, {Atom(pos, (signedNegative ? "-" : "") + ToString(magnitude))});
        return negative && !signedNegative ? Call(pos, Settings.CheckedOps ? "CheckedMinus" : "Minus", {result}) : result;
    }

    TNode Literal(const TLiteral& value, const TSourceRange& range) {
        const auto pos = Pos(range);
        switch (value.Kind) {
            case ELiteralKind::Integer: return Integer(value.Spelling, range);
            case ELiteralKind::Boolean: return Call(pos, "Bool", {Atom(pos, to_lower(value.Spelling))});
            case ELiteralKind::Null: return Call(pos, "Null", {});
            case ELiteralKind::Real: {
                auto text = value.Spelling;
                if (to_lower(text).Contains('p')) { Unsupported(range, "PostgreSQL numeric literals"); }
                const bool isFloat = AsciiEqualsIgnoreCase(text.substr(text.size() - 1), "f");
                if (isFloat) { text.pop_back(); }
                return Call(pos, isFloat ? "Float" : "Double", {Atom(pos, text)});
            }
            case ELiteralKind::String: {
                auto text = value.Spelling;
                if (Script.AnsiLexer && text.StartsWith('"')) { Error(range, "Unresolved quoted column reference"); }
                // An explicit SQL suffix overrides the default string type. Decode
                // escapes only after removing the suffix; otherwise a suffix could be
                // misinterpreted as part of the quoted value.
                TString type = Settings.UnicodeLiterals ? "Utf8" : "String";
                const auto lower = to_lower(text);
                if (lower.EndsWith('p') || lower.EndsWith("pt") || lower.EndsWith("pb") || lower.EndsWith("pv")) {
                    Unsupported(range, "PostgreSQL string literals");
                }
                if (lower.EndsWith('u')) { type = "Utf8"; text.pop_back(); }
                else if (lower.EndsWith('s')) { type = "String"; text.pop_back(); }
                else if (lower.EndsWith('j')) { type = "Json"; text.pop_back(); }
                else if (lower.EndsWith('y')) { type = "Yson"; text.pop_back(); }
                TString content;
                if (text.size() >= 4 && text.StartsWith("@@") && text.EndsWith("@@")) {
                    content = text.substr(2, text.size() - 4);
                    SubstGlobal(content, "@@@@", "@@");
                } else {
                    content = Decode(text, range);
                }
                return Call(pos, type, {Atom(pos, content)});
            }
            case ELiteralKind::Blob: Unsupported(range, "blob literals");
        }
        Y_UNREACHABLE();
    }

    // Callable selection preserves SQL warning and checked-arithmetic modes.
    // The shared type rules subsequently choose operand coercions and NULL types.
    TStringBuf BinaryName(EBinaryOp op) const {
        switch (op) {
            case EBinaryOp::Or: return "Or";
            case EBinaryOp::And: return "And";
            case EBinaryOp::Xor: return "Xor";
            case EBinaryOp::Equal: return "==";
            case EBinaryOp::NotEqual: return "!=";
            case EBinaryOp::Less: return "<";
            case EBinaryOp::LessOrEqual: return "<=";
            case EBinaryOp::Greater: return ">";
            case EBinaryOp::GreaterOrEqual: return ">=";
            case EBinaryOp::IsDistinctFrom: return "IsDistinctFrom";
            case EBinaryOp::IsNotDistinctFrom: return "IsNotDistinctFrom";
            case EBinaryOp::Add: return Settings.CheckedOps ? "CheckedAdd" : "+MayWarn";
            case EBinaryOp::Subtract: return Settings.CheckedOps ? "CheckedSub" : "-MayWarn";
            case EBinaryOp::Multiply: return Settings.CheckedOps ? "CheckedMul" : "*MayWarn";
            case EBinaryOp::Divide: return Settings.CheckedOps && Settings.ClassicDivision ? "CheckedDiv" : "/MayWarn";
            case EBinaryOp::Modulo: return Settings.CheckedOps ? "CheckedMod" : "%MayWarn";
            case EBinaryOp::BitAnd: return "BitAnd";
            case EBinaryOp::BitOr: return "BitOr";
            case EBinaryOp::BitXor: return "BitXor";
            case EBinaryOp::ShiftLeft: return "ShiftLeft";
            case EBinaryOp::ShiftRight: return "ShiftRight";
            case EBinaryOp::RotateLeft: return "RotLeft";
            case EBinaryOp::RotateRight: return "RotRight";
            case EBinaryOp::Concat: return "Concat";
            case EBinaryOp::Coalesce: return "Coalesce";
        }
        Y_UNREACHABLE();
    }

    // Build type expressions rather than assigning type annotations manually.
    // This reuses the existing validation for aliases, Decimal parameters and
    // container nesting through the shared callable type rules.
    TNode Type(const TTypeName& type) {
        const auto pos = Pos(type.Name.Range);
        if (type.Binding) { Unsupported(type.Name.Range, "bound type expressions"); }
        const auto name = to_lower(Identifier(type.Name));
        TExprNode::TListType args;
        for (const auto& arg : type.Arguments) { args.push_back(Type(arg)); }
        TNode result;
        if (!type.Members.empty() || !type.Tags.empty()) {
            Unsupported(type.Name.Range, "struct, variant and tagged type expressions");
        } else if (name == "decimal") {
            args.push_back(Atom(pos, "Decimal"));
            for (const auto& parameter : type.Parameters) {
                const auto integer = Integer(parameter, type.Name.Range);
                args.push_back(integer->HeadPtr());
            }
            result = Call(pos, "DataType", std::move(args));
        } else if (name == "set") {
            args.push_back(Call(pos, "VoidType", {}));
            result = Call(pos, "DictType", std::move(args));
        } else {
            static const THashMap<TString, TString> constructors = {{"optional", "OptionalType"}, {"list", "ListType"},
                {"tuple", "TupleType"}, {"dict", "DictType"}, {"stream", "StreamType"}, {"flow", "FlowType"},
                {"null", "NullType"}};
            if (const auto* constructor = constructors.FindPtr(name)) {
                result = Call(pos, *constructor, std::move(args));
            } else if (auto simple = LookupSimpleTypeBySqlAlias(std::string_view(name.data(), name.size()), Settings.FlexibleTypes)) {
                const TString canonical(*simple);
                if (NUdf::FindDataSlot(canonical)) {
                    result = Call(pos, "DataType", {Atom(pos, canonical)});
                } else {
                    result = Call(pos, canonical + "Type", {});
                }
            } else {
                Unsupported(type.Name.Range, "this type expression");
            }
        }
        for (ui32 i = 0; i < type.OptionalDepth; ++i) { result = Call(pos, "OptionalType", {result}); }
        return result;
    }

    // SQL aggregate/window modifiers are not ordinary scalar-call arguments.
    // Reject them before considering explicit bindings or builtins, otherwise
    // a familiar function spelling could accidentally lose relational semantics.
    TNode Function(const TCall& call, const TSourceRange& range) {
        const auto pos = Pos(range);
        if (call.Over || call.Filter || call.StarArgument || call.Quantifier != ESetQuantifier::Default
            || call.NullTreatment != ENullTreatment::Default) {
            Unsupported(range, "aggregate and window call modifiers");
        }
        TExprNode::TListType args;
        for (const auto& arg : call.Arguments) {
            if (arg.Alias) { Unsupported(range, "named function arguments"); }
            args.push_back(Expression(*arg.Expression));
        }
        if (!std::holds_alternative<TName>(call.Callable->Value)) {
            args.insert(args.begin(), Expression(*call.Callable));
            return Call(pos, "Apply", std::move(args));
        }
        const auto* name = std::get_if<TName>(&call.Callable->Value);
        if (!name) { Unsupported(range, "dynamic call targets"); }
        auto spelling = Identifier(name->Name);
        const auto key = name->Module ? Identifier(*name->Module) + "::" + spelling : spelling;
        if (const auto* callable = Bindings.Callables.FindPtr(key); callable && *callable) {
            args.insert(args.begin(), *callable);
            return Call(pos, "Apply", std::move(args));
        }
        if (name->Module) {
            auto module = Identifier(*name->Module);
            if (AsciiEqualsIgnoreCase(module, "DateTime")) { module = "DateTime2"; }
            // SqlCall delegates overload selection, optional lifting and UDF
            // type configuration to the shared YQL annotation machinery.
            return Call(pos, "SqlCall", {Atom(pos, module + "." + spelling),
                Ctx.NewList(pos, {Call(pos, "PositionalArgs", std::move(args))})});
        }
        if (const auto issue = NormalizeName(Ctx.GetPosition(pos), spelling)) {
            Error(range, issue->GetMessage());
        }
        // SQL date/time constructors accept ISO text; YQL data callables
        // require the encoded integer. Use the same MiniKQL parser as SQL v1
        // rather than implementing a second calendar/interval parser.
        static const THashMap<TString, TString> temporalTypes = {
            {"date", "Date"}, {"datetime", "Datetime"}, {"timestamp", "Timestamp"}, {"interval", "Interval"},
            {"date32", "Date32"}, {"datetime64", "Datetime64"}, {"timestamp64", "Timestamp64"}, {"interval64", "Interval64"}};
        if (const auto* type = temporalTypes.FindPtr(spelling)) {
            if (args.size() != 1 || !args.front()->IsCallable("String")) { Error(range, "Date/time constructor requires one String literal"); }
            const auto text = args.front()->Head().Content();
            const auto slot = *NUdf::FindDataSlot(*type);
            const auto value = NKikimr::NMiniKQL::ValueFromString(slot, NUdf::TStringRef(text.data(), text.size()));
            if (!value) { Error(range, "Invalid date/time literal"); }
            TString encoded;
            switch (slot) {
                case EDataSlot::Date: encoded = ToString(value.Get<ui16>()); break;
                case EDataSlot::Datetime: encoded = ToString(value.Get<ui32>()); break;
                case EDataSlot::Timestamp: encoded = ToString(value.Get<ui64>()); break;
                case EDataSlot::Date32: encoded = ToString(value.Get<i32>()); break;
                default: encoded = ToString(value.Get<i64>()); break;
            }
            return Call(pos, *type, {Atom(pos, encoded)});
        }
        if (spelling == "astuple") {
            return Ctx.NewList(pos, std::move(args));
        }
        if ((spelling == "if" || spelling == "ifstrict") && args.size() >= 2) {
            args[0] = Predicate(pos, args[0]);
            if (args.size() == 2) { args.push_back(Call(pos, "Null", {})); }
        }
        if ((spelling == "substring" || spelling == "find" || spelling == "rfind") && args.size() == 2) {
            args.push_back(Call(pos, "Null", {}));
        }
        // Only direct scalar builtins belong here. SQL aggregate names (e.g.
        // MIN/MAX/SUM) must not become similarly named scalar YQL callables.
        static const THashMap<TString, TString> builtins = {
            {"abs", "Abs"}, {"coalesce", "Coalesce"}, {"if", "If"}, {"ifstrict", "IfStrict"}, {"just", "Just"},
            {"unwrap", "Unwrap"}, {"exists", "Exists"}, {"length", "Size"}, {"len", "Size"},
            {"charlength", "Size"}, {"characterlength", "Size"},
            {"listlength", "Length"}, {"listhasitems", "HasItems"},
            {"aslist", "AsListMayWarn"}, {"asliststrict", "AsListStrict"},
            {"minof", "Min"}, {"least", "Min"}, {"maxof", "Max"}, {"greatest", "Max"},
            {"substring", "Substring"}, {"find", "Find"}, {"rfind", "RFind"},
            {"startswith", "StartsWith"}, {"endswith", "EndsWith"}, {"stringcontains", "StringContains"},
            {"bitnot", "BitNot"}, {"bitand", "BitAnd"}, {"bitor", "BitOr"}, {"bitxor", "BitXor"},
            {"countbits", "CountBits"}, {"shiftleft", "ShiftLeft"}, {"shiftright", "ShiftRight"},
            {"rotleft", "RotLeft"}, {"rotright", "RotRight"}, {"nullif", "NullIf"}, {"typeof", "TypeOf"}};
        const auto* builtin = builtins.FindPtr(spelling);
        if (!builtin) { Unsupported(range, "this SQL function"); }
        return Call(pos, *builtin, std::move(args));
    }

    const TScript& Script;
    const TExpressionBindings& Bindings;
    TExprContext& Ctx;
    const TExpressionLoweringSettings& Settings;
    THashMap<const TParameter*, TNode> Locals;
};

} // namespace

TExprNode::TPtr LowerExpression(const TExpression& expression, const TScript& script,
    const TExpressionBindings& bindings, TExprContext& ctx, const TExpressionLoweringSettings& settings)
{
    try {
        return TLowerer(script, bindings, ctx, settings).Expression(expression);
    } catch (const TLoweringError&) {
        return {};
    }
}

} // namespace NKikimr::NKqp::NNewYql
