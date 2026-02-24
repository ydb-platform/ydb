
#include "sql_expression.h"
#include "select_yql.h"
#include "sql_select_yql.h"
#include "sql_call_expr.h"
#include "sql_select.h"
#include "sql_values.h"

#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>
#include <yql/essentials/utils/utf8.h>

#include <util/charset/wide.h>
#include <util/string/ascii.h>
#include <util/string/hex.h>
#include <util/generic/scope.h>

#include "antlr_token.h"

namespace NSQLTranslationV1 {

using NALPDefaultAntlr4::SQLv1Antlr4Lexer;

using namespace NSQLv1Generated;

TNodeResult TSqlExpression::BuildSourceOrNode(const TRule_expr& node) {
    // expr:
    //     or_subexpr (OR or_subexpr)*
    //   | type_name_composite
    switch (node.Alt_case()) {
        case TRule_expr::kAltExpr1: {
            auto getNode = [](const TRule_expr_TAlt1_TBlock2& b) -> const TRule_or_subexpr& { return b.GetRule_or_subexpr2(); };
            return BinOper("Or", node.GetAlt_expr1().GetRule_or_subexpr1(), getNode,
                           node.GetAlt_expr1().GetBlock2().begin(), node.GetAlt_expr1().GetBlock2().end(), {});
        }
        case TRule_expr::kAltExpr2: {
            return Wrap(TypeNode(node.GetAlt_expr2().GetRule_type_name_composite1()));
        }
        case TRule_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodeResult TSqlExpression::Build(const TRule_tuple_or_expr& node) {
    return TupleOrExpr(node);
}

TNodeResult TSqlExpression::Build(const TRule_expr& node) {
    const bool prevIsSourceAllowed = IsSourceAllowed_;
    Y_DEFER {
        IsSourceAllowed_ = prevIsSourceAllowed;
    };

    IsSourceAllowed_ = false;
    return BuildSourceOrNode(node);
}

TNodeResult TSqlExpression::Build(const TRule_lambda_or_parameter& node) {
    // lambda_or_parameter:
    //    lambda
    //  | bind_parameter
    switch (node.Alt_case()) {
        case TRule_lambda_or_parameter::kAltLambdaOrParameter1: {
            return LambdaRule(node.alt_lambda_or_parameter1().GetRule_lambda1());
        }
        case TRule_lambda_or_parameter::kAltLambdaOrParameter2: {
            TString named;
            if (!NamedNodeImpl(node.GetAlt_lambda_or_parameter2().GetRule_bind_parameter1(), named, *this)) {
                return std::unexpected(ESQLError::Basic);
            }

            TNodePtr namedNode = GetNamedNode(named);
            if (!namedNode) {
                return std::unexpected(ESQLError::Basic);
            }

            return TNonNull(std::move(namedNode));
        }
        case TRule_lambda_or_parameter::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TSourcePtr TSqlExpression::BuildSource(const TRule_select_or_expr& node) {
    TNodePtr result = Unwrap(SelectOrExpr(node));
    if (!result) {
        return nullptr;
    }

    if (TSourcePtr source = MoveOutIfSource(result)) {
        return source;
    }

    Ctx_.Error(result->GetPos()) << "Expected SELECT/PROCESS/REDUCE statement";
    return nullptr;
}

TNodePtr TSqlExpression::BuildSourceOrNode(const TRule_smart_parenthesis& node) {
    return Unwrap(SmartParenthesis(node));
}

TNodeResult TSqlExpression::SubExpr(const TRule_mul_subexpr& node, const TTrailingQuestions& tail) {
    // mul_subexpr: con_subexpr (DOUBLE_PIPE con_subexpr)*;
    auto getNode = [](const TRule_mul_subexpr::TBlock2& b) -> const TRule_con_subexpr& { return b.GetRule_con_subexpr2(); };
    return BinOper("Concat", node.GetRule_con_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodeResult TSqlExpression::SubExpr(const TRule_add_subexpr& node, const TTrailingQuestions& tail) {
    // add_subexpr: mul_subexpr ((ASTERISK | SLASH | PERCENT) mul_subexpr)*;
    auto getNode = [](const TRule_add_subexpr::TBlock2& b) -> const TRule_mul_subexpr& { return b.GetRule_mul_subexpr2(); };
    return BinOpList(node.GetRule_mul_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodeResult TSqlExpression::SubExpr(const TRule_bit_subexpr& node, const TTrailingQuestions& tail) {
    // bit_subexpr: add_subexpr ((PLUS | MINUS) add_subexpr)*;
    auto getNode = [](const TRule_bit_subexpr::TBlock2& b) -> const TRule_add_subexpr& { return b.GetRule_add_subexpr2(); };
    return BinOpList(node.GetRule_add_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodeResult TSqlExpression::SubExpr(const TRule_neq_subexpr& node, const TTrailingQuestions& tailExternal) {
    // neq_subexpr: bit_subexpr ((SHIFT_LEFT | shift_right | ROT_LEFT | rot_right | AMPERSAND | PIPE | CARET) bit_subexpr)*
    //   // trailing QUESTIONS are used in optional simple types (String?) and optional lambda args: ($x, $y?) -> ($x)
    //   ((double_question neq_subexpr) => double_question neq_subexpr | QUESTION+)?;
    YQL_ENSURE(tailExternal.Count == 0);
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && !node.HasBlock3();

    TTrailingQuestions tail;
    if (node.HasBlock3() && node.GetBlock3().Alt_case() == TRule_neq_subexpr::TBlock3::kAlt2) {
        auto& questions = node.GetBlock3().GetAlt2();
        tail.Count = questions.GetBlock1().size();
        tail.Pos = Ctx_.TokenPosition(questions.GetBlock1().begin()->GetToken1());
        YQL_ENSURE(tail.Count > 0);
    }

    auto getNode = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_bit_subexpr& { return b.GetRule_bit_subexpr2(); };
    TNodeResult result = BinOpList(node.GetRule_bit_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    if (!result) {
        return std::unexpected(result.error());
    }
    if (node.HasBlock3()) {
        auto& block = node.GetBlock3();
        switch (block.Alt_case()) {
            case TRule_neq_subexpr::TBlock3::kAlt1: {
                TSqlExpression altExpr(Ctx_, Mode_);
                altExpr.SetPure(IsPure_);
                TNodeResult altResult = SubExpr(block.GetAlt1().GetRule_neq_subexpr2(), {});
                if (!altResult) {
                    return std::unexpected(altResult.error());
                }
                const TVector<TNodePtr> args({std::move(*result), std::move(*altResult)});
                Token(block.GetAlt1().GetRule_double_question1().GetToken1());
                result = BuildBuiltinFunc(Ctx_, Ctx_.Pos(), "Coalesce", args,
                                          /*isYqlSelect=*/IsYqlSelectProduced_);
                break;
            }
            case TRule_neq_subexpr::TBlock3::kAlt2: {
                break; // It is handled higher
            }
            case TRule_neq_subexpr::TBlock3::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }
    return result;
}

TNodeResult TSqlExpression::SubExpr(const TRule_eq_subexpr& node, const TTrailingQuestions& tail) {
    // eq_subexpr: neq_subexpr ((LESS | LESS_OR_EQ | GREATER | GREATER_OR_EQ) neq_subexpr)*;
    auto getNode = [](const TRule_eq_subexpr::TBlock2& b) -> const TRule_neq_subexpr& { return b.GetRule_neq_subexpr2(); };
    return BinOpList(node.GetRule_neq_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodeResult TSqlExpression::SubExpr(const TRule_or_subexpr& node, const TTrailingQuestions& tail) {
    // or_subexpr: and_subexpr (AND and_subexpr)*;
    auto getNode = [](const TRule_or_subexpr::TBlock2& b) -> const TRule_and_subexpr& { return b.GetRule_and_subexpr2(); };
    return BinOper("And", node.GetRule_and_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodeResult TSqlExpression::SubExpr(const TRule_and_subexpr& node, const TTrailingQuestions& tail) {
    // and_subexpr: xor_subexpr (XOR xor_subexpr)*;
    auto getNode = [](const TRule_and_subexpr::TBlock2& b) -> const TRule_xor_subexpr& { return b.GetRule_xor_subexpr2(); };
    return BinOper("Xor", node.GetRule_xor_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

bool ChangefeedSettingsEntry(const TRule_changefeed_settings_entry& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter) {
    const auto id = IdEx(node.GetRule_an_id1(), ctx);
    if (alter) {
        // currently we don't support alter settings
        ctx.Error() << to_upper(id.Name) << " alter is not supported";
        return false;
    }

    const auto& setting = node.GetRule_changefeed_setting_value3();
    auto exprNode = Unwrap(ctx.Build(setting.GetRule_expr1()));

    if (!exprNode) {
        ctx.Context().Error(id.Pos) << "Invalid changefeed setting: " << id.Name;
        return false;
    }

    if (to_lower(id.Name) == "sink_type") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }

        const auto value = exprNode->GetLiteralValue();
        if (to_lower(value) == "local") {
            settings.SinkSettings = TChangefeedSettings::TLocalSinkSettings();
        } else {
            ctx.Context().Error() << "Unknown changefeed sink type: " << value;
            return false;
        }
    } else if (to_lower(id.Name) == "mode") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }
        settings.Mode = exprNode;
    } else if (to_lower(id.Name) == "format") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }
        settings.Format = exprNode;
    } else if (to_lower(id.Name) == "initial_scan") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.InitialScan = exprNode;
    } else if (to_lower(id.Name) == "user_sids") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.UserSIDs = exprNode;
    } else if (to_lower(id.Name) == "virtual_timestamps") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.VirtualTimestamps = exprNode;
    } else if (to_lower(id.Name) == "barriers_interval" || to_lower(id.Name) == "resolved_timestamps") {
        if (exprNode->GetOpName() != "Interval") {
            ctx.Context().Error() << "Literal of Interval type is expected for " << id.Name;
            return false;
        }
        settings.BarriersInterval = exprNode;
    } else if (to_lower(id.Name) == "schema_changes") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.SchemaChanges = exprNode;
    } else if (to_lower(id.Name) == "retention_period") {
        if (exprNode->GetOpName() != "Interval") {
            ctx.Context().Error() << "Literal of Interval type is expected for " << id.Name;
            return false;
        }
        settings.RetentionPeriod = exprNode;
    } else if (to_lower(id.Name) == "topic_auto_partitioning") {
        auto v = to_lower(exprNode->GetLiteralValue());
        if (v != "enabled" && v != "disabled") {
            ctx.Context().Error() << "Literal of Interval type is expected for " << id.Name;
        }
        settings.TopicAutoPartitioning = exprNode;
    } else if (to_lower(id.Name) == "topic_max_active_partitions") {
        if (!exprNode->IsIntegerLiteral()) {
            ctx.Context().Error() << "Literal of integer type is expected for " << id.Name;
            return false;
        }
        settings.TopicMaxActivePartitions = exprNode;
    } else if (to_lower(id.Name) == "topic_min_active_partitions") {
        if (!exprNode->IsIntegerLiteral()) {
            ctx.Context().Error() << "Literal of integer type is expected for " << id.Name;
            return false;
        }
        settings.TopicPartitions = exprNode;
    } else if (to_lower(id.Name) == "aws_region") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }
        settings.AwsRegion = exprNode;
    } else {
        ctx.Context().Error(id.Pos) << "Unknown changefeed setting: " << id.Name;
        return false;
    }

    return true;
}

bool ChangefeedSettings(const TRule_changefeed_settings& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter) {
    if (!ChangefeedSettingsEntry(node.GetRule_changefeed_settings_entry1(), ctx, settings, alter)) {
        return false;
    }

    for (auto& block : node.GetBlock2()) {
        if (!ChangefeedSettingsEntry(block.GetRule_changefeed_settings_entry2(), ctx, settings, alter)) {
            return false;
        }
    }

    return true;
}

bool CreateChangefeed(const TRule_changefeed& node, TSqlExpression& ctx, TVector<TChangefeedDescription>& changefeeds) {
    changefeeds.emplace_back(IdEx(node.GetRule_an_id2(), ctx));

    if (!ChangefeedSettings(node.GetRule_changefeed_settings5(), ctx, changefeeds.back().Settings, false)) {
        return false;
    }

    return true;
}

namespace {
bool WithoutAlpha(const std::string_view& literal) {
    return literal.cend() == std::find_if(literal.cbegin(), literal.cend(), [](char c) { return std::isalpha(c) || (c & '\x80'); });
}
} // namespace

TSQLStatus Expr(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr& node) {
    TNodeResult exprNode = sqlExpr.Build(node);
    if (!exprNode) {
        return std::unexpected(exprNode.error());
    }

    exprNodes.push_back(std::move(*exprNode));
    return std::monostate();
}

TSQLStatus ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node) {
    if (auto status = Expr(sqlExpr, exprNodes, node.GetRule_expr1()); !status) {
        return std::unexpected(status.error());
    }

    for (auto b : node.GetBlock2()) {
        sqlExpr.Token(b.GetToken1());
        if (auto status = Expr(sqlExpr, exprNodes, b.GetRule_expr2()); !status) {
            return std::unexpected(status.error());
        }
    }

    return std::monostate();
}

bool ParseNumbers(TContext& ctx, const TString& strOrig, ui64& value, TString& suffix) {
    const auto str = to_lower(strOrig);
    const auto strLen = str.size();
    ui64 base = 10;
    if (strLen > 2 && str[0] == '0') {
        const auto formatChar = str[1];
        if (formatChar == 'x') {
            base = 16;
        } else if (formatChar == 'o') {
            base = 8;
        } else if (formatChar == 'b') {
            base = 2;
        }
    }
    if (strLen > 1) {
        auto iter = str.cend() - 1;
        if (*iter == 'l' || *iter == 's' || *iter == 't' || *iter == 'i' || *iter == 'b' || *iter == 'n') {
            --iter;
        }
        if (*iter == 'u' || *iter == 'p') {
            --iter;
        }
        suffix = TString(++iter, str.cend());
    }
    value = 0;
    const TString digString(str.begin() + (base == 10 ? 0 : 2), str.end() - suffix.size());
    for (const char& cur : digString) {
        const ui64 curDigit = Char2DigitTable[static_cast<int>(cur)];
        if (curDigit >= base) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", char: '" << cur << "' is out of base: " << base;
            return false;
        }

        ui64 curValue = value;
        value *= base;
        bool overflow = ((value / base) != curValue);
        if (!overflow) {
            curValue = value;
            value += curDigit;
            overflow = value < curValue;
        }

        if (overflow) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", number limit overflow";
            return false;
        }
    }
    return true;
}

TNodePtr LiteralNumber(TContext& ctx, const TRule_integer& node) {
    const TString intergerString = ctx.Token(node.GetToken1());
    if (to_lower(intergerString).EndsWith("pn")) {
        // TODO: add validation
        return new TLiteralNode(ctx.Pos(), "PgNumeric", intergerString.substr(0, intergerString.size() - 2));
    }

    ui64 value;
    TString suffix;
    if (!ParseNumbers(ctx, intergerString, value, suffix)) {
        return {};
    }

    const bool noSpaceForInt32 = value >> 31;
    const bool noSpaceForInt64 = value >> 63;
    if (suffix == "") {
        bool implicitType = true;
        if (noSpaceForInt64) {
            return new TLiteralNumberNode<ui64>(ctx.Pos(), "Uint64", ToString(value), implicitType);
        } else if (noSpaceForInt32) {
            return new TLiteralNumberNode<i64>(ctx.Pos(), "Int64", ToString(value), implicitType);
        }
        return new TLiteralNumberNode<i32>(ctx.Pos(), "Int32", ToString(value), implicitType);
    } else if (suffix == "p") {
        bool implicitType = true;
        if (noSpaceForInt64) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << intergerString << ", 64 bit signed integer overflow";
            return {};
        } else if (noSpaceForInt32) {
            return new TLiteralNumberNode<i64>(ctx.Pos(), "PgInt8", ToString(value), implicitType);
        }
        return new TLiteralNumberNode<i32>(ctx.Pos(), "PgInt4", ToString(value), implicitType);
    } else if (suffix == "u") {
        return new TLiteralNumberNode<ui32>(ctx.Pos(), "Uint32", ToString(value));
    } else if (suffix == "ul") {
        return new TLiteralNumberNode<ui64>(ctx.Pos(), "Uint64", ToString(value));
    } else if (suffix == "ut") {
        return new TLiteralNumberNode<ui8>(ctx.Pos(), "Uint8", ToString(value));
    } else if (suffix == "t") {
        return new TLiteralNumberNode<i8>(ctx.Pos(), "Int8", ToString(value));
    } else if (suffix == "l") {
        return new TLiteralNumberNode<i64>(ctx.Pos(), "Int64", ToString(value));
    } else if (suffix == "us") {
        return new TLiteralNumberNode<ui16>(ctx.Pos(), "Uint16", ToString(value));
    } else if (suffix == "s") {
        return new TLiteralNumberNode<i16>(ctx.Pos(), "Int16", ToString(value));
    } else if (suffix == "ps") {
        return new TLiteralNumberNode<i16>(ctx.Pos(), "PgInt2", ToString(value));
    } else if (suffix == "pi") {
        return new TLiteralNumberNode<i32>(ctx.Pos(), "PgInt4", ToString(value));
    } else if (suffix == "pb") {
        return new TLiteralNumberNode<i64>(ctx.Pos(), "PgInt8", ToString(value));
    } else {
        ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << intergerString << ", invalid suffix: " << suffix;
        return {};
    }
}

TNodePtr LiteralReal(TContext& ctx, const TRule_real& node) {
    const TString value(ctx.Token(node.GetToken1()));
    YQL_ENSURE(!value.empty());
    auto lower = to_lower(value);
    if (lower.EndsWith("f")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "Float", value.substr(0, value.size() - 1));
    } else if (lower.EndsWith("p")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat8", value.substr(0, value.size() - 1));
    } else if (lower.EndsWith("pf4")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat4", value.substr(0, value.size() - 3));
    } else if (lower.EndsWith("pf8")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat8", value.substr(0, value.size() - 3));
    } else if (lower.EndsWith("pn")) {
        return new TLiteralNode(ctx.Pos(), "PgNumeric", value.substr(0, value.size() - 2));
    } else {
        return new TLiteralNumberNode<double>(ctx.Pos(), "Double", value);
    }
}

TMaybe<TExprOrIdent> TSqlExpression::LiteralExpr(const TRule_literal_value& node) {
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_literal_value::kAltLiteralValue1: {
            result.Expr = LiteralNumber(Ctx_, node.GetAlt_literal_value1().GetRule_integer1());
            break;
        }
        case TRule_literal_value::kAltLiteralValue2: {
            result.Expr = LiteralReal(Ctx_, node.GetAlt_literal_value2().GetRule_real1());
            break;
        }
        case TRule_literal_value::kAltLiteralValue3: {
            const TString value(Token(node.GetAlt_literal_value3().GetToken1()));
            return BuildLiteralTypedSmartStringOrId(Ctx_, value);
        }
        case TRule_literal_value::kAltLiteralValue4: {
            const auto& token = node.GetAlt_literal_value4().GetToken1();
            Ctx_.Error(GetPos(token)) << Token(token) << " literal is not supported yet";
            break;
        }
        case TRule_literal_value::kAltLiteralValue5: {
            Token(node.GetAlt_literal_value5().GetToken1());
            result.Expr = BuildLiteralNull(Ctx_.Pos());
            break;
        }
        case TRule_literal_value::kAltLiteralValue6: {
            const auto& token = node.GetAlt_literal_value6().GetToken1();
            Ctx_.Error(GetPos(token)) << Token(token) << " literal is not supported yet";
            break;
        }
        case TRule_literal_value::kAltLiteralValue7: {
            const auto& token = node.GetAlt_literal_value7().GetToken1();
            Ctx_.Error(GetPos(token)) << Token(token) << " literal is not supported yet";
            break;
        }
        case TRule_literal_value::kAltLiteralValue8: {
            const auto& token = node.GetAlt_literal_value8().GetToken1();
            Ctx_.Error(GetPos(token)) << Token(token) << " literal is not supported yet";
            break;
        }
        case TRule_literal_value::kAltLiteralValue9: {
            const TString value(to_lower(Token(node.GetAlt_literal_value9().GetRule_bool_value1().GetToken1())));
            result.Expr = BuildLiteralBool(Ctx_.Pos(), FromString<bool>(value));
            break;
        }
        case TRule_literal_value::kAltLiteralValue10: {
            result.Expr = BuildEmptyAction(Ctx_.Pos());
            break;
        }
        case TRule_literal_value::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

template <typename TUnarySubExprType>
TNodeResult TSqlExpression::UnaryExpr(const TUnarySubExprType& node, const TTrailingQuestions& tail) {
    if constexpr (std::is_same_v<TUnarySubExprType, TRule_unary_subexpr>) {
        if (node.Alt_case() == TRule_unary_subexpr::kAltUnarySubexpr1) {
            return UnaryCasualExpr(node.GetAlt_unary_subexpr1().GetRule_unary_casual_subexpr1(), tail);
        } else if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return std::unexpected(ESQLError::Basic);
        } else {
            MaybeUnnamedSmartParenOnTop_ = false;
            return JsonApiExpr(node.GetAlt_unary_subexpr2().GetRule_json_api_expr1());
        }
    } else {
        MaybeUnnamedSmartParenOnTop_ = false;
        if (node.Alt_case() == TRule_in_unary_subexpr::kAltInUnarySubexpr1) {
            return UnaryCasualExpr(node.GetAlt_in_unary_subexpr1().GetRule_in_unary_casual_subexpr1(), tail);
        } else if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return std::unexpected(ESQLError::Basic);
        } else {
            return JsonApiExpr(node.GetAlt_in_unary_subexpr2().GetRule_json_api_expr1());
        }
    }
}

TNodePtr TSqlExpression::JsonPathSpecification(const TRule_jsonpath_spec& node) {
    /*
        jsonpath_spec: STRING_VALUE;
    */
    TString value = Token(node.GetToken1());
    TPosition pos = Ctx_.Pos();

    auto parsed = StringContent(Ctx_, pos, value);
    if (!parsed) {
        return nullptr;
    }

    return new TCallNodeImpl(pos, "Utf8", {BuildQuotedAtom(pos, parsed->Content, parsed->Flags)});
}

TNodePtr TSqlExpression::JsonReturningTypeRule(const TRule_type_name_simple& node) {
    /*
        (RETURNING type_name_simple)?
    */
    return TypeSimple(node, /* onlyDataAllowed */ true);
}

TNodeResult TSqlExpression::JsonInputArg(const TRule_json_common_args& node) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */

    TNodeResult jsonExpr = Build(node.GetRule_expr1());
    if (!IsUnwrappable(jsonExpr)) {
        return std::unexpected(jsonExpr.error());
    }

    // FIXME(vityaman): do not suppress syntax errors.
    if (!jsonExpr || (*jsonExpr)->IsNull()) {
        jsonExpr = TNonNull(TNodePtr(new TCallNodeImpl(Ctx_.Pos(), "Nothing", {new TCallNodeImpl(Ctx_.Pos(), "OptionalType", {BuildDataType(Ctx_.Pos(), "Json")})})));
    }

    return jsonExpr;
}

TSQLStatus TSqlExpression::AddJsonVariable(const TRule_json_variable& node, TVector<TNodePtr>& children) {
    /*
        json_variable: expr AS json_variable_name;
    */
    TString rawName;
    TPosition namePos = Ctx_.Pos();
    ui32 nameFlags = 0;

    TNodeResult expr = Build(node.GetRule_expr1());
    const auto& nameRule = node.GetRule_json_variable_name3();
    switch (nameRule.GetAltCase()) {
        case TRule_json_variable_name::kAltJsonVariableName1:
            rawName = Id(nameRule.GetAlt_json_variable_name1().GetRule_id_expr1(), *this);
            nameFlags = TNodeFlags::ArbitraryContent;
            break;
        case TRule_json_variable_name::kAltJsonVariableName2: {
            const auto& token = nameRule.GetAlt_json_variable_name2().GetToken1();
            namePos = GetPos(token);
            auto parsed = StringContentOrIdContent(Ctx_, namePos, token.GetValue());
            if (!parsed) {
                return std::monostate(); // FIXME(vityaman): do not suppress syntax errors.
            }
            rawName = parsed->Content;
            nameFlags = parsed->Flags;
            break;
        }
        case TRule_json_variable_name::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    TNodePtr nameExpr = BuildQuotedAtom(namePos, rawName, nameFlags);

    if (!IsUnwrappable(expr)) {
        return std::unexpected(expr.error());
    }

    children.push_back(BuildTuple(namePos, {nameExpr, Unwrap(expr)}));
    return std::monostate();
}

TSQLStatus TSqlExpression::AddJsonVariables(const TRule_json_variables& node, TVector<TNodePtr>& children) {
    /*
        json_variables: json_variable (COMMA json_variable)*;
    */
    if (auto status = AddJsonVariable(node.GetRule_json_variable1(), children); !IsUnwrappable(status)) {
        return std::unexpected(status.error());
    }

    for (size_t i = 0; i < node.Block2Size(); i++) {
        if (auto status = AddJsonVariable(node.GetBlock2(i).GetRule_json_variable2(), children); !IsUnwrappable(status)) {
            return std::unexpected(status.error());
        }
    }

    return std::monostate();
}

TNodeResult TSqlExpression::JsonVariables(const TRule_json_common_args& node) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TVector<TNodePtr> variables;
    TPosition pos = Ctx_.Pos();
    if (node.HasBlock4()) {
        const auto& block = node.GetBlock4();
        pos = GetPos(block.GetToken1());
        if (auto status = AddJsonVariables(block.GetRule_json_variables2(), variables); !IsUnwrappable(status)) {
            return std::unexpected(status.error());
        }
    }
    return TNonNull(TNodePtr(new TCallNodeImpl(pos, "JsonVariables", variables)));
}

TSQLStatus TSqlExpression::AddJsonCommonArgs(const TRule_json_common_args& node, TVector<TNodePtr>& children) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TNodeResult jsonExpr = JsonInputArg(node);
    TNodePtr jsonPath = JsonPathSpecification(node.GetRule_jsonpath_spec3());
    TNodeResult variables = JsonVariables(node);

    if (!IsUnwrappable(jsonExpr)) {
        return std::unexpected(jsonExpr.error());
    }
    if (!jsonPath) {
        return std::unexpected(ESQLError::Basic);
    }
    if (!IsUnwrappable(variables)) {
        return std::unexpected(variables.error());
    }

    children.push_back(Unwrap(jsonExpr));
    children.push_back(jsonPath);
    children.push_back(Unwrap(variables));
    return std::monostate();
}

TNodePtr TSqlExpression::JsonValueCaseHandler(const TRule_json_case_handler& node, EJsonValueHandlerMode& mode) {
    /*
        json_case_handler: ERROR | NULL | (DEFAULT expr);
    */

    switch (node.GetAltCase()) {
        case TRule_json_case_handler::kAltJsonCaseHandler1: {
            const auto pos = GetPos(node.GetAlt_json_case_handler1().GetToken1());
            mode = EJsonValueHandlerMode::Error;
            return new TCallNodeImpl(pos, "Null", {});
        }
        case TRule_json_case_handler::kAltJsonCaseHandler2: {
            const auto pos = GetPos(node.GetAlt_json_case_handler2().GetToken1());
            mode = EJsonValueHandlerMode::DefaultValue;
            return new TCallNodeImpl(pos, "Null", {});
        }
        case TRule_json_case_handler::kAltJsonCaseHandler3:
            mode = EJsonValueHandlerMode::DefaultValue;
            return Unwrap(Build(node.GetAlt_json_case_handler3().GetRule_expr2())); // FIXME(YQL-20436): Use TSQLV1Result!
        case TRule_json_case_handler::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

void TSqlExpression::AddJsonValueCaseHandlers(const TRule_json_value& node, TVector<TNodePtr>& children) {
    /*
        json_case_handler*
    */
    if (node.Block5Size() > 2) {
        Ctx_.Error() << "Only 1 ON EMPTY and/or 1 ON ERROR clause is expected";
        Ctx_.IncrementMonCounter("sql_errors", "JsonValueTooManyHandleClauses");
        return;
    }

    TNodePtr onEmpty;
    EJsonValueHandlerMode onEmptyMode = EJsonValueHandlerMode::DefaultValue;
    TNodePtr onError;
    EJsonValueHandlerMode onErrorMode = EJsonValueHandlerMode::DefaultValue;
    for (size_t i = 0; i < node.Block5Size(); i++) {
        const auto block = node.GetBlock5(i);
        const bool isEmptyClause = to_lower(block.GetToken3().GetValue()) == "empty";

        if (isEmptyClause && onEmpty != nullptr) {
            Ctx_.Error() << "Only 1 ON EMPTY clause is expected";
            Ctx_.IncrementMonCounter("sql_errors", "JsonValueMultipleOnEmptyClauses");
            return;
        }

        if (!isEmptyClause && onError != nullptr) {
            Ctx_.Error() << "Only 1 ON ERROR clause is expected";
            Ctx_.IncrementMonCounter("sql_errors", "JsonValueMultipleOnErrorClauses");
            return;
        }

        if (isEmptyClause && onError != nullptr) {
            Ctx_.Error() << "ON EMPTY clause must be before ON ERROR clause";
            Ctx_.IncrementMonCounter("sql_errors", "JsonValueOnEmptyAfterOnError");
            return;
        }

        EJsonValueHandlerMode currentMode;
        TNodePtr currentHandler = JsonValueCaseHandler(block.GetRule_json_case_handler1(), currentMode);

        if (isEmptyClause) {
            onEmpty = currentHandler;
            onEmptyMode = currentMode;
        } else {
            onError = currentHandler;
            onErrorMode = currentMode;
        }
    }

    if (onEmpty == nullptr) {
        onEmpty = new TCallNodeImpl(Ctx_.Pos(), "Null", {});
    }

    if (onError == nullptr) {
        onError = new TCallNodeImpl(Ctx_.Pos(), "Null", {});
    }

    children.push_back(BuildQuotedAtom(Ctx_.Pos(), ToString(onEmptyMode), TNodeFlags::Default));
    children.push_back(onEmpty);
    children.push_back(BuildQuotedAtom(Ctx_.Pos(), ToString(onErrorMode), TNodeFlags::Default));
    children.push_back(onError);
}

TNodeResult TSqlExpression::JsonValueExpr(const TRule_json_value& node) {
    /*
        json_value: JSON_VALUE LPAREN
            json_common_args
            (RETURNING type_name_simple)?
            (json_case_handler ON (EMPTY | ERROR))*
        RPAREN;
    */
    TVector<TNodePtr> children;
    if (auto status = AddJsonCommonArgs(node.GetRule_json_common_args3(), children); !IsUnwrappable(status)) {
        return std::unexpected(status.error());
    }

    AddJsonValueCaseHandlers(node, children);

    if (node.HasBlock4()) {
        auto returningType = JsonReturningTypeRule(node.GetBlock4().GetRule_type_name_simple2());
        if (!returningType) {
            return std::unexpected(ESQLError::Basic);
        }

        children.push_back(returningType);
    }

    return TNonNull(TNodePtr(new TCallNodeImpl(GetPos(node.GetToken1()), "JsonValue", children)));
}

void TSqlExpression::AddJsonExistsHandler(const TRule_json_exists& node, TVector<TNodePtr>& children) {
    /*
        json_exists: JSON_EXISTS LPAREN
            json_common_args
            json_exists_handler?
        RPAREN;
    */
    auto buildJustBool = [&](const TPosition& pos, bool value) {
        return new TCallNodeImpl(pos, "Just", {BuildLiteralBool(pos, value)});
    };

    if (!node.HasBlock4()) {
        children.push_back(buildJustBool(Ctx_.Pos(), false));
        return;
    }

    const auto& handlerRule = node.GetBlock4().GetRule_json_exists_handler1();
    const auto& token = handlerRule.GetToken1();
    const auto pos = GetPos(token);
    const auto mode = to_lower(token.GetValue());
    if (mode == "unknown") {
        const auto nothingNode = new TCallNodeImpl(pos, "Nothing", {new TCallNodeImpl(pos, "OptionalType", {BuildDataType(pos, "Bool")})});
        children.push_back(nothingNode);
    } else if (mode != "error") {
        children.push_back(buildJustBool(pos, FromString<bool>(mode)));
    }
}

TNodeResult TSqlExpression::JsonExistsExpr(const TRule_json_exists& node) {
    /*
        json_exists: JSON_EXISTS LPAREN
            json_common_args
            json_exists_handler?
        RPAREN;
    */
    TVector<TNodePtr> children;
    if (auto status = AddJsonCommonArgs(node.GetRule_json_common_args3(), children); !IsUnwrappable(status)) {
        return std::unexpected(status.error());
    }

    AddJsonExistsHandler(node, children);

    return TNonNull(TNodePtr(new TCallNodeImpl(GetPos(node.GetToken1()), "JsonExists", children)));
}

EJsonQueryWrap TSqlExpression::JsonQueryWrapper(const TRule_json_query& node) {
    /*
        json_query: JSON_QUERY LPAREN
            json_common_args
            (json_query_wrapper WRAPPER)?
            (json_query_handler ON EMPTY)?
            (json_query_handler ON ERROR)?
        RPAREN;
    */
    // default behaviour - no wrapping
    if (!node.HasBlock4()) {
        return EJsonQueryWrap::NoWrap;
    }

    // WITHOUT ARRAY? - no wrapping
    const auto& wrapperRule = node.GetBlock4().GetRule_json_query_wrapper1();
    if (wrapperRule.GetAltCase() == TRule_json_query_wrapper::kAltJsonQueryWrapper1) {
        return EJsonQueryWrap::NoWrap;
    }

    // WITH (CONDITIONAL | UNCONDITIONAL)? ARRAY? - wrapping depends on 2nd token. Default is UNCONDITIONAL
    const auto& withWrapperRule = wrapperRule.GetAlt_json_query_wrapper2();
    if (!withWrapperRule.HasBlock2()) {
        return EJsonQueryWrap::Wrap;
    }

    const auto& token = withWrapperRule.GetBlock2().GetToken1();
    if (to_lower(token.GetValue()) == "conditional") {
        return EJsonQueryWrap::ConditionalWrap;
    } else {
        return EJsonQueryWrap::Wrap;
    }
}

EJsonQueryHandler TSqlExpression::JsonQueryHandler(const TRule_json_query_handler& node) {
    /*
        json_query_handler: ERROR | NULL | (EMPTY ARRAY) | (EMPTY OBJECT);
    */
    switch (node.GetAltCase()) {
        case TRule_json_query_handler::kAltJsonQueryHandler1:
            return EJsonQueryHandler::Error;
        case TRule_json_query_handler::kAltJsonQueryHandler2:
            return EJsonQueryHandler::Null;
        case TRule_json_query_handler::kAltJsonQueryHandler3:
            return EJsonQueryHandler::EmptyArray;
        case TRule_json_query_handler::kAltJsonQueryHandler4:
            return EJsonQueryHandler::EmptyObject;
        case TRule_json_query_handler::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodeResult TSqlExpression::JsonQueryExpr(const TRule_json_query& node) {
    /*
        json_query: JSON_QUERY LPAREN
            json_common_args
            (json_query_wrapper WRAPPER)?
            (json_query_handler ON EMPTY)?
            (json_query_handler ON ERROR)?
        RPAREN;
    */

    TVector<TNodePtr> children;
    if (auto status = AddJsonCommonArgs(node.GetRule_json_common_args3(), children); !IsUnwrappable(status)) {
        return std::unexpected(status.error());
    }

    auto addChild = [&](TPosition pos, const TString& content) {
        children.push_back(BuildQuotedAtom(pos, content, TNodeFlags::Default));
    };

    const auto wrapMode = JsonQueryWrapper(node);
    addChild(Ctx_.Pos(), ToString(wrapMode));

    auto onEmpty = EJsonQueryHandler::Null;
    if (node.HasBlock5()) {
        if (wrapMode != EJsonQueryWrap::NoWrap) {
            Ctx_.Error() << "ON EMPTY is prohibited because WRAPPER clause is specified";
            Ctx_.IncrementMonCounter("sql_errors", "JsonQueryOnEmptyWithWrapper");
            return std::unexpected(ESQLError::Basic);
        }
        onEmpty = JsonQueryHandler(node.GetBlock5().GetRule_json_query_handler1());
    }
    addChild(Ctx_.Pos(), ToString(onEmpty));

    auto onError = EJsonQueryHandler::Null;
    if (node.HasBlock6()) {
        onError = JsonQueryHandler(node.GetBlock6().GetRule_json_query_handler1());
    }
    addChild(Ctx_.Pos(), ToString(onError));

    return TNonNull(TNodePtr(new TCallNodeImpl(GetPos(node.GetToken1()), "JsonQuery", children)));
}

TNodeResult TSqlExpression::JsonApiExpr(const TRule_json_api_expr& node) {
    /*
        json_api_expr: json_value | json_exists | json_query;
    */
    TPosition pos = Ctx_.Pos();
    TNodeResult result = std::unexpected(ESQLError::Basic);
    switch (node.GetAltCase()) {
        case TRule_json_api_expr::kAltJsonApiExpr1: {
            const auto& jsonValue = node.GetAlt_json_api_expr1().GetRule_json_value1();
            pos = GetPos(jsonValue.GetToken1());
            result = JsonValueExpr(jsonValue);
            break;
        }
        case TRule_json_api_expr::kAltJsonApiExpr2: {
            const auto& jsonExists = node.GetAlt_json_api_expr2().GetRule_json_exists1();
            pos = GetPos(jsonExists.GetToken1());
            result = JsonExistsExpr(jsonExists);
            break;
        }
        case TRule_json_api_expr::kAltJsonApiExpr3: {
            const auto& jsonQuery = node.GetAlt_json_api_expr3().GetRule_json_query1();
            pos = GetPos(jsonQuery.GetToken1());
            result = JsonQueryExpr(jsonQuery);
            break;
        }
        case TRule_json_api_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return result;
}

TNodePtr TSqlExpression::RowPatternVarAccess(TString var, const TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2 block) {
    switch (block.GetAltCase()) {
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt1:
            break;
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt2:
            break;
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt3:
            switch (block.GetAlt3().GetRule_an_id_or_type1().GetAltCase()) {
                case TRule_an_id_or_type::kAltAnIdOrType1: {
                    const auto& idOrType = block.GetAlt3().GetRule_an_id_or_type1().GetAlt_an_id_or_type1().GetRule_id_or_type1();
                    switch (idOrType.GetAltCase()) {
                        case TRule_id_or_type::kAltIdOrType1: {
                            const auto column = Id(idOrType.GetAlt_id_or_type1().GetRule_id1(), *this);
                            return BuildMatchRecognizeColumnAccess(Ctx_.Pos(), std::move(var), std::move(column));
                        }
                        case TRule_id_or_type::kAltIdOrType2:
                            break;
                        case TRule_id_or_type::ALT_NOT_SET:
                            Y_UNREACHABLE();
                    }
                    break;
                }
                case TRule_an_id_or_type::kAltAnIdOrType2:
                    break;
                case TRule_an_id_or_type::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }
            break;
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return {};
}

template <typename TUnaryCasualExprRule>
TNodeResult TSqlExpression::UnaryCasualExpr(const TUnaryCasualExprRule& node, const TTrailingQuestions& tail) {
    // unary_casual_subexpr: (id_expr | atom_expr) unary_subexpr_suffix;
    // OR
    // in_unary_casual_subexpr: (id_expr_in | in_atom_expr) unary_subexpr_suffix;
    // where
    // unary_subexpr_suffix: (key_expr | invoke_expr |(DOT (bind_parameter | DIGITS | id)))* (COLLATE id)?;

    const auto& suffix = node.GetRule_unary_subexpr_suffix2();
    const bool suffixIsEmpty = suffix.GetBlock1().empty() && !suffix.HasBlock2();
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && suffixIsEmpty;
    TString name;
    TNodePtr expr;
    bool typePossible = false;
    auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TUnaryCasualExprRule::TBlock1::kAlt1: {
            MaybeUnnamedSmartParenOnTop_ = false;
            auto& alt = block.GetAlt1();
            if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                name = Id(alt.GetRule_id_expr1(), *this);
                typePossible = !IsQuotedId(alt.GetRule_id_expr1(), *this);
            } else {
                // type was never possible here
                name = Id(alt.GetRule_id_expr_in1(), *this);
            }
            break;
        }
        case TUnaryCasualExprRule::TBlock1::kAlt2: {
            auto& alt = block.GetAlt2();
            TSQLResult<TExprOrIdent> exprOrId;
            if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                exprOrId = AtomExpr(alt.GetRule_atom_expr1(), suffixIsEmpty ? tail : TTrailingQuestions{});
            } else {
                MaybeUnnamedSmartParenOnTop_ = false;
                exprOrId = InAtomExpr(alt.GetRule_in_atom_expr1(), suffixIsEmpty ? tail : TTrailingQuestions{});
            }

            if (!exprOrId) {
                if (exprOrId.error() == ESQLError::Basic) {
                    Ctx_.IncrementMonCounter("sql_errors", "BadAtomExpr");
                    return std::unexpected(ESQLError::Basic);
                } else {
                    return std::unexpected(exprOrId.error());
                }
            }
            if (!exprOrId->Expr) {
                name = exprOrId->Ident;
            } else {
                expr = exprOrId->Expr;
            }
            break;
        }
        case TUnaryCasualExprRule::TBlock1::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    // bool onlyDots = true;
    bool isColumnRef = !expr;
    bool isFirstElem = true;

    for (auto& _b : suffix.GetBlock1()) {
        auto& b = _b.GetBlock1();
        switch (b.Alt_case()) {
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt1: {
                // key_expr
                // onlyDots = false;
                break;
            }
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2: {
                // invoke_expr - cannot be a column, function name
                if (isFirstElem) {
                    isColumnRef = false;
                }

                // onlyDots = false;
                break;
            }
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt3: {
                // In case of MATCH_RECOGNIZE lambdas
                // X.Y is treated as Var.Column access
                if (isColumnRef && (EColumnRefState::MatchRecognizeMeasures == Ctx_.GetColumnReferenceState() ||
                                    EColumnRefState::MatchRecognizeDefine == Ctx_.GetColumnReferenceState() ||
                                    EColumnRefState::MatchRecognizeDefineAggregate == Ctx_.GetColumnReferenceState())) {
                    if (suffix.GetBlock1().size() != 1) {
                        Ctx_.Error() << "Expected Var.Column, but got chain of " << suffix.GetBlock1().size() << " column accesses";
                        return std::unexpected(ESQLError::Basic);
                    }
                    return Wrap(RowPatternVarAccess(std::move(name), b.GetAlt3().GetBlock2()));
                }
                break;
            }
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::ALT_NOT_SET:
                Y_UNREACHABLE();
        }

        isFirstElem = false;
    }

    isFirstElem = true;
    TVector<INode::TIdPart> ids;
    INode::TPtr lastExpr;
    if (!isColumnRef) {
        lastExpr = expr;
    } else {
        const bool flexibleTypes = Ctx_.FlexibleTypes;
        bool columnOrType = false;
        auto columnRefsState = Ctx_.GetColumnReferenceState();
        bool explicitPgType = columnRefsState == EColumnRefState::AsPgType;
        if (explicitPgType && typePossible && suffixIsEmpty) {
            auto pgType = BuildSimpleType(Ctx_, Ctx_.Pos(), name, false);
            if (pgType && tail.Count) {
                Ctx_.Error() << "Optional types are not supported in this context";
                return std::unexpected(ESQLError::Basic);
            }
            return Wrap(std::move(pgType));
        }
        if (auto simpleType = LookupSimpleType(name, flexibleTypes, false); simpleType && typePossible && suffixIsEmpty) {
            if (tail.Count > 0 || columnRefsState == EColumnRefState::Deny || !flexibleTypes) {
                // a type
                return Wrap(AddOptionals(BuildSimpleType(Ctx_, Ctx_.Pos(), name, false), tail.Count));
            }
            // type or column: ambiguity will be resolved on type annotation stage
            columnOrType = columnRefsState == EColumnRefState::Allow;
        }
        if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return std::unexpected(ESQLError::Basic);
        }
        if (!Ctx_.CheckColumnReference(Ctx_.Pos(), name)) {
            return std::unexpected(ESQLError::Basic);
        }

        TNodePtr id;
        if (IsYqlSelectProduced_) {
            id = BuildYqlColumnRef(Ctx_.Pos());
        } else if (columnOrType) {
            id = BuildColumnOrType(Ctx_.Pos());
        } else {
            id = BuildColumn(Ctx_.Pos());
        }
        ids.emplace_back(std::move(id));

        ids.push_back(name);
    }

    TPosition pos(Ctx_.Pos());
    for (auto& _b : suffix.GetBlock1()) {
        auto& b = _b.GetBlock1();
        switch (b.Alt_case()) {
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt1: {
                // key_expr
                TNodePtr keyExpr = KeyExpr(b.GetAlt1().GetRule_key_expr1());
                if (!keyExpr) {
                    Ctx_.IncrementMonCounter("sql_errors", "BadKeyExpr");
                    return std::unexpected(ESQLError::Basic);
                }

                if (!lastExpr) {
                    lastExpr = BuildAccess(pos, ids, false);
                    ids.clear();
                }

                ids.push_back(lastExpr);
                ids.push_back(keyExpr);
                lastExpr = BuildAccess(pos, ids, true);
                ids.clear();
                break;
            }
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2: {
                // invoke_expr - cannot be a column, function name
                TSqlCallExpr call(Ctx_, Mode_);
                call.SetYqlSelectProduced(IsYqlSelectProduced_);
                if (isFirstElem && !name.empty()) {
                    call.AllowDistinct();
                    call.InitName(name);
                } else {
                    call.InitExpr(lastExpr);
                }

                bool initRet = call.Init(b.GetAlt2().GetRule_invoke_expr1());
                if (initRet) {
                    call.IncCounters();
                }

                if (!initRet) {
                    return std::unexpected(ESQLError::Basic);
                }

                if (auto result = call.BuildCall()) {
                    lastExpr = std::move(*result);
                    if (auto* call = lastExpr->GetCallNode(); call && call->GetOpName() == "DataType") {
                        lastExpr = AddOptionals(std::move(lastExpr), tail.Count);
                    }
                } else {
                    return std::unexpected(result.error());
                }

                break;
            }
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt3: {
                // dot
                if (lastExpr) {
                    if (TSourcePtr source = MoveOutIfSource(lastExpr)) {
                        lastExpr = ToSubSelectNode(std::move(source));
                        if (!lastExpr) {
                            return std::unexpected(ESQLError::Basic);
                        }
                    }

                    ids.push_back(lastExpr);
                }

                auto bb = b.GetAlt3().GetBlock2();
                switch (bb.Alt_case()) {
                    case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt1: {
                        TString named;
                        if (!NamedNodeImpl(bb.GetAlt1().GetRule_bind_parameter1(), named, *this)) {
                            return std::unexpected(ESQLError::Basic);
                        }
                        auto namedNode = GetNamedNode(named);
                        if (!namedNode) {
                            return std::unexpected(ESQLError::Basic);
                        }

                        ids.push_back(named);
                        ids.back().Expr = namedNode;
                        break;
                    }
                    case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt2: {
                        const TString str(Token(bb.GetAlt2().GetToken1()));
                        ids.push_back(str);
                        break;
                    }
                    case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt3: {
                        ids.push_back(Id(bb.GetAlt3().GetRule_an_id_or_type1(), *this));
                        break;
                    }
                    case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::ALT_NOT_SET:
                        Y_UNREACHABLE();
                }

                if (lastExpr) {
                    lastExpr = BuildAccess(pos, ids, false);
                    ids.clear();
                }

                break;
            }
            case TRule_unary_subexpr_suffix::TBlock1::TBlock1::ALT_NOT_SET:
                Y_UNREACHABLE();
        }

        isFirstElem = false;
    }

    if (!lastExpr) {
        lastExpr = BuildAccess(pos, ids, false);
        ids.clear();
    }

    if (suffix.HasBlock2()) {
        Ctx_.IncrementMonCounter("sql_errors", "CollateUnarySubexpr");
        Error() << "unary_subexpr: COLLATE is not implemented yet";
        return std::unexpected(ESQLError::Basic);
    }

    return Wrap(std::move(lastExpr));
}

TNodePtr TSqlExpression::BindParameterRule(const TRule_bind_parameter& rule, const TTrailingQuestions& tail) {
    TString namedArg;
    if (!NamedNodeImpl(rule, namedArg, *this)) {
        return {};
    }
    if (SmartParenthesisMode_ == ESmartParenthesis::SqlLambdaParams) {
        Ctx_.IncrementMonCounter("sql_features", "LambdaArgument");
        if (tail.Count > 1) {
            Ctx_.Error(tail.Pos) << "Expecting at most one '?' token here (for optional lambda parameters), but got " << tail.Count;
            return {};
        }
        return BuildAtom(Ctx_.Pos(), namedArg, NYql::TNodeFlags::ArbitraryContent, tail.Count != 0);
    }
    if (tail.Count) {
        UnexpectedQuestionToken(tail);
        return {};
    }
    Ctx_.IncrementMonCounter("sql_features", "NamedNodeUseAtom");
    auto ret = GetNamedNode(namedArg);
    if (ret) {
        ret->SetRefPos(Ctx_.Pos());
    }

    return ret;
}

TNodeResult TSqlExpression::LambdaRule(const TRule_lambda& rule) {
    const auto& alt = rule;
    const bool isSqlLambda = alt.HasBlock2();
    if (!isSqlLambda) {
        return SmartParenthesis(alt.GetRule_smart_parenthesis1());
    }

    MaybeUnnamedSmartParenOnTop_ = false;
    TNodePtr parenthesis;
    {
        // we allow column reference here to postpone error and report it with better description in SqlLambdaParams
        TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
        TSqlExpression expr(Ctx_, Mode_);
        expr.SetPure(IsPure_);
        expr.SetSmartParenthesisMode(ESmartParenthesis::SqlLambdaParams);
        parenthesis = Unwrap(expr.SmartParenthesis(alt.GetRule_smart_parenthesis1()));
    }
    if (!parenthesis) {
        return std::unexpected(ESQLError::Basic);
    }

    ui32 optionalArgumentsCount = 0;
    TVector<TSymbolNameWithPos> args;
    if (!SqlLambdaParams(parenthesis, args, optionalArgumentsCount)) {
        return std::unexpected(ESQLError::Basic);
    }
    auto bodyBlock = alt.GetBlock2();
    Token(bodyBlock.GetToken1());
    TPosition pos(Ctx_.Pos());
    TVector<TNodePtr> exprSeq;
    for (auto& arg : args) {
        arg.Name = PushNamedAtom(arg.Pos, arg.Name);
    }
    bool ret = false;
    TColumnRefScope scope(Ctx_, EColumnRefState::Deny);
    scope.SetNoColumnErrContext("in lambda function");
    if (bodyBlock.GetBlock2().HasAlt1()) {
        ret = SqlLambdaExprBody(Ctx_, bodyBlock.GetBlock2().GetAlt1().GetRule_expr2(), exprSeq);
    } else {
        ret = SqlLambdaExprBody(Ctx_, bodyBlock.GetBlock2().GetAlt2().GetRule_lambda_body2(), exprSeq);
    }

    TVector<TString> argNames;
    for (const auto& arg : args) {
        argNames.push_back(arg.Name);
        if (!PopNamedNode(arg.Name)) {
            return std::unexpected(ESQLError::Basic);
        }
    }
    if (!ret) {
        return std::unexpected(ESQLError::Basic);
    }

    auto lambdaNode = BuildSqlLambda(pos, std::move(argNames), std::move(exprSeq));
    if (optionalArgumentsCount > 0) {
        lambdaNode = new TCallNodeImpl(pos, "WithOptionalArgs", {lambdaNode,
                                                                 BuildQuotedAtom(pos, ToString(optionalArgumentsCount), TNodeFlags::Default)});
    }

    return Wrap(lambdaNode);
}

TNodeResult TSqlExpression::CastRule(const TRule_cast_expr& rule) {
    const auto& alt = rule;

    Token(alt.GetToken1());
    TPosition pos(Ctx_.Pos());

    TSqlExpression expr(Ctx_, Mode_);
    expr.SetPure(IsPure_);
    expr.SetYqlSelectProduced(IsYqlSelectProduced_);
    TNodeResult exprNode = expr.Build(rule.GetRule_expr3());
    if (!exprNode) {
        return std::unexpected(exprNode.error());
    }

    TNodePtr type = TypeNodeOrBind(rule.GetRule_type_name_or_bind5());
    if (!type) {
        return std::unexpected(ESQLError::Basic);
    }

    return TNonNull(TNodePtr(new TCallNodeImpl(pos, "SafeCast", {*exprNode, type})));
}

TNodePtr TSqlExpression::BitCastRule(const TRule_bitcast_expr& rule) {
    Ctx_.IncrementMonCounter("sql_features", "BitCast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx_.Pos());
    TSqlExpression expr(Ctx_, Mode_);
    expr.SetPure(IsPure_);
    auto exprNode = Unwrap(expr.Build(rule.GetRule_expr3()));
    if (!exprNode) {
        return {};
    }
    auto type = TypeSimple(rule.GetRule_type_name_simple5(), true);
    if (!type) {
        return {};
    }
    return new TCallNodeImpl(pos, "BitCast", {exprNode, type});
}

TNodeResult TSqlExpression::ExistsRule(const TRule_exists_expr& rule) {
    if (IsYqlSelectProduced_) {
        return BuildYqlExists(Ctx_, Mode_, rule);
    }

    Ctx_.IncrementMonCounter("sql_features", "Exists");

    TPosition pos;
    TSourcePtr source;
    Token(rule.GetToken2());
    switch (rule.GetBlock3().Alt_case()) {
        case TRule_exists_expr::TBlock3::kAlt1: {
            const auto& alt = rule.GetBlock3().GetAlt1().GetRule_select_stmt1();
            TSqlSelect select(Ctx_, Mode_);
            source = select.Build(alt, pos);
            break;
        }
        case TRule_exists_expr::TBlock3::kAlt2: {
            const auto& alt = rule.GetBlock3().GetAlt2().GetRule_values_stmt1();
            TSqlValues values(Ctx_, Mode_);
            source = values.Build(alt, pos);
            break;
        }
        case TRule_exists_expr::TBlock3::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    if (!source) {
        Ctx_.IncrementMonCounter("sql_errors", "BadSource");
        return std::unexpected(ESQLError::Basic);
    }

    const bool checkExist = true;
    auto select = BuildSourceNode(Ctx_.Pos(), source, checkExist, Ctx_.Settings.EmitReadsForExists);
    return BuildBuiltinFunc(Ctx_, Ctx_.Pos(), "ListHasItems", {select},
                            /*isYqlSelect=*/IsYqlSelectProduced_);
}

TNodeResult TSqlExpression::CaseRule(const TRule_case_expr& rule) {
    // case_expr: CASE expr? when_expr+ (ELSE expr)? END;
    // when_expr: WHEN expr THEN expr;
    Ctx_.IncrementMonCounter("sql_features", "Case");
    const auto& alt = rule;

    Token(alt.GetToken1());
    if (!alt.HasBlock4()) {
        Ctx_.IncrementMonCounter("sql_errors", "ElseIsRequired");
        Error() << "ELSE is required";
        return std::unexpected(ESQLError::Basic);
    }

    Token(alt.GetBlock4().GetToken1());
    TNodeResult elseExpr = [&] {
        TSqlExpression expr(Ctx_, Mode_);
        expr.SetPure(IsPure_);
        expr.SetYqlSelectProduced(IsYqlSelectProduced_);
        return expr.Build(alt.GetBlock4().GetRule_expr2());
    }();

    TNodePtr caseExpr;
    if (alt.HasBlock2()) {
        TSqlExpression expr(Ctx_, Mode_);
        expr.SetPure(IsPure_);
        expr.SetYqlSelectProduced(IsYqlSelectProduced_);
        if (auto result = expr.Build(alt.GetBlock2().GetRule_expr1())) {
            caseExpr = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }
    }

    TVector<TCaseBranch> branches;
    for (size_t i = 0; i < alt.Block3Size(); ++i) {
        branches.emplace_back();
        const auto& block = alt.GetBlock3(i).GetRule_when_expr1();

        Token(block.GetToken1());
        TSqlExpression condExpr(Ctx_, Mode_);
        condExpr.SetPure(IsPure_);
        condExpr.SetYqlSelectProduced(IsYqlSelectProduced_);
        if (auto result = condExpr.Build(block.GetRule_expr2())) {
            branches.back().Pred = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }

        if (caseExpr) {
            branches.back().Pred = BuildBinaryOp(Ctx_, Ctx_.Pos(), "==", caseExpr->Clone(), branches.back().Pred);
        }
        if (!branches.back().Pred) {
            return std::unexpected(ESQLError::Basic);
        }

        Token(block.GetToken3());
        TSqlExpression thenExpr(Ctx_, Mode_);
        thenExpr.SetPure(IsPure_);
        thenExpr.SetYqlSelectProduced(IsYqlSelectProduced_);
        if (auto result = thenExpr.Build(block.GetRule_expr4())) {
            branches.back().Value = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }
    }

    auto final = ReduceCaseBranches(branches.begin(), branches.end());
    if (!final) {
        return std::unexpected(final.error());
    }

    if (!elseExpr) {
        return std::unexpected(elseExpr.error());
    }

    return BuildBuiltinFunc(Ctx_, Ctx_.Pos(), "If", {final->Pred, final->Value, *elseExpr},
                            /*isYqlSelect=*/IsYqlSelectProduced_);
}

TSQLResult<TExprOrIdent> TSqlExpression::AtomExpr(const TRule_atom_expr& node, const TTrailingQuestions& tail) {
    // atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | lambda
    //   | cast_expr
    //   | exists_expr
    //   | case_expr
    //   | an_id_or_type NAMESPACE (id_or_type | STRING_VALUE)
    //   | value_constructor
    //   | bitcast_expr
    //   | list_literal
    //   | dict_literal
    //   | struct_literal
    // ;
    if (node.Alt_case() != TRule_atom_expr::kAltAtomExpr2 && tail.Count) {
        UnexpectedQuestionToken(tail);
        return std::unexpected(ESQLError::Basic);
    }
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && (node.Alt_case() == TRule_atom_expr::kAltAtomExpr3);
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_atom_expr::kAltAtomExpr1:
            Ctx_.IncrementMonCounter("sql_features", "LiteralExpr");
            if (auto result = LiteralExpr(node.GetAlt_atom_expr1().GetRule_literal_value1())) {
                return std::move(*result);
            } else {
                return std::unexpected(ESQLError::Basic);
            }
        case TRule_atom_expr::kAltAtomExpr2:
            result.Expr = BindParameterRule(node.GetAlt_atom_expr2().GetRule_bind_parameter1(), tail);
            break;
        case TRule_atom_expr::kAltAtomExpr3:
            if (auto expected = LambdaRule(node.GetAlt_atom_expr3().GetRule_lambda1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_atom_expr::kAltAtomExpr4:
            if (auto expected = CastRule(node.GetAlt_atom_expr4().GetRule_cast_expr1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_atom_expr::kAltAtomExpr5:
            if (auto expected = ExistsRule(node.GetAlt_atom_expr5().GetRule_exists_expr1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_atom_expr::kAltAtomExpr6:
            if (auto expected = CaseRule(node.GetAlt_atom_expr6().GetRule_case_expr1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_atom_expr::kAltAtomExpr7: {
            const auto& alt = node.GetAlt_atom_expr7();
            TString module(Id(alt.GetRule_an_id_or_type1(), *this));
            TPosition pos(Ctx_.Pos());
            TString name;
            switch (alt.GetBlock3().Alt_case()) {
                case TRule_atom_expr::TAlt7::TBlock3::kAlt1:
                    name = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), *this);
                    break;
                case TRule_atom_expr::TAlt7::TBlock3::kAlt2: {
                    name = Token(alt.GetBlock3().GetAlt2().GetToken1());
                    if (Ctx_.AnsiQuotedIdentifiers && name.StartsWith('"')) {
                        // same as previous case
                        name = IdContentFromString(Ctx_, name);
                    } else {
                        module = "@" + module;
                    }
                    break;
                }
                case TRule_atom_expr::TAlt7::TBlock3::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }
            result.Expr = BuildCallable(pos, module, name, {});
            break;
        }
        case TRule_atom_expr::kAltAtomExpr8: {
            result.Expr = ValueConstructor(node.GetAlt_atom_expr8().GetRule_value_constructor1());
            break;
        }
        case TRule_atom_expr::kAltAtomExpr9:
            result.Expr = BitCastRule(node.GetAlt_atom_expr9().GetRule_bitcast_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr10:
            result.Expr = ListLiteral(node.GetAlt_atom_expr10().GetRule_list_literal1());
            break;
        case TRule_atom_expr::kAltAtomExpr11:
            result.Expr = DictLiteral(node.GetAlt_atom_expr11().GetRule_dict_literal1());
            break;
        case TRule_atom_expr::kAltAtomExpr12:
            result.Expr = StructLiteral(node.GetAlt_atom_expr12().GetRule_struct_literal1());
            break;
        case TRule_atom_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    if (!result.Expr) {
        return std::unexpected(ESQLError::Basic);
    }
    return result;
}

TSQLResult<TExprOrIdent> TSqlExpression::InAtomExpr(const TRule_in_atom_expr& node, const TTrailingQuestions& tail) {
    // in_atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | lambda
    //   | cast_expr
    //   | case_expr
    //   | an_id_or_type NAMESPACE (id_or_type | STRING_VALUE)
    //   | value_constructor
    //   | bitcast_expr
    //   | list_literal
    //   | dict_literal
    //   | struct_literal
    // ;
    if (node.Alt_case() != TRule_in_atom_expr::kAltInAtomExpr2 && tail.Count) {
        UnexpectedQuestionToken(tail);
        return std::unexpected(ESQLError::Basic);
    }
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_in_atom_expr::kAltInAtomExpr1:
            Ctx_.IncrementMonCounter("sql_features", "LiteralExpr");
            if (auto result = LiteralExpr(node.GetAlt_in_atom_expr1().GetRule_literal_value1())) {
                return *result;
            } else {
                return std::unexpected(ESQLError::Basic);
            }
        case TRule_in_atom_expr::kAltInAtomExpr2:
            result.Expr = BindParameterRule(node.GetAlt_in_atom_expr2().GetRule_bind_parameter1(), tail);
            break;
        case TRule_in_atom_expr::kAltInAtomExpr3:
            if (auto expected = LambdaRule(node.GetAlt_in_atom_expr3().GetRule_lambda1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_in_atom_expr::kAltInAtomExpr4:
            if (auto expected = CastRule(node.GetAlt_in_atom_expr4().GetRule_cast_expr1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_in_atom_expr::kAltInAtomExpr5:
            if (auto expected = CaseRule(node.GetAlt_in_atom_expr5().GetRule_case_expr1())) {
                result.Expr = std::move(*expected);
            } else {
                return std::unexpected(expected.error());
            }
            break;
        case TRule_in_atom_expr::kAltInAtomExpr6: {
            const auto& alt = node.GetAlt_in_atom_expr6();
            TString module(Id(alt.GetRule_an_id_or_type1(), *this));
            TPosition pos(Ctx_.Pos());
            TString name;
            switch (alt.GetBlock3().Alt_case()) {
                case TRule_in_atom_expr::TAlt6::TBlock3::kAlt1:
                    name = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), *this);
                    break;
                case TRule_in_atom_expr::TAlt6::TBlock3::kAlt2: {
                    name = Token(alt.GetBlock3().GetAlt2().GetToken1());
                    if (Ctx_.AnsiQuotedIdentifiers && name.StartsWith('"')) {
                        // same as previous case
                        name = IdContentFromString(Ctx_, name);
                    } else {
                        module = "@" + module;
                    }
                    break;
                }
                case TRule_in_atom_expr::TAlt6::TBlock3::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }
            result.Expr = BuildCallable(pos, module, name, {});
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr7: {
            result.Expr = ValueConstructor(node.GetAlt_in_atom_expr7().GetRule_value_constructor1());
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr8:
            result.Expr = BitCastRule(node.GetAlt_in_atom_expr8().GetRule_bitcast_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr9:
            result.Expr = ListLiteral(node.GetAlt_in_atom_expr9().GetRule_list_literal1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr10:
            result.Expr = DictLiteral(node.GetAlt_in_atom_expr10().GetRule_dict_literal1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr11:
            result.Expr = StructLiteral(node.GetAlt_in_atom_expr11().GetRule_struct_literal1());
            break;
        case TRule_in_atom_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    if (!result.Expr) {
        return std::unexpected(ESQLError::Basic);
    }
    return result;
}

bool TSqlExpression::SqlLambdaParams(const TNodePtr& node, TVector<TSymbolNameWithPos>& args, ui32& optionalArgumentsCount) {
    args.clear();
    optionalArgumentsCount = 0;
    auto errMsg = TStringBuf("Invalid lambda arguments syntax. Lambda arguments should start with '$' as named value.");
    auto tupleNodePtr = node->GetTupleNode();

    if (!tupleNodePtr) {
        Ctx_.Error(node->GetPos()) << errMsg;
        return false;
    }
    THashSet<TString> dupArgsChecker;
    for (const auto& argPtr : tupleNodePtr->Elements()) {
        auto contentPtr = argPtr->GetAtomContent();
        if (!contentPtr || !contentPtr->StartsWith("$")) {
            Ctx_.Error(argPtr->GetPos()) << errMsg;
            return false;
        }
        if (argPtr->IsOptionalArg()) {
            ++optionalArgumentsCount;
        } else if (optionalArgumentsCount > 0) {
            Ctx_.Error(argPtr->GetPos()) << "Non-optional argument can not follow optional one";
            return false;
        }

        if (!IsAnonymousName(*contentPtr) && !dupArgsChecker.insert(*contentPtr).second) {
            Ctx_.Error(argPtr->GetPos()) << "Duplicate lambda argument parametr: '" << *contentPtr << "'.";
            return false;
        }
        args.push_back(TSymbolNameWithPos{*contentPtr, argPtr->GetPos()});
    }
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_expr& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    expr.SetPure(IsPure_);
    TNodePtr nodeExpr = Unwrap(expr.Build(node));
    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    expr.SetPure(true);
    TVector<TString> localNames;
    bool hasError = false;
    for (auto& block : node.GetBlock2()) {
        const auto& rule = block.GetRule_lambda_stmt1();
        switch (rule.Alt_case()) {
            case TRule_lambda_stmt::kAltLambdaStmt1: {
                TVector<TSymbolNameWithPos> names;
                auto nodeExpr = expr.NamedNode(rule.GetAlt_lambda_stmt1().GetRule_named_nodes_stmt1(), names);
                if (!nodeExpr) {
                    hasError = true;
                    continue;
                } else if (nodeExpr->GetSource()) {
                    ctx.Error() << "SELECT is not supported inside lambda body";
                    hasError = true;
                    continue;
                }
                if (names.size() > 1) {
                    auto ref = ctx.MakeName("tie");
                    exprSeq.push_back(nodeExpr->Y("EnsureTupleSize", nodeExpr, nodeExpr->Q(ToString(names.size()))));
                    exprSeq.back()->SetLabel(ref);
                    for (size_t i = 0; i < names.size(); ++i) {
                        TNodePtr nthExpr = nodeExpr->Y("Nth", ref, nodeExpr->Q(ToString(i)));
                        names[i].Name = PushNamedAtom(names[i].Pos, names[i].Name);
                        nthExpr->SetLabel(names[i].Name);
                        localNames.push_back(names[i].Name);
                        exprSeq.push_back(nthExpr);
                    }
                } else {
                    auto& symbol = names.front();
                    symbol.Name = PushNamedAtom(symbol.Pos, symbol.Name);
                    nodeExpr->SetLabel(symbol.Name);
                    localNames.push_back(symbol.Name);
                    exprSeq.push_back(nodeExpr);
                }
                break;
            }
            case TRule_lambda_stmt::kAltLambdaStmt2: {
                if (!ImportStatement(rule.GetAlt_lambda_stmt2().GetRule_import_stmt1(), &localNames)) {
                    hasError = true;
                }
                break;
            }
            case TRule_lambda_stmt::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TNodePtr nodeExpr;
    if (!hasError) {
        nodeExpr = Unwrap(expr.Build(node.GetRule_expr4()));
    }

    for (const auto& name : localNames) {
        if (!PopNamedNode(name)) {
            return false;
        }
    }

    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

TNodeResult TSqlExpression::SubExpr(const TRule_con_subexpr& node, const TTrailingQuestions& tail) {
    // con_subexpr: unary_subexpr | unary_op unary_subexpr;
    switch (node.Alt_case()) {
        case TRule_con_subexpr::kAltConSubexpr1:
            return UnaryExpr(node.GetAlt_con_subexpr1().GetRule_unary_subexpr1(), tail);
        case TRule_con_subexpr::kAltConSubexpr2: {
            MaybeUnnamedSmartParenOnTop_ = false;
            Ctx_.IncrementMonCounter("sql_features", "UnaryOperation");
            TString opName;
            auto token = node.GetAlt_con_subexpr2().GetRule_unary_op1().GetToken1();
            Token(token);
            TPosition pos(Ctx_.Pos());
            auto tokenId = token.GetId();
            if (IS_TOKEN(tokenId, NOT)) {
                opName = "Not";
            } else if (IS_TOKEN(tokenId, PLUS)) {
                opName = "Plus";
            } else if (IS_TOKEN(tokenId, MINUS)) {
                opName = Ctx_.Scoped->PragmaCheckedOps ? "CheckedMinus" : "Minus";
            } else if (IS_TOKEN(tokenId, TILDA)) {
                opName = "BitNot";
            } else {
                Ctx_.IncrementMonCounter("sql_errors", "UnsupportedUnaryOperation");
                Error() << "Unsupported unary operation: " << token.GetValue();
                return std::unexpected(ESQLError::Basic);
            }
            Ctx_.IncrementMonCounter("sql_unary_operations", opName);
            if (auto result = UnaryExpr(node.GetAlt_con_subexpr2().GetRule_unary_subexpr2(), tail)) {
                return Wrap((*result)->ApplyUnaryOp(Ctx_, pos, opName));
            } else {
                return std::unexpected(result.error());
            }
        }
        case TRule_con_subexpr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return std::unexpected(ESQLError::Basic);
}

TNodeResult TSqlExpression::SubExpr(const TRule_xor_subexpr& node, const TTrailingQuestions& tail) {
    // xor_subexpr: eq_subexpr cond_expr?;
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && !node.HasBlock2();
    TNodeResult res(SubExpr(node.GetRule_eq_subexpr1(), node.HasBlock2() ? TTrailingQuestions{} : tail));
    if (!res) {
        return std::unexpected(res.error());
    }
    TPosition pos(Ctx_.Pos());
    if (node.HasBlock2()) {
        auto cond = node.GetBlock2().GetRule_cond_expr1();
        switch (cond.Alt_case()) {
            case TRule_cond_expr::kAltCondExpr1: {
                const auto& matchOp = cond.GetAlt_cond_expr1();
                const bool notMatch = matchOp.HasBlock1();
                const TCiString& opName = Token(matchOp.GetRule_match_op2().GetToken1());
                TNodeResult pattern = SubExpr(cond.GetAlt_cond_expr1().GetRule_eq_subexpr3(), matchOp.HasBlock4() ? TTrailingQuestions{} : tail);
                if (!pattern) {
                    return std::unexpected(pattern.error());
                }
                TNodePtr isMatch;
                if (opName == "like" || opName == "ilike") {
                    const TString* escapeLiteral = nullptr;
                    TNodePtr escapeNode;
                    const auto& escaper = BuildUdf(Ctx_, pos, "Re2", "PatternFromLike", {});
                    TVector<TNodePtr> escaperArgs({escaper, *pattern});

                    if (matchOp.HasBlock4()) {
                        const auto& escapeBlock = matchOp.GetBlock4();
                        TNodeResult escapeExpr = SubExpr(escapeBlock.GetRule_eq_subexpr2(), tail);
                        if (!escapeExpr) {
                            return std::unexpected(escapeExpr.error());
                        }
                        escapeLiteral = (*escapeExpr)->GetLiteral("String");
                        escapeNode = (*escapeExpr);
                        if (escapeLiteral) {
                            Ctx_.IncrementMonCounter("sql_features", "LikeEscape");
                            if (escapeLiteral->size() != 1) {
                                Ctx_.IncrementMonCounter("sql_errors", "LikeMultiCharEscape");
                                Error() << "ESCAPE clause requires single character argument";
                                return std::unexpected(ESQLError::Basic);
                            }
                            if (escapeLiteral[0] == "%" || escapeLiteral[0] == "_" || escapeLiteral[0] == "\\") {
                                Ctx_.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "'%', '_' and '\\' are currently not supported in ESCAPE clause, ";
                                Error() << "please choose any other character";
                                return std::unexpected(ESQLError::Basic);
                            }
                            if (!IsAscii(escapeLiteral->front())) {
                                Ctx_.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "Non-ASCII symbols are not supported in ESCAPE clause, ";
                                Error() << "please choose ASCII character";
                                return std::unexpected(ESQLError::Basic);
                            }
                            escaperArgs.push_back(BuildLiteralRawString(pos, *escapeLiteral));
                        } else {
                            Ctx_.IncrementMonCounter("sql_errors", "LikeNotLiteralEscape");
                            Error() << "ESCAPE clause requires String literal argument";
                            return std::unexpected(ESQLError::Basic);
                        }
                    }

                    auto re2options = BuildUdf(Ctx_, pos, "Re2", "Options", {});
                    if (opName == "ilike") {
                        Ctx_.IncrementMonCounter("sql_features", "CaseInsensitiveLike");
                    }
                    auto csModeLiteral = BuildLiteralBool(pos, opName != "ilike");
                    csModeLiteral->SetLabel("CaseSensitive");
                    auto csOption = BuildStructure(pos, {csModeLiteral});
                    auto optionsApply = new TCallNodeImpl(pos, "NamedApply", {re2options, BuildTuple(pos, {}), csOption});

                    const TNodePtr escapedPattern = new TCallNodeImpl(pos, "Apply", {escaperArgs});
                    auto list = new TAstListNodeImpl(pos, {escapedPattern, optionsApply});
                    auto runConfig = new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "quote", 0), list});

                    const TNodePtr matcher = new TCallNodeImpl(pos, "AssumeStrict", {BuildUdf(Ctx_, pos, "Re2", "Match", {runConfig})});
                    isMatch = new TCallNodeImpl(pos, "Apply", {matcher, *res});

                    bool isUtf8 = false;
                    const TString* literalPattern = (*pattern)->GetLiteral("String");
                    if (!literalPattern) {
                        literalPattern = (*pattern)->GetLiteral("Utf8");
                        isUtf8 = literalPattern != nullptr;
                    }

                    if (literalPattern) {
                        bool inEscape = false;
                        TMaybe<char> escape;
                        if (escapeLiteral) {
                            escape = escapeLiteral->front();
                        }

                        bool mayIgnoreCase;
                        TVector<TPatternComponent<char>> components;
                        if (isUtf8) {
                            auto splitResult = SplitPattern(UTF8ToUTF32<false>(*literalPattern), escape, inEscape);
                            for (const auto& component : splitResult) {
                                TPatternComponent<char> converted;
                                converted.IsSimple = component.IsSimple;
                                converted.Prefix = WideToUTF8(component.Prefix);
                                converted.Suffix = WideToUTF8(component.Suffix);
                                components.push_back(std::move(converted));
                            }
                            mayIgnoreCase = ToLowerUTF8(*literalPattern) == ToUpperUTF8(*literalPattern);
                        } else {
                            components = SplitPattern(*literalPattern, escape, inEscape);
                            mayIgnoreCase = WithoutAlpha(*literalPattern);
                        }

                        if (inEscape) {
                            Ctx_.IncrementMonCounter("sql_errors", "LikeEscapeSymbolEnd");
                            Error() << "LIKE pattern should not end with escape symbol";
                            return std::unexpected(ESQLError::Basic);
                        }

                        if ((opName == "like") || mayIgnoreCase || Ctx_.OptimizeSimpleIlike) {
                            // TODO: expand LIKE in optimizers - we can analyze argument types there
                            const bool useIgnoreCaseOp = (opName == "ilike") && !mayIgnoreCase;
                            const auto& equalOp = useIgnoreCaseOp ? "EqualsIgnoreCase" : "==";
                            const auto& startsWithOp = useIgnoreCaseOp ? "StartsWithIgnoreCase" : "StartsWith";
                            const auto& endsWithOp = useIgnoreCaseOp ? "EndsWithIgnoreCase" : "EndsWith";
                            const auto& containsOp = useIgnoreCaseOp ? "StringContainsIgnoreCase" : "StringContains";

                            YQL_ENSURE(!components.empty());
                            const auto& first = components.front();
                            if (components.size() == 1 && first.IsSimple) {
                                // no '%'s and '_'s  in pattern
                                YQL_ENSURE(first.Prefix == first.Suffix);
                                isMatch = BuildBinaryOp(Ctx_, pos, equalOp, *res, BuildLiteralRawString(pos, first.Suffix, isUtf8));
                            } else if (!first.Prefix.empty()) {
                                const TString& prefix = first.Prefix;
                                TNodePtr prefixMatch;
                                if (Ctx_.EmitStartsWith) {
                                    prefixMatch = BuildBinaryOp(Ctx_, pos, startsWithOp, *res, BuildLiteralRawString(pos, prefix, isUtf8));
                                } else {
                                    prefixMatch = BuildBinaryOp(Ctx_, pos, ">=", *res, BuildLiteralRawString(pos, prefix, isUtf8));
                                    auto upperBound = isUtf8 ? NextValidUtf8(prefix) : NextLexicographicString(prefix);
                                    if (upperBound) {
                                        prefixMatch = BuildBinaryOp(
                                            Ctx_,
                                            pos,
                                            "And",
                                            prefixMatch,
                                            BuildBinaryOp(Ctx_, pos, "<", *res, BuildLiteralRawString(pos, TString(*upperBound), isUtf8)));
                                    }
                                }

                                if (Ctx_.AnsiLike && first.IsSimple && components.size() == 2 && components.back().IsSimple) {
                                    const TString& suffix = components.back().Suffix;
                                    // 'prefix%suffix'
                                    if (suffix.empty()) {
                                        isMatch = prefixMatch;
                                    } else {
                                        // len(str) >= len(prefix) + len(suffix) && StartsWith(str, prefix) && EndsWith(str, suffix)
                                        TNodePtr sizePred = BuildBinaryOp(Ctx_, pos, ">=",
                                                                          TNodePtr(new TCallNodeImpl(pos, "Size", {*res})),
                                                                          TNodePtr(new TLiteralNumberNode<ui32>(pos, "Uint32", ToString(prefix.size() + suffix.size()))));
                                        TNodePtr suffixMatch = BuildBinaryOp(Ctx_, pos, endsWithOp, *res, BuildLiteralRawString(pos, suffix, isUtf8));
                                        isMatch = new TCallNodeImpl(pos, "And", {sizePred,
                                                                                 prefixMatch,
                                                                                 suffixMatch});
                                    }
                                } else {
                                    isMatch = BuildBinaryOp(Ctx_, pos, "And", prefixMatch, isMatch);
                                }
                            } else if (Ctx_.AnsiLike && AllOf(components, [](const auto& comp) { return comp.IsSimple; })) {
                                YQL_ENSURE(first.Prefix.empty());
                                if (components.size() == 3 && components.back().Prefix.empty()) {
                                    // '%foo%'
                                    YQL_ENSURE(!components[1].Prefix.empty());
                                    isMatch = BuildBinaryOp(Ctx_, pos, containsOp, *res, BuildLiteralRawString(pos, components[1].Prefix, isUtf8));
                                } else if (components.size() == 2) {
                                    // '%foo'
                                    isMatch = BuildBinaryOp(Ctx_, pos, endsWithOp, *res, BuildLiteralRawString(pos, components[1].Prefix, isUtf8));
                                }
                            } else if (Ctx_.AnsiLike && !components.back().Suffix.empty()) {
                                const TString& suffix = components.back().Suffix;
                                TNodePtr suffixMatch = BuildBinaryOp(Ctx_, pos, endsWithOp, *res, BuildLiteralRawString(pos, suffix, isUtf8));
                                isMatch = BuildBinaryOp(Ctx_, pos, "And", suffixMatch, isMatch);
                            }
                            // TODO: more StringContains/StartsWith/EndsWith cases?
                        }
                    }

                    Ctx_.IncrementMonCounter("sql_features", notMatch ? "NotLike" : "Like");

                } else if (opName == "regexp" || opName == "rlike" || opName == "match") {
                    if (matchOp.HasBlock4()) {
                        Ctx_.IncrementMonCounter("sql_errors", "RegexpEscape");
                        TString opNameUpper(opName);
                        opNameUpper.to_upper();
                        Error() << opName << " and ESCAPE clauses should not be used together";
                        return std::unexpected(ESQLError::Basic);
                    }

                    if (!Ctx_.PragmaRegexUseRe2) {
                        if (!Ctx_.Warning(pos, TIssuesIds::CORE_LEGACY_REGEX_ENGINE, [](auto& out) {
                                out << "Legacy regex engine works incorrectly with unicode. "
                                    << "Use PRAGMA RegexUseRe2='true';";
                            })) {
                            return std::unexpected(ESQLError::Basic);
                        }
                    }

                    const auto& matcher = Ctx_.PragmaRegexUseRe2
                                              ? BuildUdf(Ctx_, pos, "Re2", opName == "match" ? "Match" : "Grep", {BuildTuple(pos, {*pattern, BuildLiteralNull(pos)})})
                                              : BuildUdf(Ctx_, pos, "Pcre", opName == "match" ? "BacktrackingMatch" : "BacktrackingGrep", {*pattern});
                    isMatch = new TCallNodeImpl(pos, "Apply", {matcher, *res});
                    if (opName != "match") {
                        Ctx_.IncrementMonCounter("sql_features", notMatch ? "NotRegexp" : "Regexp");
                    } else {
                        Ctx_.IncrementMonCounter("sql_features", notMatch ? "NotMatch" : "Match");
                    }
                } else {
                    Ctx_.IncrementMonCounter("sql_errors", "UnknownMatchOp");
                    AltNotImplemented("match_op", cond);
                    return std::unexpected(ESQLError::Basic);
                }
                return Wrap((notMatch && isMatch) ? isMatch->ApplyUnaryOp(Ctx_, pos, "Not") : isMatch);
            }
            case TRule_cond_expr::kAltCondExpr2: {
                // | NOT? IN COMPACT? in_expr
                auto altInExpr = cond.GetAlt_cond_expr2();

                if (IsYqlSelectProduced_) {
                    return YqlXorSubExpr(std::move(*res), altInExpr, tail);
                }

                const bool notIn = altInExpr.HasBlock1();
                auto hints = BuildTuple(pos, {});
                bool isCompact = altInExpr.HasBlock3();
                if (!isCompact) {
                    auto sqlHints = Ctx_.PullHintForToken(Ctx_.TokenPosition(altInExpr.GetToken2()));
                    isCompact = AnyOf(sqlHints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
                }
                if (isCompact) {
                    Ctx_.IncrementMonCounter("sql_features", "IsCompactHint");
                    auto sizeHint = BuildTuple(pos, {BuildQuotedAtom(pos, "isCompact", NYql::TNodeFlags::Default)});
                    hints = BuildTuple(pos, {sizeHint});
                }
                TSqlExpression inSubexpr(Ctx_, Mode_);
                inSubexpr.SetPure(IsPure_);
                auto inRight = inSubexpr.SqlInExpr(altInExpr.GetRule_in_expr4(), tail);
                auto isIn = Unwrap(BuildBuiltinFunc(Ctx_, pos, "In", {*res, inRight, hints}, /*isYqlSelect=*/false));
                Ctx_.IncrementMonCounter("sql_features", notIn ? "NotIn" : "In");
                return Wrap((notIn && isIn) ? isIn->ApplyUnaryOp(Ctx_, pos, "Not") : isIn);
            }
            case TRule_cond_expr::kAltCondExpr3: {
                if (tail.Count) {
                    UnexpectedQuestionToken(tail);
                    return std::unexpected(ESQLError::Basic);
                }
                auto altCase = cond.GetAlt_cond_expr3().GetBlock1().Alt_case();
                const bool notNoll =
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt2 ||
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4;

                if (altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4 &&
                    !cond.GetAlt_cond_expr3().GetBlock1().GetAlt4().HasBlock1())
                {
                    if (!Ctx_.Warning(Ctx_.Pos(), TIssuesIds::YQL_MISSING_IS_BEFORE_NOT_NULL, [](auto& out) {
                            out << "Missing IS keyword before NOT NULL";
                        }, Ctx_.DisableLegacyNotNull)) {
                        return std::unexpected(ESQLError::Basic);
                    }
                }

                auto isNull = BuildIsNullOp(pos, *res);
                Ctx_.IncrementMonCounter("sql_features", notNoll ? "NotNull" : "Null");
                return Wrap((notNoll && isNull) ? isNull->ApplyUnaryOp(Ctx_, pos, "Not") : isNull);
            }
            case TRule_cond_expr::kAltCondExpr4: {
                auto alt = cond.GetAlt_cond_expr4();
                const bool symmetric = alt.HasBlock3() && IS_TOKEN(alt.GetBlock3().GetToken1().GetId(), SYMMETRIC);
                const bool negation = alt.HasBlock1();

                TNodeResult left = SubExpr(alt.GetRule_eq_subexpr4(), {});
                if (!left && left.error() != ESQLError::Basic) {
                    return std::unexpected(left.error());
                }

                TNodeResult right = SubExpr(alt.GetRule_eq_subexpr6(), tail);
                if (!right && right.error() != ESQLError::Basic) {
                    return std::unexpected(right.error());
                }

                if (!left || !right) {
                    return std::unexpected(ESQLError::Basic);
                }

                const bool bothArgNull = (*left)->IsNull() && (*right)->IsNull();
                const bool oneArgNull = (*left)->IsNull() || (*right)->IsNull();

                if ((*res)->IsNull() || bothArgNull || (symmetric && oneArgNull)) {
                    if (!Ctx_.Warning(pos, TIssuesIds::YQL_OPERATION_WILL_RETURN_NULL, [](auto& out) {
                            out << "BETWEEN operation will return NULL here";
                        })) {
                        return std::unexpected(ESQLError::Basic);
                    }
                }

                auto buildSubexpr = [&](const TNodePtr& left, const TNodePtr& right) {
                    if (negation) {
                        return BuildBinaryOpRaw(
                            pos,
                            "Or",
                            BuildBinaryOpRaw(pos, "<", *res, left),
                            BuildBinaryOpRaw(pos, ">", *res, right));
                    } else {
                        return BuildBinaryOpRaw(
                            pos,
                            "And",
                            BuildBinaryOpRaw(pos, ">=", *res, left),
                            BuildBinaryOpRaw(pos, "<=", *res, right));
                    }
                };

                if (symmetric) {
                    Ctx_.IncrementMonCounter("sql_features", negation ? "NotBetweenSymmetric" : "BetweenSymmetric");
                    return Wrap(BuildBinaryOpRaw(
                        pos,
                        negation ? "And" : "Or",
                        buildSubexpr(*left, *right),
                        buildSubexpr(*right, *left)));
                } else {
                    Ctx_.IncrementMonCounter("sql_features", negation ? "NotBetween" : "Between");
                    return Wrap(buildSubexpr(*left, *right));
                }
            }
            case TRule_cond_expr::kAltCondExpr5: {
                auto alt = cond.GetAlt_cond_expr5();
                auto getNode = [](const TRule_cond_expr::TAlt5::TBlock1& b) -> const TRule_eq_subexpr& { return b.GetRule_eq_subexpr2(); };
                return BinOpList(node.GetRule_eq_subexpr1(), getNode, alt.GetBlock1().begin(), alt.GetBlock1().end(), tail);
            }
            case TRule_cond_expr::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }
    return res;
}

TNodeResult TSqlExpression::YqlXorSubExpr(
    TNodePtr lhs,
    const TRule_cond_expr::TAlt2& alt,
    const TTrailingQuestions& tail)
{
    YQL_ENSURE(IsYqlSelectProduced_, "YqlSelect expected");

    TMaybe<TPosition> negation;
    if (alt.HasBlock1()) {
        Token(alt.GetBlock1().GetToken1());
        negation = Ctx_.Pos();
    }

    Token(alt.GetToken2());

    if (alt.HasBlock3()) {
        Token(alt.GetBlock3().GetToken1());
        return UnsupportedYqlSelect(Ctx_, "COMPACT");
    }

    TNodeResult rhs = [&]() {
        TSqlExpression expr(Ctx_, Mode_);
        expr.SetPure(IsPure_);
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::InStatement);
        expr.SetYqlSelectProduced(true);

        return expr.UnaryExpr(alt.GetRule_in_expr4().GetRule_in_unary_subexpr1(), tail);
    }();

    if (!rhs) {
        return std::unexpected(rhs.error());
    }

    TNodePtr expr;
    if (auto source = GetYqlSource(*rhs)) {
        expr = BuildYqlInSubquery(std::move(source), std::move(lhs));
    } else {
        TNodePtr hints = BuildTuple(Ctx_.Pos(), {});
        TVector<TNodePtr> args = {
            std::move(lhs),
            std::move(*rhs),
            std::move(hints),
        };

        TNodeResult result = BuildBuiltinFunc(Ctx_, Ctx_.Pos(), "In", std::move(args), /*isYqlSelect=*/true);
        if (!result) {
            return std::unexpected(result.error());
        }

        expr = std::move(*result);
    }

    if (negation) {
        expr = expr->ApplyUnaryOp(Ctx_, *negation, "Not");
    }
    return Wrap(expr);
}

TNodePtr TSqlExpression::BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const {
    TPosition pos(Ctx_.Pos());
    const size_t opCount = end - begin;
    Y_DEBUG_ABORT_UNLESS(opCount >= 2);
    if (opCount == 2) {
        return BuildBinaryOp(Ctx_, pos, opName, *begin, *(begin + 1));
    }
    if (opCount == 3) {
        return BuildBinaryOp(Ctx_, pos, opName, BuildBinaryOp(Ctx_, pos, opName, *begin, *(begin + 1)), *(begin + 2));
    } else {
        auto mid = begin + opCount / 2;
        return BuildBinaryOp(Ctx_, pos, opName, BinOperList(opName, begin, mid), BinOperList(opName, mid, end));
    }
}

TSQLResult<TSqlExpression::TCaseBranch> TSqlExpression::ReduceCaseBranches(TVector<TCaseBranch>::const_iterator begin, TVector<TCaseBranch>::const_iterator end) const {
    YQL_ENSURE(begin < end);
    const size_t branchCount = end - begin;
    if (branchCount == 1) {
        return *begin;
    }

    auto mid = begin + branchCount / 2;
    auto left = ReduceCaseBranches(begin, mid);
    auto right = ReduceCaseBranches(mid, end);

    TVector<TNodePtr> preds;
    preds.reserve(branchCount);
    for (auto it = begin; it != end; ++it) {
        preds.push_back(it->Pred);
    }

    if (!left) {
        return std::unexpected(left.error());
    }
    if (!right) {
        return std::unexpected(right.error());
    }

    TNodePtr pred = new TCallNodeImpl(Ctx_.Pos(), "Or", CloneContainer(std::move(preds)));

    TNodeResult value = BuildBuiltinFunc(Ctx_, Ctx_.Pos(), "If", {left->Pred, left->Value, right->Value},
                                         /*isYqlSelect=*/IsYqlSelectProduced_);
    if (!value) {
        return std::unexpected(value.error());
    }

    return TCaseBranch{
        .Pred = std::move(pred),
        .Value = std::move(*value),
    };
}

template <typename TNode, typename TGetNode, typename TIter>
TNodeResult TSqlExpression::BinOper(const TString& opName, const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    if (begin == end) {
        return SubExpr(node, tail);
    }

    IsSourceAllowed_ = false;

    // can't have top level smart_parenthesis node if any binary operation is present
    MaybeUnnamedSmartParenOnTop_ = false;
    Ctx_.IncrementMonCounter("sql_binary_operations", opName);
    const size_t listSize = end - begin;
    TVector<TNodePtr> nodes;
    nodes.reserve(1 + listSize);

    if (auto result = SubExpr(node, {});
        result || result.error() == ESQLError::Basic) {
        nodes.push_back(Unwrap(std::move(result)));
    } else {
        return std::unexpected(result.error());
    }

    for (; begin != end; ++begin) {
        if (auto result = SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{});
            result || result.error() == ESQLError::Basic)
        {
            nodes.push_back(Unwrap(std::move(result)));
        } else {
            return std::unexpected(result.error());
        }
    }
    return Wrap(BinOperList(opName, nodes.begin(), nodes.end()));
}

template <typename TNode, typename TGetNode, typename TIter>
TNodeResult TSqlExpression::BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && (begin == end);
    TNodeResult partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        IsSourceAllowed_ = false;

        Ctx_.IncrementMonCounter("sql_features", "BinaryOperation");
        Token(begin->GetToken1());
        TPosition pos(Ctx_.Pos());
        TString opName;
        auto tokenId = begin->GetToken1().GetId();
        if (IS_TOKEN(tokenId, LESS)) {
            opName = "<";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Less");
        } else if (IS_TOKEN(tokenId, LESS_OR_EQ)) {
            opName = "<=";
            Ctx_.IncrementMonCounter("sql_binary_operations", "LessOrEq");
        } else if (IS_TOKEN(tokenId, GREATER)) {
            opName = ">";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Greater");
        } else if (IS_TOKEN(tokenId, GREATER_OR_EQ)) {
            opName = ">=";
            Ctx_.IncrementMonCounter("sql_binary_operations", "GreaterOrEq");
        } else if (IS_TOKEN(tokenId, PLUS)) {
            opName = Ctx_.Scoped->PragmaCheckedOps ? "CheckedAdd" : "+MayWarn";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Plus");
        } else if (IS_TOKEN(tokenId, MINUS)) {
            opName = Ctx_.Scoped->PragmaCheckedOps ? "CheckedSub" : "-MayWarn";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Minus");
        } else if (IS_TOKEN(tokenId, ASTERISK)) {
            opName = Ctx_.Scoped->PragmaCheckedOps ? "CheckedMul" : "*MayWarn";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Multiply");
        } else if (IS_TOKEN(tokenId, SLASH)) {
            opName = "/MayWarn";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Divide");
            if (!Ctx_.Scoped->PragmaClassicDivision && partialResult) {
                partialResult = TNonNull(TNodePtr(new TCallNodeImpl(pos, "SafeCast", {std::move(*partialResult), BuildDataType(pos, "Double")})));
            } else if (Ctx_.Scoped->PragmaCheckedOps) {
                opName = "CheckedDiv";
            }
        } else if (IS_TOKEN(tokenId, PERCENT)) {
            opName = Ctx_.Scoped->PragmaCheckedOps ? "CheckedMod" : "%MayWarn";
            Ctx_.IncrementMonCounter("sql_binary_operations", "Mod");
        } else {
            Ctx_.IncrementMonCounter("sql_errors", "UnsupportedBinaryOperation");
            Error() << "Unsupported binary operation token: " << tokenId;
            return std::unexpected(ESQLError::Basic);
        }

        TNodeResult lhs = std::move(partialResult);
        TNodeResult rhs = SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{});

        if (!lhs && lhs.error() != ESQLError::Basic) {
            return std::unexpected(lhs.error());
        }
        if (!rhs && rhs.error() != ESQLError::Basic) {
            return std::unexpected(rhs.error());
        }

        partialResult = Wrap(BuildBinaryOp(Ctx_, pos, opName, Unwrap(std::move(lhs)), Unwrap(std::move(rhs))));
        ++begin;
    }

    return partialResult;
}

template <typename TGetNode, typename TIter>
TNodeResult TSqlExpression::BinOpList(const TRule_bit_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && (begin == end);
    TNodeResult partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx_.IncrementMonCounter("sql_features", "BinaryOperation");
        TString opName;
        switch (begin->GetBlock1().Alt_case()) {
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt1: {
                Token(begin->GetBlock1().GetAlt1().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt1().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, SHIFT_LEFT)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                opName = "ShiftLeft";
                Ctx_.IncrementMonCounter("sql_binary_operations", "ShiftLeft");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt2: {
                opName = "ShiftRight";
                Ctx_.IncrementMonCounter("sql_binary_operations", "ShiftRight");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt3: {
                Token(begin->GetBlock1().GetAlt3().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt3().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, ROT_LEFT)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                opName = "RotLeft";
                Ctx_.IncrementMonCounter("sql_binary_operations", "RotLeft");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt4: {
                opName = "RotRight";
                Ctx_.IncrementMonCounter("sql_binary_operations", "RotRight");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt5: {
                Token(begin->GetBlock1().GetAlt5().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt5().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, AMPERSAND)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                opName = "BitAnd";
                Ctx_.IncrementMonCounter("sql_binary_operations", "BitAnd");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt6: {
                Token(begin->GetBlock1().GetAlt6().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt6().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, PIPE)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                opName = "BitOr";
                Ctx_.IncrementMonCounter("sql_binary_operations", "BitOr");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt7: {
                Token(begin->GetBlock1().GetAlt7().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt7().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, CARET)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                opName = "BitXor";
                Ctx_.IncrementMonCounter("sql_binary_operations", "BitXor");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::ALT_NOT_SET:
                Y_UNREACHABLE();
        }

        TPosition pos = Ctx_.Pos();

        TNodeResult lhs = std::move(partialResult);
        TNodeResult rhs = SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{});

        if (!lhs && lhs.error() != ESQLError::Basic) {
            return std::unexpected(lhs.error());
        }
        if (!rhs && rhs.error() != ESQLError::Basic) {
            return std::unexpected(rhs.error());
        }

        partialResult = Wrap(BuildBinaryOp(Ctx_, pos, opName, Unwrap(std::move(lhs)), Unwrap(std::move(rhs))));
        ++begin;
    }

    return partialResult;
}

template <typename TGetNode, typename TIter>
TNodeResult TSqlExpression::BinOpList(const TRule_eq_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop_ = MaybeUnnamedSmartParenOnTop_ && (begin == end);
    TNodeResult partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx_.IncrementMonCounter("sql_features", "BinaryOperation");
        TString opName;
        switch (begin->GetBlock1().Alt_case()) {
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt1: {
                Token(begin->GetBlock1().GetAlt1().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt1().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, EQUALS)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                Ctx_.IncrementMonCounter("sql_binary_operations", "Equals");
                opName = "==";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt2: {
                Token(begin->GetBlock1().GetAlt2().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt2().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, EQUALS2)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                Ctx_.IncrementMonCounter("sql_binary_operations", "Equals2");
                opName = "==";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt3: {
                Token(begin->GetBlock1().GetAlt3().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt3().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, NOT_EQUALS)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                Ctx_.IncrementMonCounter("sql_binary_operations", "NotEquals");
                opName = "!=";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt4: {
                Token(begin->GetBlock1().GetAlt4().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt4().GetToken1().GetId();
                if (!IS_TOKEN(tokenId, NOT_EQUALS2)) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return std::unexpected(ESQLError::Basic);
                }
                Ctx_.IncrementMonCounter("sql_binary_operations", "NotEquals2");
                opName = "!=";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt5: {
                Token(begin->GetBlock1().GetAlt5().GetRule_distinct_from_op1().GetToken1());
                opName = begin->GetBlock1().GetAlt5().GetRule_distinct_from_op1().HasBlock2() ? "IsNotDistinctFrom" : "IsDistinctFrom";
                Ctx_.IncrementMonCounter("sql_binary_operations", opName);
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::ALT_NOT_SET:
                Y_UNREACHABLE();
        }

        TPosition pos = Ctx_.Pos();

        TNodeResult lhs = std::move(partialResult);
        TNodeResult rhs = SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{});

        if (!lhs && lhs.error() != ESQLError::Basic) {
            return std::unexpected(lhs.error());
        }
        if (!rhs && rhs.error() != ESQLError::Basic) {
            return std::unexpected(rhs.error());
        }

        partialResult = Wrap(BuildBinaryOp(Ctx_, pos, opName, Unwrap(std::move(lhs)), Unwrap(std::move(rhs))));
        ++begin;
    }

    return partialResult;
}

TNodePtr TSqlExpression::SqlInExpr(const TRule_in_expr& node, const TTrailingQuestions& tail) {
    TSqlExpression expr(Ctx_, Mode_);
    expr.SetPure(IsPure_);
    expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::InStatement);
    TNodePtr result = Unwrap(expr.UnaryExpr(node.GetRule_in_unary_subexpr1(), tail));

    if (TSourcePtr source = MoveOutIfSource(result)) {
        if (IsSubqueryRef(source)) { // Prevent redundant ref to ref
            return source;
        }

        Ctx_.IncrementMonCounter("sql_features", "InSubquery");

        const auto alias = Ctx_.MakeName("subquerynode");
        const auto ref = Ctx_.MakeName("subquery");

        auto& blocks = Ctx_.GetCurrentBlocks();
        blocks.emplace_back(BuildSubquery(
            std::move(source),
            alias,
            /* inSubquery = */ Mode_ == NSQLTranslation::ESqlMode::SUBQUERY,
            /* ensureTupleSize = */ -1,
            Ctx_.Scoped));
        blocks.back()->SetLabel(ref);

        return BuildSubqueryRef(blocks.back(), ref, /* tupleIndex = */ -1);
    }

    return result;
}

bool TSqlExpression::IsTopLevelGroupBy() const {
    return MaybeUnnamedSmartParenOnTop_ &&
           SmartParenthesisMode_ == ESmartParenthesis::GroupBy;
}

TNodePtr TSqlExpression::ToSubSelectNode(TSourcePtr source) {
    if (IsSubqueryRef(source)) {
        return TNonNull(TNodePtr(std::move(source)));
    }

    source->UseAsInner();
    return BuildSourceNode(
        source->GetPos(),
        source,
        /*checkExist=*/false,
        /*withTables=*/false,
        /*isInlineScalar=*/true,
        /*isPure=*/IsPure_);
}

TNodeResult TSqlExpression::SelectSubExpr(const TRule_select_subexpr& node) {
    if (IsYqlSelectProduced_) {
        return BuildYqlSelectSubExpr(Ctx_, Mode_, node, Ctx_.GetColumnReferenceState());
    }

    TNodeResult result = std::unexpected(ESQLError::Basic);
    if (IsOnlySubExpr(node)) {
        result = SelectOrExpr(node.GetRule_select_subexpr_intersect1()
                                  .GetRule_select_or_expr1());
    } else {
        result = Wrap(TSqlSelect(Ctx_, Mode_).BuildSubSelect(node));
    }

    if (!result) {
        return std::unexpected(result.error());
    }

    if (TSourcePtr source = MoveOutIfSource(*result)) {
        if (IsSourceAllowed_) {
            return TNonNull(TNodePtr(std::move(source)));
        }

        result = Wrap(ToSubSelectNode(std::move(source)));
    }

    return result;
}

TNodeResult TSqlExpression::SelectOrExpr(const TRule_select_or_expr& node) {
    switch (node.Alt_case()) {
        case NSQLv1Generated::TRule_select_or_expr::kAltSelectOrExpr1: {
            const auto& select_kind = node.GetAlt_select_or_expr1().GetRule_select_kind_partial1();
            TSourcePtr source = TSqlSelect(Ctx_, Mode_).BuildSubSelect(select_kind);
            return Wrap(std::move(source));
        }
        case NSQLv1Generated::TRule_select_or_expr::kAltSelectOrExpr2:
            return TupleOrExpr(node.GetAlt_select_or_expr2().GetRule_tuple_or_expr1());
        case NSQLv1Generated::TRule_select_or_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodeResult TSqlExpression::TupleOrExpr(const TRule_tuple_or_expr& node) {
    TVector<TNodePtr> exprs;
    const TPosition pos(Ctx_.Pos());

    const bool isTuple = node.HasBlock4();

    bool expectTuple = SmartParenthesisMode_ == ESmartParenthesis::InStatement;
    EExpr mode = EExpr::Regular;
    if (SmartParenthesisMode_ == ESmartParenthesis::SqlLambdaParams) {
        mode = EExpr::SqlLambdaParams;
        expectTuple = true;
    }

    {
        const auto& head = node.GetRule_expr1();
        const auto* headName = node.HasBlock2() ? &node.GetBlock2().GetRule_an_id_or_type2() : nullptr;

        bool isDefinitelyTuple = isTuple || expectTuple || !node.GetBlock3().empty();
        if ((!headName && !isDefinitelyTuple) || IsSelect(head)) {
            return BuildSourceOrNode(head);
        }

        if (auto result = NamedExpr(head, headName, mode)) {
            exprs.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& item : node.GetBlock3()) {
            if (auto result = NamedExpr(item.GetRule_named_expr2(), mode)) {
                exprs.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }
    }

    bool hasAliases = false;
    bool hasUnnamed = false;
    for (const auto& expr : exprs) {
        if (expr->GetLabel()) {
            hasAliases = true;
        } else {
            hasUnnamed = true;
        }
        if (hasAliases && hasUnnamed && !IsTopLevelGroupBy()) {
            Ctx_.IncrementMonCounter("sql_errors", "AnonymousStructMembers");
            Ctx_.Error(pos) << "Structure does not allow anonymous members";
            return std::unexpected(ESQLError::Basic);
        }
    }
    if (IsTopLevelGroupBy()) {
        if (isTuple) {
            Ctx_.IncrementMonCounter("sql_errors", "SimpleTupleInGroupBy");
            Token(node.GetBlock4().GetToken1());
            Ctx_.Error() << "Unexpected trailing comma in grouping elements list";
            return std::unexpected(ESQLError::Basic);
        }
        Ctx_.IncrementMonCounter("sql_features", "ListOfNamedNode");
        return Wrap(BuildListOfNamedNodes(pos, std::move(exprs)));
    }
    Ctx_.IncrementMonCounter("sql_features", hasUnnamed ? "SimpleTuple" : "SimpleStruct");
    return (hasUnnamed || expectTuple || exprs.size() == 0)
               ? Wrap(BuildTuple(pos, exprs))
               : Wrap(BuildStructure(pos, exprs));
}

TNodePtr TSqlExpression::EmptyTuple() {
    if (IsTopLevelGroupBy()) {
        return BuildListOfNamedNodes(Ctx_.Pos(), TVector<TNodePtr>{});
    }

    return BuildTuple(Ctx_.Pos(), TVector<TNodePtr>{});
}

TNodeResult TSqlExpression::SmartParenthesis(const TRule_smart_parenthesis& node) {
    Token(node.GetToken1());
    switch (node.GetBlock2().GetAltCase()) {
        case NSQLv1Generated::TRule_smart_parenthesis_TBlock2::kAlt1:
            return SelectSubExpr(node.GetBlock2().GetAlt1().GetRule_select_subexpr1());
        case NSQLv1Generated::TRule_smart_parenthesis_TBlock2::kAlt2:
            return TNonNull(EmptyTuple());
        case NSQLv1Generated::TRule_smart_parenthesis_TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodePtr TSqlExpression::GetNamedNode(const TString& name) {
    TNodePtr node = TTranslation::GetNamedNode(name);
    if (!node) {
        return nullptr;
    }

    return WrapYqlSelectSubExpr(std::move(node));
}

} // namespace NSQLTranslationV1
