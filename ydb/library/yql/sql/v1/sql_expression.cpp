#include "sql_expression.h"
#include "sql_call_expr.h"
#include "sql_select.h"
#include "sql_values.h"
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/utils/utf8.h>
#include <util/charset/wide.h>
#include <util/string/ascii.h>
#include <util/string/hex.h>

namespace NSQLTranslationV1 {

using NALPDefault::SQLv1LexerTokens;

using namespace NSQLv1Generated;

TNodePtr TSqlExpression::Build(const TRule_expr& node) {
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
                return TypeNode(node.GetAlt_expr2().GetRule_type_name_composite1());
            }
            case TRule_expr::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }
    }

TNodePtr TSqlExpression::SubExpr(const TRule_mul_subexpr& node, const TTrailingQuestions& tail) {
        // mul_subexpr: con_subexpr (DOUBLE_PIPE con_subexpr)*;
        auto getNode = [](const TRule_mul_subexpr::TBlock2& b) -> const TRule_con_subexpr& { return b.GetRule_con_subexpr2(); };
        return BinOper("Concat", node.GetRule_con_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodePtr TSqlExpression::SubExpr(const TRule_add_subexpr& node, const TTrailingQuestions& tail) {
        // add_subexpr: mul_subexpr ((ASTERISK | SLASH | PERCENT) mul_subexpr)*;
        auto getNode = [](const TRule_add_subexpr::TBlock2& b) -> const TRule_mul_subexpr& { return b.GetRule_mul_subexpr2(); };
        return BinOpList(node.GetRule_mul_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodePtr TSqlExpression::SubExpr(const TRule_bit_subexpr& node, const TTrailingQuestions& tail) {
        // bit_subexpr: add_subexpr ((PLUS | MINUS) add_subexpr)*;
        auto getNode = [](const TRule_bit_subexpr::TBlock2& b) -> const TRule_add_subexpr& { return b.GetRule_add_subexpr2(); };
        return BinOpList(node.GetRule_add_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

TNodePtr TSqlExpression::SubExpr(const TRule_neq_subexpr& node, const TTrailingQuestions& tailExternal) {
        //neq_subexpr: bit_subexpr ((SHIFT_LEFT | shift_right | ROT_LEFT | rot_right | AMPERSAND | PIPE | CARET) bit_subexpr)*
        //  // trailing QUESTIONS are used in optional simple types (String?) and optional lambda args: ($x, $y?) -> ($x)
        //  ((double_question neq_subexpr) => double_question neq_subexpr | QUESTION+)?;
        YQL_ENSURE(tailExternal.Count == 0);
        MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && !node.HasBlock3();
        TTrailingQuestions tail;
        if (node.HasBlock3() && node.GetBlock3().Alt_case() == TRule_neq_subexpr::TBlock3::kAlt2) {
            auto& questions = node.GetBlock3().GetAlt2();
            tail.Count = questions.GetBlock1().size();
            tail.Pos = Ctx.TokenPosition(questions.GetBlock1().begin()->GetToken1());
            YQL_ENSURE(tail.Count > 0);
        }

        auto getNode = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_bit_subexpr& { return b.GetRule_bit_subexpr2(); };
        auto result = BinOpList(node.GetRule_bit_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
        if (!result) {
            return {};
        }
        if (node.HasBlock3()) {
            auto& block = node.GetBlock3();
            if (block.Alt_case() == TRule_neq_subexpr::TBlock3::kAlt1) {
                TSqlExpression altExpr(Ctx, Mode);
                auto altResult = SubExpr(block.GetAlt1().GetRule_neq_subexpr2(), {});
                if (!altResult) {
                    return {};
                }
                const TVector<TNodePtr> args({result, altResult});
                Token(block.GetAlt1().GetRule_double_question1().GetToken1());
                result = BuildBuiltinFunc(Ctx, Ctx.Pos(), "Coalesce", args);
            }
        }
        return result;
    }

    TNodePtr TSqlExpression::SubExpr(const TRule_eq_subexpr& node, const TTrailingQuestions& tail) {
        // eq_subexpr: neq_subexpr ((LESS | LESS_OR_EQ | GREATER | GREATER_OR_EQ) neq_subexpr)*;
        auto getNode = [](const TRule_eq_subexpr::TBlock2& b) -> const TRule_neq_subexpr& { return b.GetRule_neq_subexpr2(); };
        return BinOpList(node.GetRule_neq_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    TNodePtr TSqlExpression::SubExpr(const TRule_or_subexpr& node, const TTrailingQuestions& tail) {
        // or_subexpr: and_subexpr (AND and_subexpr)*;
        auto getNode = [](const TRule_or_subexpr::TBlock2& b) -> const TRule_and_subexpr& { return b.GetRule_and_subexpr2(); };
        return BinOper("And", node.GetRule_and_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
}

TNodePtr TSqlExpression::SubExpr(const TRule_and_subexpr& node, const TTrailingQuestions& tail) {
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
    auto exprNode = ctx.Build(setting.GetRule_expr1());

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
    } else if (to_lower(id.Name) == "virtual_timestamps") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.VirtualTimestamps = exprNode;
    } else if (to_lower(id.Name) == "resolved_timestamps") {
        if (exprNode->GetOpName() != "Interval") {
            ctx.Context().Error() << "Literal of Interval type is expected for " << id.Name;
            return false;
        }
        settings.ResolvedTimestamps = exprNode;
    } else if (to_lower(id.Name) == "retention_period") {
        if (exprNode->GetOpName() != "Interval") {
            ctx.Context().Error() << "Literal of Interval type is expected for " << id.Name;
            return false;
        }
        settings.RetentionPeriod = exprNode;
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
    bool WithoutAlpha(const std::string_view &literal) {
        return literal.cend() == std::find_if(literal.cbegin(), literal.cend(), [](char c) { return std::isalpha(c) || (c & '\x80'); });
    }
}


bool Expr(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr& node) {
    TNodePtr exprNode = sqlExpr.Build(node);
    if (!exprNode) {
        return false;
    }
    exprNodes.push_back(exprNode);
    return true;
}

bool ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node) {
    if (!Expr(sqlExpr, exprNodes, node.GetRule_expr1())) {
        return false;
    }
    for (auto b: node.GetBlock2()) {
        sqlExpr.Token(b.GetToken1());
        if (!Expr(sqlExpr, exprNodes, b.GetRule_expr2())) {
            return false;
        }
    }
    return true;
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
        if (*iter == 'l' || *iter == 's' || *iter == 't' || *iter == 's' || *iter == 'i' ||  *iter == 'b' || *iter == 'n') {
            --iter;
        }
        if (*iter == 'u' || *iter == 'p') {
            --iter;
        }
        suffix = TString(++iter, str.cend());
    }
    value = 0;
    const TString digString(str.begin() + (base == 10 ? 0 : 2), str.end() - suffix.size());
    for (const char& cur: digString) {
        const ui64 curDigit = Char2DigitTable[static_cast<int>(cur)];
        if (curDigit >= base) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", char: '" << cur <<
                "' is out of base: " << base;
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
    if (suffix ==  "") {
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
        return new TLiteralNumberNode<float>(ctx.Pos(), "Float", value.substr(0, value.size()-1));
    } else if (lower.EndsWith("p")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat8", value.substr(0, value.size()-1));
    } else if (lower.EndsWith("pf4")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat4", value.substr(0, value.size()-3));
    } else if (lower.EndsWith("pf8")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat8", value.substr(0, value.size()-3));
    } else if (lower.EndsWith("pn")) {
        return new TLiteralNode(ctx.Pos(), "PgNumeric", value.substr(0, value.size()-2));
    } else {
        return new TLiteralNumberNode<double>(ctx.Pos(), "Double", value);
    }
}

TMaybe<TExprOrIdent> TSqlExpression::LiteralExpr(const TRule_literal_value& node) {
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_literal_value::kAltLiteralValue1: {
            result.Expr = LiteralNumber(Ctx, node.GetAlt_literal_value1().GetRule_integer1());
            break;
        }
        case TRule_literal_value::kAltLiteralValue2: {
            result.Expr = LiteralReal(Ctx, node.GetAlt_literal_value2().GetRule_real1());
            break;
        }
        case TRule_literal_value::kAltLiteralValue3: {
            const TString value(Token(node.GetAlt_literal_value3().GetToken1()));
            return BuildLiteralTypedSmartStringOrId(Ctx, value);
        }
        case TRule_literal_value::kAltLiteralValue5: {
            Token(node.GetAlt_literal_value5().GetToken1());
            result.Expr = BuildLiteralNull(Ctx.Pos());
            break;
        }
        case TRule_literal_value::kAltLiteralValue9: {
            const TString value(to_lower(Token(node.GetAlt_literal_value9().GetRule_bool_value1().GetToken1())));
            result.Expr = BuildLiteralBool(Ctx.Pos(), FromString<bool>(value));
            break;
        }
        case TRule_literal_value::kAltLiteralValue10: {
            result.Expr = BuildEmptyAction(Ctx.Pos());
            break;
        }
        case TRule_literal_value::kAltLiteralValue4:
        case TRule_literal_value::kAltLiteralValue6:
        case TRule_literal_value::kAltLiteralValue7:
        case TRule_literal_value::kAltLiteralValue8:
        case TRule_literal_value::ALT_NOT_SET:
            AltNotImplemented("literal_value", node);
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

template<typename TUnarySubExprType>
TNodePtr TSqlExpression::UnaryExpr(const TUnarySubExprType& node, const TTrailingQuestions& tail) {
    if constexpr (std::is_same_v<TUnarySubExprType, TRule_unary_subexpr>) {
        if (node.Alt_case() == TRule_unary_subexpr::kAltUnarySubexpr1) {
            return UnaryCasualExpr(node.GetAlt_unary_subexpr1().GetRule_unary_casual_subexpr1(), tail);
        } else if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return {};
        } else {
            MaybeUnnamedSmartParenOnTop = false;
            return JsonApiExpr(node.GetAlt_unary_subexpr2().GetRule_json_api_expr1());
        }
    } else {
        MaybeUnnamedSmartParenOnTop = false;
        if (node.Alt_case() == TRule_in_unary_subexpr::kAltInUnarySubexpr1) {
            return UnaryCasualExpr(node.GetAlt_in_unary_subexpr1().GetRule_in_unary_casual_subexpr1(), tail);
        } else if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return {};
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
    TPosition pos = Ctx.Pos();

    auto parsed = StringContent(Ctx, pos, value);
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

TNodePtr TSqlExpression::JsonInputArg(const TRule_json_common_args& node) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TNodePtr jsonExpr = Build(node.GetRule_expr1());
    if (!jsonExpr || jsonExpr->IsNull()) {
        jsonExpr = new TCallNodeImpl(Ctx.Pos(), "Nothing", {
            new TCallNodeImpl(Ctx.Pos(), "OptionalType", {BuildDataType(Ctx.Pos(), "Json")})
        });
    }

    return jsonExpr;
}

void TSqlExpression::AddJsonVariable(const TRule_json_variable& node, TVector<TNodePtr>& children) {
    /*
        json_variable: expr AS json_variable_name;
    */
    TNodePtr expr;
    TString rawName;
    TPosition namePos = Ctx.Pos();
    ui32 nameFlags = 0;

    expr = Build(node.GetRule_expr1());
    const auto& nameRule = node.GetRule_json_variable_name3();
    switch (nameRule.GetAltCase()) {
        case TRule_json_variable_name::kAltJsonVariableName1:
            rawName = Id(nameRule.GetAlt_json_variable_name1().GetRule_id_expr1(), *this);
            nameFlags = TNodeFlags::ArbitraryContent;
            break;
        case TRule_json_variable_name::kAltJsonVariableName2: {
            const auto& token = nameRule.GetAlt_json_variable_name2().GetToken1();
            namePos = GetPos(token);
            auto parsed = StringContentOrIdContent(Ctx, namePos, token.GetValue());
            if (!parsed) {
                return;
            }
            rawName = parsed->Content;
            nameFlags = parsed->Flags;
            break;
        }
        case TRule_json_variable_name::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    TNodePtr nameExpr = BuildQuotedAtom(namePos, rawName, nameFlags);
    children.push_back(BuildTuple(namePos, {nameExpr, expr}));
}

void TSqlExpression::AddJsonVariables(const TRule_json_variables& node, TVector<TNodePtr>& children) {
    /*
        json_variables: json_variable (COMMA json_variable)*;
    */
    AddJsonVariable(node.GetRule_json_variable1(), children);
    for (size_t i = 0; i < node.Block2Size(); i++) {
            AddJsonVariable(node.GetBlock2(i).GetRule_json_variable2(), children);
    }
}

TNodePtr TSqlExpression::JsonVariables(const TRule_json_common_args& node) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TVector<TNodePtr> variables;
    TPosition pos = Ctx.Pos();
    if (node.HasBlock4()) {
        const auto& block = node.GetBlock4();
        pos = GetPos(block.GetToken1());
        AddJsonVariables(block.GetRule_json_variables2(), variables);
    }
    return new TCallNodeImpl(pos, "JsonVariables", variables);
}

void TSqlExpression::AddJsonCommonArgs(const TRule_json_common_args& node, TVector<TNodePtr>& children) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TNodePtr jsonExpr = JsonInputArg(node);
    TNodePtr jsonPath = JsonPathSpecification(node.GetRule_jsonpath_spec3());
    TNodePtr variables = JsonVariables(node);

    children.push_back(jsonExpr);
    children.push_back(jsonPath);
    children.push_back(variables);
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
            return Build(node.GetAlt_json_case_handler3().GetRule_expr2());
        case TRule_json_case_handler::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

void TSqlExpression::AddJsonValueCaseHandlers(const TRule_json_value& node, TVector<TNodePtr>& children) {
    /*
        json_case_handler*
    */
    if (node.Block5Size() > 2) {
        Ctx.Error() << "Only 1 ON EMPTY and/or 1 ON ERROR clause is expected";
        Ctx.IncrementMonCounter("sql_errors", "JsonValueTooManyHandleClauses");
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
            Ctx.Error() << "Only 1 ON EMPTY clause is expected";
            Ctx.IncrementMonCounter("sql_errors", "JsonValueMultipleOnEmptyClauses");
            return;
        }

        if (!isEmptyClause && onError != nullptr) {
            Ctx.Error() << "Only 1 ON ERROR clause is expected";
            Ctx.IncrementMonCounter("sql_errors", "JsonValueMultipleOnErrorClauses");
            return;
        }

        if (isEmptyClause && onError != nullptr) {
            Ctx.Error() << "ON EMPTY clause must be before ON ERROR clause";
            Ctx.IncrementMonCounter("sql_errors", "JsonValueOnEmptyAfterOnError");
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
        onEmpty = new TCallNodeImpl(Ctx.Pos(), "Null", {});
    }

    if (onError == nullptr) {
        onError = new TCallNodeImpl(Ctx.Pos(), "Null", {});
    }

    children.push_back(BuildQuotedAtom(Ctx.Pos(), ToString(onEmptyMode), TNodeFlags::Default));
    children.push_back(onEmpty);
    children.push_back(BuildQuotedAtom(Ctx.Pos(), ToString(onErrorMode), TNodeFlags::Default));
    children.push_back(onError);
}

TNodePtr TSqlExpression::JsonValueExpr(const TRule_json_value& node) {
    /*
        json_value: JSON_VALUE LPAREN
            json_common_args
            (RETURNING type_name_simple)?
            (json_case_handler ON (EMPTY | ERROR))*
        RPAREN;
    */
    TVector<TNodePtr> children;
    AddJsonCommonArgs(node.GetRule_json_common_args3(), children);
    AddJsonValueCaseHandlers(node, children);

    if (node.HasBlock4()) {
        auto returningType = JsonReturningTypeRule(node.GetBlock4().GetRule_type_name_simple2());
        if (!returningType) {
            return {};
        }
        children.push_back(returningType);
    }

    return new TCallNodeImpl(GetPos(node.GetToken1()), "JsonValue", children);
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
        children.push_back(buildJustBool(Ctx.Pos(), false));
        return;
    }

    const auto& handlerRule = node.GetBlock4().GetRule_json_exists_handler1();
    const auto& token = handlerRule.GetToken1();
    const auto pos = GetPos(token);
    const auto mode = to_lower(token.GetValue());
    if (mode == "unknown") {
        const auto nothingNode = new TCallNodeImpl(pos, "Nothing", {
            new TCallNodeImpl(pos, "OptionalType", {BuildDataType(pos, "Bool")})
        });
        children.push_back(nothingNode);
    } else if (mode != "error") {
        children.push_back(buildJustBool(pos, FromString<bool>(mode)));
    }
}

TNodePtr TSqlExpression::JsonExistsExpr(const TRule_json_exists& node) {
    /*
        json_exists: JSON_EXISTS LPAREN
            json_common_args
            json_exists_handler?
        RPAREN;
    */
    TVector<TNodePtr> children;
    AddJsonCommonArgs(node.GetRule_json_common_args3(), children);

    AddJsonExistsHandler(node, children);

    return new TCallNodeImpl(GetPos(node.GetToken1()), "JsonExists", children);
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlExpression::JsonQueryExpr(const TRule_json_query& node) {
    /*
        json_query: JSON_QUERY LPAREN
            json_common_args
            (json_query_wrapper WRAPPER)?
            (json_query_handler ON EMPTY)?
            (json_query_handler ON ERROR)?
        RPAREN;
    */

    TVector<TNodePtr> children;
    AddJsonCommonArgs(node.GetRule_json_common_args3(), children);

    auto addChild = [&](TPosition pos, const TString& content) {
        children.push_back(BuildQuotedAtom(pos, content, TNodeFlags::Default));
    };

    const auto wrapMode = JsonQueryWrapper(node);
    addChild(Ctx.Pos(), ToString(wrapMode));

    auto onEmpty = EJsonQueryHandler::Null;
    if (node.HasBlock5()) {
        if (wrapMode != EJsonQueryWrap::NoWrap) {
            Ctx.Error() << "ON EMPTY is prohibited because WRAPPER clause is specified";
            Ctx.IncrementMonCounter("sql_errors", "JsonQueryOnEmptyWithWrapper");
            return nullptr;
        }
        onEmpty = JsonQueryHandler(node.GetBlock5().GetRule_json_query_handler1());
    }
    addChild(Ctx.Pos(), ToString(onEmpty));

    auto onError = EJsonQueryHandler::Null;
    if (node.HasBlock6()) {
        onError = JsonQueryHandler(node.GetBlock6().GetRule_json_query_handler1());
    }
    addChild(Ctx.Pos(), ToString(onError));

    return new TCallNodeImpl(GetPos(node.GetToken1()), "JsonQuery", children);
}

TNodePtr TSqlExpression::JsonApiExpr(const TRule_json_api_expr& node) {
    /*
        json_api_expr: json_value | json_exists | json_query;
    */
    TPosition pos = Ctx.Pos();
    TNodePtr result = nullptr;
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
            Y_ABORT("You should change implementation according to grammar changes");
    }

    return result;
}

TNodePtr MatchRecognizeVarAccess(TTranslation& ctx, const TString& var, const TRule_an_id_or_type& suffix, bool theSameVar) {
    switch (suffix.GetAltCase()) {
        case TRule_an_id_or_type::kAltAnIdOrType1:
            break;
        case TRule_an_id_or_type::kAltAnIdOrType2:
            break;
        case TRule_an_id_or_type::ALT_NOT_SET:
            break;
    }
    const auto& column = Id(
            suffix.GetAlt_an_id_or_type1()
                .GetRule_id_or_type1().GetAlt_id_or_type1().GetRule_id1(),
            ctx
    );
    return BuildMatchRecognizeVarAccess(TPosition{}, var, column, theSameVar);
}

TNodePtr TSqlExpression::RowPatternVarAccess(const TString& alias, const TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2 block) {
    switch (block.GetAltCase()) {
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt1:
            break;
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt2:
            break;
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt3:
            switch (block.GetAlt3().GetRule_an_id_or_type1().GetAltCase()) {
                case TRule_an_id_or_type::kAltAnIdOrType1: {
                    const auto &idOrType = block.GetAlt3().GetRule_an_id_or_type1().GetAlt_an_id_or_type1().GetRule_id_or_type1();
                    switch(idOrType.GetAltCase()) {
                        case TRule_id_or_type::kAltIdOrType1:
                            return BuildMatchRecognizeVarAccess(
                                    Ctx.Pos(),
                                    alias,
                                    Id(idOrType.GetAlt_id_or_type1().GetRule_id1(), *this),
                                    Ctx.GetMatchRecognizeDefineVar() == alias
                            );
                        case TRule_id_or_type::kAltIdOrType2:
                            break;
                        case TRule_id_or_type::ALT_NOT_SET:
                            break;
                    }
                    break;
                }
                case TRule_an_id_or_type::kAltAnIdOrType2:
                    break;
                case TRule_an_id_or_type::ALT_NOT_SET:
                    break;
            }
            break;
        case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    return TNodePtr{};
}

template<typename TUnaryCasualExprRule>
TNodePtr TSqlExpression::UnaryCasualExpr(const TUnaryCasualExprRule& node, const TTrailingQuestions& tail) {
    // unary_casual_subexpr: (id_expr | atom_expr) unary_subexpr_suffix;
    // OR
    // in_unary_casual_subexpr: (id_expr_in | in_atom_expr) unary_subexpr_suffix;
    // where
    // unary_subexpr_suffix: (key_expr | invoke_expr |(DOT (bind_parameter | DIGITS | id)))* (COLLATE id)?;

    const auto& suffix = node.GetRule_unary_subexpr_suffix2();
    const bool suffixIsEmpty = suffix.GetBlock1().empty() && !suffix.HasBlock2();
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && suffixIsEmpty;
    TString name;
    TNodePtr expr;
    bool typePossible = false;
    auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TUnaryCasualExprRule::TBlock1::kAlt1: {
            MaybeUnnamedSmartParenOnTop = false;
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
            TMaybe<TExprOrIdent> exprOrId;
            if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                exprOrId = AtomExpr(alt.GetRule_atom_expr1(), suffixIsEmpty ? tail : TTrailingQuestions{});
            } else {
                MaybeUnnamedSmartParenOnTop = false;
                exprOrId = InAtomExpr(alt.GetRule_in_atom_expr1(), suffixIsEmpty ? tail : TTrailingQuestions{});
            }

            if (!exprOrId) {
                Ctx.IncrementMonCounter("sql_errors", "BadAtomExpr");
                return nullptr;
            }
            if (!exprOrId->Expr) {
                name = exprOrId->Ident;
            } else {
                expr = exprOrId->Expr;
            }
            break;
        }
        case TUnaryCasualExprRule::TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
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
            if (isColumnRef && EColumnRefState::MatchRecognize == Ctx.GetColumnReferenceState()) {
                if (auto rowPatternVarAccess = RowPatternVarAccess(
                    name,
                    b.GetAlt3().GetBlock2())
                ) {
                    return rowPatternVarAccess;
                }
            }
            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::TBlock1::ALT_NOT_SET:
            AltNotImplemented("unary_subexpr_suffix", b);
            return nullptr;
        }

        isFirstElem = false;
    }

    isFirstElem = true;
    TVector<INode::TIdPart> ids;
    INode::TPtr lastExpr;
    if (!isColumnRef) {
        lastExpr = expr;
    } else {
        const bool flexibleTypes = Ctx.FlexibleTypes;
        bool columnOrType = false;
        auto columnRefsState = Ctx.GetColumnReferenceState();
        bool explicitPgType = columnRefsState == EColumnRefState::AsPgType;
        if (explicitPgType && typePossible && suffixIsEmpty) {
            auto pgType = BuildSimpleType(Ctx, Ctx.Pos(), name, false);
            if (pgType && tail.Count) {
                Ctx.Error() << "Optional types are not supported in this context";
                return {};
            }
            return pgType;
        }
        if (auto simpleType = LookupSimpleType(name, flexibleTypes, false); simpleType && typePossible && suffixIsEmpty) {
            if (tail.Count > 0 || columnRefsState == EColumnRefState::Deny || !flexibleTypes) {
                // a type
                return AddOptionals(BuildSimpleType(Ctx, Ctx.Pos(), name, false), tail.Count);
            }
            // type or column: ambiguity will be resolved on type annotation stage
            columnOrType = columnRefsState == EColumnRefState::Allow;
        }
        if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return {};
        }
        if (!Ctx.CheckColumnReference(Ctx.Pos(), name)) {
            return nullptr;
        }

        ids.push_back(columnOrType ? BuildColumnOrType(Ctx.Pos()) : BuildColumn(Ctx.Pos()));
        ids.push_back(name);
    }

    TPosition pos(Ctx.Pos());
    for (auto& _b : suffix.GetBlock1()) {
        auto& b = _b.GetBlock1();
        switch (b.Alt_case()) {
        case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt1: {
            // key_expr
            auto keyExpr = KeyExpr(b.GetAlt1().GetRule_key_expr1());
            if (!keyExpr) {
                Ctx.IncrementMonCounter("sql_errors", "BadKeyExpr");
                return nullptr;
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
            TSqlCallExpr call(Ctx, Mode);
            if (isFirstElem && !name.Empty()) {
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
                return nullptr;
            }

            lastExpr = call.BuildCall();
            if (!lastExpr) {
                return nullptr;
            }

            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt3: {
            // dot
            if (lastExpr) {
                ids.push_back(lastExpr);
            }

            auto bb = b.GetAlt3().GetBlock2();
            switch (bb.Alt_case()) {
                case TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2::kAlt1: {
                    TString named;
                    if (!NamedNodeImpl(bb.GetAlt1().GetRule_bind_parameter1(), named, *this)) {
                        return nullptr;
                    }
                    auto namedNode = GetNamedNode(named);
                    if (!namedNode) {
                        return nullptr;
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
                    Y_ABORT("You should change implementation according to grammar changes");
            }

            if (lastExpr) {
                lastExpr = BuildAccess(pos, ids, false);
                ids.clear();
            }

            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::TBlock1::ALT_NOT_SET:
            AltNotImplemented("unary_subexpr_suffix", b);
            return nullptr;
        }

        isFirstElem = false;
    }

    if (!lastExpr) {
        lastExpr = BuildAccess(pos, ids, false);
        ids.clear();
    }

    if (suffix.HasBlock2()) {
        Ctx.IncrementMonCounter("sql_errors", "CollateUnarySubexpr");
        Error() << "unary_subexpr: COLLATE is not implemented yet";
    }

    return lastExpr;
}

TNodePtr TSqlExpression::BindParameterRule(const TRule_bind_parameter& rule, const TTrailingQuestions& tail) {
    TString namedArg;
    if (!NamedNodeImpl(rule, namedArg, *this)) {
        return {};
    }
    if (SmartParenthesisMode == ESmartParenthesis::SqlLambdaParams) {
        Ctx.IncrementMonCounter("sql_features", "LambdaArgument");
        if (tail.Count > 1) {
            Ctx.Error(tail.Pos) << "Expecting at most one '?' token here (for optional lambda parameters), but got " << tail.Count;
            return {};
        }
        return BuildAtom(Ctx.Pos(), namedArg, NYql::TNodeFlags::ArbitraryContent, tail.Count != 0);
    }
    if (tail.Count) {
        UnexpectedQuestionToken(tail);
        return {};
    }
    Ctx.IncrementMonCounter("sql_features", "NamedNodeUseAtom");
    return GetNamedNode(namedArg);
}

TNodePtr TSqlExpression::LambdaRule(const TRule_lambda& rule) {
    const auto& alt = rule;
    const bool isSqlLambda = alt.HasBlock2();
    if (!isSqlLambda) {
        return SmartParenthesis(alt.GetRule_smart_parenthesis1());
    }

    MaybeUnnamedSmartParenOnTop = false;
    TNodePtr parenthesis;
    {
        // we allow column reference here to postpone error and report it with better description in SqlLambdaParams
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        expr.SetSmartParenthesisMode(ESmartParenthesis::SqlLambdaParams);
        parenthesis = expr.SmartParenthesis(alt.GetRule_smart_parenthesis1());
    }
    if (!parenthesis) {
        return {};
    }

    ui32 optionalArgumentsCount = 0;
    TVector<TSymbolNameWithPos> args;
    if (!SqlLambdaParams(parenthesis, args, optionalArgumentsCount)) {
        return {};
    }
    auto bodyBlock = alt.GetBlock2();
    Token(bodyBlock.GetToken1());
    TPosition pos(Ctx.Pos());
    TVector<TNodePtr> exprSeq;
    for (auto& arg: args) {
        arg.Name = PushNamedAtom(arg.Pos, arg.Name);
    }
    bool ret = false;
    TColumnRefScope scope(Ctx, EColumnRefState::Deny);
    scope.SetNoColumnErrContext("in lambda function");
    if (bodyBlock.GetBlock2().HasAlt1()) {
        ret = SqlLambdaExprBody(Ctx, bodyBlock.GetBlock2().GetAlt1().GetRule_expr2(), exprSeq);
    } else {
        ret = SqlLambdaExprBody(Ctx, bodyBlock.GetBlock2().GetAlt2().GetRule_lambda_body2(), exprSeq);
    }

    TVector<TString> argNames;
    for (const auto& arg : args) {
        argNames.push_back(arg.Name);
        PopNamedNode(arg.Name);
    }
    if (!ret) {
        return {};
    }

    auto lambdaNode = BuildSqlLambda(pos, std::move(argNames), std::move(exprSeq));
    if (optionalArgumentsCount > 0) {
        lambdaNode = new TCallNodeImpl(pos, "WithOptionalArgs", {
            lambdaNode,
            BuildQuotedAtom(pos, ToString(optionalArgumentsCount), TNodeFlags::Default)
            });
    }

    return lambdaNode;
}

TNodePtr TSqlExpression::CastRule(const TRule_cast_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Cast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx.Pos());
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(rule.GetRule_expr3());
    if (!exprNode) {
        return {};
    }
    auto type = TypeNodeOrBind(rule.GetRule_type_name_or_bind5());
    if (!type) {
        return {};
    }
    return new TCallNodeImpl(pos, "SafeCast", {exprNode, type});
}

TNodePtr TSqlExpression::BitCastRule(const TRule_bitcast_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "BitCast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx.Pos());
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(rule.GetRule_expr3());
    if (!exprNode) {
        return {};
    }
    auto type = TypeSimple(rule.GetRule_type_name_simple5(), true);
    if (!type) {
        return {};
    }
    return new TCallNodeImpl(pos, "BitCast", {exprNode, type});
}

TNodePtr TSqlExpression::ExistsRule(const TRule_exists_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Exists");

    TPosition pos;
    TSourcePtr source;
    Token(rule.GetToken2());
    switch (rule.GetBlock3().Alt_case()) {
        case TRule_exists_expr::TBlock3::kAlt1: {
            const auto& alt = rule.GetBlock3().GetAlt1().GetRule_select_stmt1();
            TSqlSelect select(Ctx, Mode);
            source = select.Build(alt, pos);
            break;
        }
        case TRule_exists_expr::TBlock3::kAlt2: {
            const auto& alt = rule.GetBlock3().GetAlt2().GetRule_values_stmt1();
            TSqlValues values(Ctx, Mode);
            source = values.Build(alt, pos);
            break;
        }
        case TRule_exists_expr::TBlock3::ALT_NOT_SET:
            AltNotImplemented("exists_expr", rule.GetBlock3());
    }

    if (!source) {
        Ctx.IncrementMonCounter("sql_errors", "BadSource");
        return nullptr;
    }
    const bool checkExist = true;
    return BuildBuiltinFunc(Ctx, Ctx.Pos(), "ListHasItems", {BuildSourceNode(pos, std::move(source), checkExist)});
}

TNodePtr TSqlExpression::CaseRule(const TRule_case_expr& rule) {
    // case_expr: CASE expr? when_expr+ (ELSE expr)? END;
    // when_expr: WHEN expr THEN expr;
    Ctx.IncrementMonCounter("sql_features", "Case");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TNodePtr elseExpr;
    if (alt.HasBlock4()) {
        Token(alt.GetBlock4().GetToken1());
        TSqlExpression expr(Ctx, Mode);
        elseExpr = expr.Build(alt.GetBlock4().GetRule_expr2());
    } else {
        Ctx.IncrementMonCounter("sql_errors", "ElseIsRequired");
        Error() << "ELSE is required";
        return {};
    }

    TNodePtr caseExpr;
    if (alt.HasBlock2()) {
        TSqlExpression expr(Ctx, Mode);
        caseExpr = expr.Build(alt.GetBlock2().GetRule_expr1());
        if (!caseExpr) {
            return {};
        }
    }

    TVector<TCaseBranch> branches;
    for (size_t i = 0; i < alt.Block3Size(); ++i) {
        branches.emplace_back();
        const auto& block = alt.GetBlock3(i).GetRule_when_expr1();
        Token(block.GetToken1());
        TSqlExpression condExpr(Ctx, Mode);
        branches.back().Pred = condExpr.Build(block.GetRule_expr2());
        if (caseExpr) {
            branches.back().Pred = BuildBinaryOp(Ctx, Ctx.Pos(), "==", caseExpr->Clone(), branches.back().Pred);
        }
        if (!branches.back().Pred) {
            return {};
        }
        Token(block.GetToken3());
        TSqlExpression thenExpr(Ctx, Mode);
        branches.back().Value = thenExpr.Build(block.GetRule_expr4());
        if (!branches.back().Value) {
            return {};
        }
    }
    auto final = ReduceCaseBranches(branches.begin(), branches.end());
    return BuildBuiltinFunc(Ctx, Ctx.Pos(), "If", { final.Pred, final.Value, elseExpr });
}

TMaybe<TExprOrIdent> TSqlExpression::AtomExpr(const TRule_atom_expr& node, const TTrailingQuestions& tail) {
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
        return {};
    }
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (node.Alt_case() == TRule_atom_expr::kAltAtomExpr3);
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_atom_expr::kAltAtomExpr1:
            Ctx.IncrementMonCounter("sql_features", "LiteralExpr");
            return LiteralExpr(node.GetAlt_atom_expr1().GetRule_literal_value1());
        case TRule_atom_expr::kAltAtomExpr2:
            result.Expr = BindParameterRule(node.GetAlt_atom_expr2().GetRule_bind_parameter1(), tail);
            break;
        case TRule_atom_expr::kAltAtomExpr3:
            result.Expr = LambdaRule(node.GetAlt_atom_expr3().GetRule_lambda1());
            break;
        case TRule_atom_expr::kAltAtomExpr4:
            result.Expr = CastRule(node.GetAlt_atom_expr4().GetRule_cast_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr5:
            result.Expr = ExistsRule(node.GetAlt_atom_expr5().GetRule_exists_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr6:
            result.Expr = CaseRule(node.GetAlt_atom_expr6().GetRule_case_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr7: {
            const auto& alt = node.GetAlt_atom_expr7();
            TString module(Id(alt.GetRule_an_id_or_type1(), *this));
            TPosition pos(Ctx.Pos());
            TString name;
            switch (alt.GetBlock3().Alt_case()) {
                case TRule_atom_expr::TAlt7::TBlock3::kAlt1:
                    name = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), *this);
                    break;
                case TRule_atom_expr::TAlt7::TBlock3::kAlt2: {
                    name = Token(alt.GetBlock3().GetAlt2().GetToken1());
                    if (Ctx.AnsiQuotedIdentifiers && name.StartsWith('"')) {
                        // same as previous case
                        name = IdContentFromString(Ctx, name);
                    } else {
                        module = "@" + module;
                    }
                    break;
                }
                case TRule_atom_expr::TAlt7::TBlock3::ALT_NOT_SET:
                    Y_ABORT("Unsigned number: you should change implementation according to grammar changes");
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
            AltNotImplemented("atom_expr", node);
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

TMaybe<TExprOrIdent> TSqlExpression::InAtomExpr(const TRule_in_atom_expr& node, const TTrailingQuestions& tail) {
    // in_atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | lambda
    //   | cast_expr
    //   | case_expr
    //   | an_id_or_type NAMESPACE (id_or_type | STRING_VALUE)
    //   | LPAREN select_stmt RPAREN
    //   | value_constructor
    //   | bitcast_expr
    //   | list_literal
    //   | dict_literal
    //   | struct_literal
    // ;
    if (node.Alt_case() != TRule_in_atom_expr::kAltInAtomExpr2 && tail.Count) {
        UnexpectedQuestionToken(tail);
        return {};
    }
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_in_atom_expr::kAltInAtomExpr1:
            Ctx.IncrementMonCounter("sql_features", "LiteralExpr");
            return LiteralExpr(node.GetAlt_in_atom_expr1().GetRule_literal_value1());
        case TRule_in_atom_expr::kAltInAtomExpr2:
            result.Expr = BindParameterRule(node.GetAlt_in_atom_expr2().GetRule_bind_parameter1(), tail);
            break;
        case TRule_in_atom_expr::kAltInAtomExpr3:
            result.Expr = LambdaRule(node.GetAlt_in_atom_expr3().GetRule_lambda1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr4:
            result.Expr = CastRule(node.GetAlt_in_atom_expr4().GetRule_cast_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr5:
            result.Expr = CaseRule(node.GetAlt_in_atom_expr5().GetRule_case_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr6: {
            const auto& alt = node.GetAlt_in_atom_expr6();
            TString module(Id(alt.GetRule_an_id_or_type1(), *this));
            TPosition pos(Ctx.Pos());
            TString name;
            switch (alt.GetBlock3().Alt_case()) {
            case TRule_in_atom_expr::TAlt6::TBlock3::kAlt1:
                name = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), *this);
                break;
            case TRule_in_atom_expr::TAlt6::TBlock3::kAlt2: {
                name = Token(alt.GetBlock3().GetAlt2().GetToken1());
                if (Ctx.AnsiQuotedIdentifiers && name.StartsWith('"')) {
                    // same as previous case
                    name = IdContentFromString(Ctx, name);
                } else {
                    module = "@" + module;
                }
                break;
            }
            case TRule_in_atom_expr::TAlt6::TBlock3::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
            }
            result.Expr = BuildCallable(pos, module, name, {});
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr7: {
            Token(node.GetAlt_in_atom_expr7().GetToken1());
            // reset column reference scope (select will reenable it where needed)
            TColumnRefScope scope(Ctx, EColumnRefState::Deny);
            TSqlSelect select(Ctx, Mode);
            TPosition pos;
            auto source = select.Build(node.GetAlt_in_atom_expr7().GetRule_select_stmt2(), pos);
            if (!source) {
                Ctx.IncrementMonCounter("sql_errors", "BadSource");
                return {};
            }
            Ctx.IncrementMonCounter("sql_features", "InSubquery");
            const auto alias = Ctx.MakeName("subquerynode");
            const auto ref = Ctx.MakeName("subquery");
            auto& blocks = Ctx.GetCurrentBlocks();
            blocks.push_back(BuildSubquery(std::move(source), alias, Mode == NSQLTranslation::ESqlMode::SUBQUERY, -1, Ctx.Scoped));
            blocks.back()->SetLabel(ref);
            result.Expr = BuildSubqueryRef(blocks.back(), ref, -1);
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr8: {
            result.Expr = ValueConstructor(node.GetAlt_in_atom_expr8().GetRule_value_constructor1());
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr9:
            result.Expr = BitCastRule(node.GetAlt_in_atom_expr9().GetRule_bitcast_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr10:
            result.Expr = ListLiteral(node.GetAlt_in_atom_expr10().GetRule_list_literal1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr11:
            result.Expr = DictLiteral(node.GetAlt_in_atom_expr11().GetRule_dict_literal1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr12:
            result.Expr = StructLiteral(node.GetAlt_in_atom_expr12().GetRule_struct_literal1());
            break;
        case TRule_in_atom_expr::ALT_NOT_SET:
            AltNotImplemented("in_atom_expr", node);
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

bool TSqlExpression::SqlLambdaParams(const TNodePtr& node, TVector<TSymbolNameWithPos>& args, ui32& optionalArgumentsCount) {
    args.clear();
    optionalArgumentsCount = 0;
    auto errMsg = TStringBuf("Invalid lambda arguments syntax. Lambda arguments should start with '$' as named value.");
    auto tupleNodePtr = node->GetTupleNode();;
    if (!tupleNodePtr) {
        Ctx.Error(node->GetPos()) << errMsg;
        return false;
    }
    THashSet<TString> dupArgsChecker;
    for (const auto& argPtr: tupleNodePtr->Elements()) {
        auto contentPtr = argPtr->GetAtomContent();
        if (!contentPtr || !contentPtr->StartsWith("$")) {
            Ctx.Error(argPtr->GetPos()) << errMsg;
            return false;
        }
        if (argPtr->IsOptionalArg()) {
            ++optionalArgumentsCount;
        } else if (optionalArgumentsCount > 0) {
            Ctx.Error(argPtr->GetPos()) << "Non-optional argument can not follow optional one";
            return false;
        }

        if (!IsAnonymousName(*contentPtr) && !dupArgsChecker.insert(*contentPtr).second) {
            Ctx.Error(argPtr->GetPos()) << "Duplicate lambda argument parametr: '" << *contentPtr << "'.";
            return false;
        }
        args.push_back(TSymbolNameWithPos{*contentPtr, argPtr->GetPos()});
    }
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_expr& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    TNodePtr nodeExpr = expr.Build(node);
    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    TVector<TString> localNames;
    bool hasError = false;
    for (auto& block: node.GetBlock2()) {
        const auto& rule = block.GetRule_lambda_stmt1();
        switch (rule.Alt_case()) {
            case TRule_lambda_stmt::kAltLambdaStmt1: {
                TVector<TSymbolNameWithPos> names;
                auto nodeExpr = NamedNode(rule.GetAlt_lambda_stmt1().GetRule_named_nodes_stmt1(), names);
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
                Y_ABORT("SampleClause: does not correspond to grammar changes");
        }
    }

    TNodePtr nodeExpr;
    if (!hasError) {
        nodeExpr = expr.Build(node.GetRule_expr4());
    }

    for (const auto& name : localNames) {
        PopNamedNode(name);
    }

    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

TNodePtr TSqlExpression::SubExpr(const TRule_con_subexpr& node, const TTrailingQuestions& tail) {
    // con_subexpr: unary_subexpr | unary_op unary_subexpr;
    switch (node.Alt_case()) {
        case TRule_con_subexpr::kAltConSubexpr1:
            return UnaryExpr(node.GetAlt_con_subexpr1().GetRule_unary_subexpr1(), tail);
        case TRule_con_subexpr::kAltConSubexpr2: {
            MaybeUnnamedSmartParenOnTop = false;
            Ctx.IncrementMonCounter("sql_features", "UnaryOperation");
            TString opName;
            auto token = node.GetAlt_con_subexpr2().GetRule_unary_op1().GetToken1();
            Token(token);
            TPosition pos(Ctx.Pos());
            switch (token.GetId()) {
                case SQLv1LexerTokens::TOKEN_NOT: opName = "Not"; break;
                case SQLv1LexerTokens::TOKEN_PLUS: opName = "Plus"; break;
                case SQLv1LexerTokens::TOKEN_MINUS: opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedMinus" : "Minus"; break;
                case SQLv1LexerTokens::TOKEN_TILDA: opName = "BitNot"; break;
                default:
                    Ctx.IncrementMonCounter("sql_errors", "UnsupportedUnaryOperation");
                    Error() << "Unsupported unary operation: " << token.GetValue();
                    return nullptr;
            }
            Ctx.IncrementMonCounter("sql_unary_operations", opName);
            auto expr = UnaryExpr(node.GetAlt_con_subexpr2().GetRule_unary_subexpr2(), tail);
            return expr ? expr->ApplyUnaryOp(Ctx, pos, opName) : expr;
        }
        case TRule_con_subexpr::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    return nullptr;
}

TNodePtr TSqlExpression::SubExpr(const TRule_xor_subexpr& node, const TTrailingQuestions& tail) {
    // xor_subexpr: eq_subexpr cond_expr?;
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && !node.HasBlock2();
    TNodePtr res(SubExpr(node.GetRule_eq_subexpr1(), node.HasBlock2() ? TTrailingQuestions{} : tail));
    if (!res) {
        return {};
    }
    TPosition pos(Ctx.Pos());
    if (node.HasBlock2()) {
        auto cond = node.GetBlock2().GetRule_cond_expr1();
        switch (cond.Alt_case()) {
            case TRule_cond_expr::kAltCondExpr1: {
                const auto& matchOp = cond.GetAlt_cond_expr1();
                const bool notMatch = matchOp.HasBlock1();
                const TCiString& opName = Token(matchOp.GetRule_match_op2().GetToken1());
                const auto& pattern = SubExpr(cond.GetAlt_cond_expr1().GetRule_eq_subexpr3(), matchOp.HasBlock4() ? TTrailingQuestions{} : tail);
                if (!pattern) {
                    return {};
                }
                TNodePtr isMatch;
                if (opName == "like" || opName == "ilike") {
                    const TString* escapeLiteral = nullptr;
                    TNodePtr escapeNode;
                    const auto& escaper = BuildUdf(Ctx, pos, "Re2", "PatternFromLike", {});
                    TVector<TNodePtr> escaperArgs({ escaper, pattern });

                    if (matchOp.HasBlock4()) {
                        const auto& escapeBlock = matchOp.GetBlock4();
                        TNodePtr escapeExpr = SubExpr(escapeBlock.GetRule_eq_subexpr2(), tail);
                        if (!escapeExpr) {
                            return {};
                        }
                        escapeLiteral = escapeExpr->GetLiteral("String");
                        escapeNode = escapeExpr;
                        if (escapeLiteral) {
                            Ctx.IncrementMonCounter("sql_features", "LikeEscape");
                            if (escapeLiteral->size() != 1) {
                                Ctx.IncrementMonCounter("sql_errors", "LikeMultiCharEscape");
                                Error() << "ESCAPE clause requires single character argument";
                                return nullptr;
                            }
                            if (escapeLiteral[0] == "%" || escapeLiteral[0] == "_" || escapeLiteral[0] == "\\") {
                                Ctx.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "'%', '_' and '\\' are currently not supported in ESCAPE clause, ";
                                Error() << "please choose any other character";
                                return nullptr;
                            }
                            if (!IsAscii(escapeLiteral->front())) {
                                Ctx.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "Non-ASCII symbols are not supported in ESCAPE clause, ";
                                Error() << "please choose ASCII character";
                                return nullptr;
                            }
                            escaperArgs.push_back(BuildLiteralRawString(pos, *escapeLiteral));
                        } else {
                            Ctx.IncrementMonCounter("sql_errors", "LikeNotLiteralEscape");
                            Error() << "ESCAPE clause requires String literal argument";
                            return nullptr;
                        }
                    }

                    auto re2options = BuildUdf(Ctx, pos, "Re2", "Options", {});
                    if (opName == "ilike") {
                        Ctx.IncrementMonCounter("sql_features", "CaseInsensitiveLike");
                    }
                    auto csModeLiteral = BuildLiteralBool(pos, opName != "ilike");
                    csModeLiteral->SetLabel("CaseSensitive");
                    auto csOption = BuildStructure(pos, { csModeLiteral });
                    auto optionsApply = new TCallNodeImpl(pos, "NamedApply", { re2options, BuildTuple(pos, {}), csOption });

                    const TNodePtr escapedPattern = new TCallNodeImpl(pos, "Apply", { escaperArgs });
                    auto list = new TAstListNodeImpl(pos, { escapedPattern, optionsApply });
                    auto runConfig = new TAstListNodeImpl(pos, { new TAstAtomNodeImpl(pos, "quote", 0), list });

                    const TNodePtr matcher = new TCallNodeImpl(pos, "AssumeStrict", { BuildUdf(Ctx, pos, "Re2", "Match", { runConfig }) });
                    isMatch = new TCallNodeImpl(pos, "Apply", { matcher, res });

                    bool isUtf8 = false;
                    const TString* literalPattern = pattern->GetLiteral("String");
                    if (!literalPattern) {
                        literalPattern = pattern->GetLiteral("Utf8");
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
                            Ctx.IncrementMonCounter("sql_errors", "LikeEscapeSymbolEnd");
                            Error() << "LIKE pattern should not end with escape symbol";
                            return nullptr;
                        }

                        if (opName == "like" || mayIgnoreCase) {
                            // TODO: expand LIKE in optimizers - we can analyze argument types there
                            YQL_ENSURE(!components.empty());
                            const auto& first = components.front();
                            if (components.size() == 1 && first.IsSimple) {
                                // no '%'s and '_'s  in pattern
                                YQL_ENSURE(first.Prefix == first.Suffix);
                                isMatch = BuildBinaryOp(Ctx, pos, "==", res, BuildLiteralRawString(pos, first.Suffix, isUtf8));
                            } else if (!first.Prefix.empty()) {
                                const TString& prefix = first.Prefix;
                                TNodePtr prefixMatch;
                                if (Ctx.EmitStartsWith) {
                                    prefixMatch = BuildBinaryOp(Ctx, pos, "StartsWith", res, BuildLiteralRawString(pos, prefix, isUtf8));
                                } else {
                                    prefixMatch = BuildBinaryOp(Ctx, pos, ">=", res, BuildLiteralRawString(pos, prefix, isUtf8));
                                    auto upperBound = isUtf8 ? NextValidUtf8(prefix) : NextLexicographicString(prefix);
                                    if (upperBound) {
                                        prefixMatch = BuildBinaryOp(
                                            Ctx,
                                            pos,
                                            "And",
                                            prefixMatch,
                                            BuildBinaryOp(Ctx, pos, "<", res, BuildLiteralRawString(pos, TString(*upperBound), isUtf8))
                                        );
                                    }
                                }

                                if (Ctx.AnsiLike && first.IsSimple && components.size() == 2 && components.back().IsSimple) {
                                    const TString& suffix = components.back().Suffix;
                                    // 'prefix%suffix'
                                    if (suffix.empty()) {
                                        isMatch = prefixMatch;
                                    } else {
                                        // len(str) >= len(prefix) + len(suffix) && StartsWith(str, prefix) && EndsWith(str, suffix)
                                        TNodePtr sizePred = BuildBinaryOp(Ctx, pos, ">=",
                                            TNodePtr(new TCallNodeImpl(pos, "Size", { res })),
                                            TNodePtr(new TLiteralNumberNode<ui32>(pos, "Uint32", ToString(prefix.size() + suffix.size()))));
                                        TNodePtr suffixMatch = BuildBinaryOp(Ctx, pos, "EndsWith", res, BuildLiteralRawString(pos, suffix, isUtf8));
                                        isMatch = new TCallNodeImpl(pos, "And", {
                                            sizePred,
                                            prefixMatch,
                                            suffixMatch
                                        });
                                    }
                                } else {
                                    isMatch = BuildBinaryOp(Ctx, pos, "And", prefixMatch, isMatch);
                                }
                            } else if (Ctx.AnsiLike && AllOf(components, [](const auto& comp) { return comp.IsSimple; })) {
                                YQL_ENSURE(first.Prefix.empty());
                                if (components.size() == 3 && components.back().Prefix.empty()) {
                                    // '%foo%'
                                    YQL_ENSURE(!components[1].Prefix.empty());
                                    isMatch = BuildBinaryOp(Ctx, pos, "StringContains", res, BuildLiteralRawString(pos, components[1].Prefix, isUtf8));
                                } else if (components.size() == 2) {
                                    // '%foo'
                                    isMatch = BuildBinaryOp(Ctx, pos, "EndsWith", res, BuildLiteralRawString(pos, components[1].Prefix, isUtf8));
                                }
                            } else if (Ctx.AnsiLike && !components.back().Suffix.empty()) {
                                const TString& suffix = components.back().Suffix;
                                TNodePtr suffixMatch = BuildBinaryOp(Ctx, pos, "EndsWith", res, BuildLiteralRawString(pos, suffix, isUtf8));
                                isMatch = BuildBinaryOp(Ctx, pos, "And", suffixMatch, isMatch);
                            }
                            // TODO: more StringContains/StartsWith/EndsWith cases?
                        }
                    }

                    Ctx.IncrementMonCounter("sql_features", notMatch ? "NotLike" : "Like");

                } else if (opName == "regexp" || opName == "rlike" || opName == "match") {
                    if (matchOp.HasBlock4()) {
                        Ctx.IncrementMonCounter("sql_errors", "RegexpEscape");
                        TString opNameUpper(opName);
                        opNameUpper.to_upper();
                        Error() << opName << " and ESCAPE clauses should not be used together";
                        return nullptr;
                    }

                    if (!Ctx.PragmaRegexUseRe2) {
                        Ctx.Warning(pos, TIssuesIds::CORE_LEGACY_REGEX_ENGINE) << "Legacy regex engine works incorrectly with unicode. Use PRAGMA RegexUseRe2='true';";
                    }

                    const auto& matcher = Ctx.PragmaRegexUseRe2 ?
                        BuildUdf(Ctx, pos, "Re2", opName == "match" ? "Match" : "Grep", {BuildTuple(pos, {pattern, BuildLiteralNull(pos)})}):
                        BuildUdf(Ctx, pos, "Pcre", opName == "match" ? "BacktrackingMatch" : "BacktrackingGrep", { pattern });
                    isMatch = new TCallNodeImpl(pos, "Apply", { matcher, res });
                    if (opName != "match") {
                        Ctx.IncrementMonCounter("sql_features", notMatch ? "NotRegexp" : "Regexp");
                    } else {
                        Ctx.IncrementMonCounter("sql_features", notMatch ? "NotMatch" : "Match");
                    }
                } else {
                    Ctx.IncrementMonCounter("sql_errors", "UnknownMatchOp");
                    AltNotImplemented("match_op", cond);
                    return nullptr;
                }
                return (notMatch && isMatch) ? isMatch->ApplyUnaryOp(Ctx, pos, "Not") : isMatch;
            }
            case TRule_cond_expr::kAltCondExpr2: {
                //   | NOT? IN COMPACT? in_expr
                auto altInExpr = cond.GetAlt_cond_expr2();
                const bool notIn = altInExpr.HasBlock1();
                auto hints = BuildTuple(pos, {});
                bool isCompact = altInExpr.HasBlock3();
                if (!isCompact) {
                    auto sqlHints = Ctx.PullHintForToken(Ctx.TokenPosition(altInExpr.GetToken2()));
                    isCompact = AnyOf(sqlHints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
                }
                if (isCompact) {
                    Ctx.IncrementMonCounter("sql_features", "IsCompactHint");
                    auto sizeHint = BuildTuple(pos, { BuildQuotedAtom(pos, "isCompact", NYql::TNodeFlags::Default) });
                    hints = BuildTuple(pos, { sizeHint });
                }
                TSqlExpression inSubexpr(Ctx, Mode);
                auto inRight = inSubexpr.SqlInExpr(altInExpr.GetRule_in_expr4(), tail);
                auto isIn = BuildBuiltinFunc(Ctx, pos, "In", {res, inRight, hints});
                Ctx.IncrementMonCounter("sql_features", notIn ? "NotIn" : "In");
                return (notIn && isIn) ? isIn->ApplyUnaryOp(Ctx, pos, "Not") : isIn;
            }
            case TRule_cond_expr::kAltCondExpr3: {
                if (tail.Count) {
                    UnexpectedQuestionToken(tail);
                    return {};
                }
                auto altCase = cond.GetAlt_cond_expr3().GetBlock1().Alt_case();
                const bool notNoll =
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt2 ||
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4
                ;

                if (altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4 &&
                    !cond.GetAlt_cond_expr3().GetBlock1().GetAlt4().HasBlock1())
                {
                    Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_MISSING_IS_BEFORE_NOT_NULL) << "Missing IS keyword before NOT NULL";
                }

                auto isNull = BuildIsNullOp(pos, res);
                Ctx.IncrementMonCounter("sql_features", notNoll ? "NotNull" : "Null");
                return (notNoll && isNull) ? isNull->ApplyUnaryOp(Ctx, pos, "Not") : isNull;
            }
            case TRule_cond_expr::kAltCondExpr4: {
                auto alt = cond.GetAlt_cond_expr4();
                const bool symmetric = alt.HasBlock3() && alt.GetBlock3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_SYMMETRIC;
                const bool negation = alt.HasBlock1();
                TNodePtr left = SubExpr(alt.GetRule_eq_subexpr4(), {});
                TNodePtr right = SubExpr(alt.GetRule_eq_subexpr6(), tail);
                if (!left || !right) {
                    return {};
                }

                const bool bothArgNull = left->IsNull() && right->IsNull();
                const bool oneArgNull  = left->IsNull() || right->IsNull();

                if (res->IsNull() || bothArgNull || (symmetric && oneArgNull)) {
                    Ctx.Warning(pos, TIssuesIds::YQL_OPERATION_WILL_RETURN_NULL)
                    << "BETWEEN operation will return NULL here";
                }

                auto buildSubexpr = [&](const TNodePtr& left, const TNodePtr& right) {
                    if (negation) {
                        return BuildBinaryOpRaw(
                            pos,
                            "Or",
                            BuildBinaryOpRaw(pos, "<", res, left),
                            BuildBinaryOpRaw(pos, ">", res, right)
                        );
                    } else {
                        return BuildBinaryOpRaw(
                            pos,
                            "And",
                            BuildBinaryOpRaw(pos, ">=", res, left),
                            BuildBinaryOpRaw(pos, "<=", res, right)
                        );
                    }
                };

                if (symmetric) {
                    Ctx.IncrementMonCounter("sql_features", negation? "NotBetweenSymmetric" : "BetweenSymmetric");
                    return BuildBinaryOpRaw(
                        pos, 
                        negation? "And" : "Or", 
                        buildSubexpr(left, right), 
                        buildSubexpr(right, left)
                    );
                } else {
                    Ctx.IncrementMonCounter("sql_features", negation? "NotBetween" : "Between");
                    return buildSubexpr(left, right);
                }
            }
            case TRule_cond_expr::kAltCondExpr5: {
                auto alt = cond.GetAlt_cond_expr5();
                auto getNode = [](const TRule_cond_expr::TAlt5::TBlock1& b) -> const TRule_eq_subexpr& { return b.GetRule_eq_subexpr2(); };
                return BinOpList(node.GetRule_eq_subexpr1(), getNode, alt.GetBlock1().begin(), alt.GetBlock1().end(), tail);
            }
            case TRule_cond_expr::ALT_NOT_SET:
                Ctx.IncrementMonCounter("sql_errors", "UnknownConditionExpr");
                AltNotImplemented("cond_expr", cond);
                return nullptr;
        }
    }
    return res;
}

TNodePtr TSqlExpression::BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const {
    TPosition pos(Ctx.Pos());
    const size_t opCount = end - begin;
    Y_DEBUG_ABORT_UNLESS(opCount >= 2);
    if (opCount == 2) {
        return BuildBinaryOp(Ctx, pos, opName, *begin, *(begin+1));
    } if (opCount == 3) {
        return BuildBinaryOp(Ctx, pos, opName, BuildBinaryOp(Ctx, pos, opName, *begin, *(begin+1)), *(begin+2));
    } else {
        auto mid = begin + opCount / 2;
        return BuildBinaryOp(Ctx, pos, opName, BinOperList(opName, begin, mid), BinOperList(opName, mid, end));
    }
}

TSqlExpression::TCaseBranch TSqlExpression::ReduceCaseBranches(TVector<TCaseBranch>::const_iterator begin, TVector<TCaseBranch>::const_iterator end) const {
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

    TCaseBranch result;
    result.Pred = new TCallNodeImpl(Ctx.Pos(), "Or", CloneContainer(preds));
    result.Value = BuildBuiltinFunc(Ctx, Ctx.Pos(), "If", { left.Pred, left.Value, right.Value });
    return result;
}

template <typename TNode, typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOper(const TString& opName, const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    if (begin == end) {
        return SubExpr(node, tail);
    }
    // can't have top level smart_parenthesis node if any binary operation is present
    MaybeUnnamedSmartParenOnTop = false;
    Ctx.IncrementMonCounter("sql_binary_operations", opName);
    const size_t listSize = end - begin;
    TVector<TNodePtr> nodes;
    nodes.reserve(1 + listSize);
    nodes.push_back(SubExpr(node, {}));
    for (; begin != end; ++begin) {
        nodes.push_back(SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
    }
    return BinOperList(opName, nodes.begin(), nodes.end());
}

template <typename TNode, typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (begin == end);
    TNodePtr partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        Token(begin->GetToken1());
        TPosition pos(Ctx.Pos());
        TString opName;
        auto tokenId = begin->GetToken1().GetId();
        switch (tokenId) {
            case SQLv1LexerTokens::TOKEN_LESS:
                Ctx.IncrementMonCounter("sql_binary_operations", "Less");
                opName = "<";
                break;
            case SQLv1LexerTokens::TOKEN_LESS_OR_EQ:
                opName = "<=";
                Ctx.IncrementMonCounter("sql_binary_operations", "LessOrEq");
                break;
            case SQLv1LexerTokens::TOKEN_GREATER:
                opName = ">";
                Ctx.IncrementMonCounter("sql_binary_operations", "Greater");
                break;
            case SQLv1LexerTokens::TOKEN_GREATER_OR_EQ:
                opName = ">=";
                Ctx.IncrementMonCounter("sql_binary_operations", "GreaterOrEq");
                break;
            case SQLv1LexerTokens::TOKEN_PLUS:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedAdd" : "+MayWarn";
                Ctx.IncrementMonCounter("sql_binary_operations", "Plus");
                break;
            case SQLv1LexerTokens::TOKEN_MINUS:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedSub" : "-MayWarn";
                Ctx.IncrementMonCounter("sql_binary_operations", "Minus");
                break;
            case SQLv1LexerTokens::TOKEN_ASTERISK:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedMul" : "*MayWarn";
                Ctx.IncrementMonCounter("sql_binary_operations", "Multiply");
                break;
            case SQLv1LexerTokens::TOKEN_SLASH:
                opName = "/MayWarn";
                Ctx.IncrementMonCounter("sql_binary_operations", "Divide");
                if (!Ctx.Scoped->PragmaClassicDivision && partialResult) {
                    partialResult = new TCallNodeImpl(pos, "SafeCast", {std::move(partialResult), BuildDataType(pos, "Double")});
                } else if (Ctx.Scoped->PragmaCheckedOps) {
                    opName = "CheckedDiv";
                }
                break;
            case SQLv1LexerTokens::TOKEN_PERCENT:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedMod" : "%MayWarn";
                Ctx.IncrementMonCounter("sql_binary_operations", "Mod");
                break;
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnsupportedBinaryOperation");
                Error() << "Unsupported binary operation token: " << tokenId;
                return nullptr;
        }

        partialResult = BuildBinaryOp(Ctx, pos, opName, partialResult, SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
        ++begin;
    }

    return partialResult;
}

template <typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TRule_bit_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (begin == end);
    TNodePtr partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        TString opName;
        switch (begin->GetBlock1().Alt_case()) {
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt1: {
                Token(begin->GetBlock1().GetAlt1().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt1().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_SHIFT_LEFT) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "ShiftLeft";
                Ctx.IncrementMonCounter("sql_binary_operations", "ShiftLeft");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt2: {
                opName = "ShiftRight";
                Ctx.IncrementMonCounter("sql_binary_operations", "ShiftRight");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt3: {
                Token(begin->GetBlock1().GetAlt3().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt3().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_ROT_LEFT) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "RotLeft";
                Ctx.IncrementMonCounter("sql_binary_operations", "RotLeft");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt4: {
                opName = "RotRight";
                Ctx.IncrementMonCounter("sql_binary_operations", "RotRight");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt5: {
                Token(begin->GetBlock1().GetAlt5().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt5().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_AMPERSAND) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "BitAnd";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitAnd");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt6: {
                Token(begin->GetBlock1().GetAlt6().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt6().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_PIPE) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "BitOr";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitOr");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt7: {
                Token(begin->GetBlock1().GetAlt7().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt7().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_CARET) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "BitXor";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitXor");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }

        partialResult = BuildBinaryOp(Ctx, Ctx.Pos(), opName, partialResult, SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
        ++begin;
    }

    return partialResult;
}

template <typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TRule_eq_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (begin == end);
    TNodePtr partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        TString opName;
        switch (begin->GetBlock1().Alt_case()) {
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt1: {
                Token(begin->GetBlock1().GetAlt1().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt1().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_EQUALS) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "Equals");
                opName = "==";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt2: {
                Token(begin->GetBlock1().GetAlt2().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt2().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_EQUALS2) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "Equals2");
                opName = "==";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt3: {
                Token(begin->GetBlock1().GetAlt3().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt3().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_NOT_EQUALS) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "NotEquals");
                opName = "!=";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt4: {
                Token(begin->GetBlock1().GetAlt4().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt4().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_NOT_EQUALS2) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "NotEquals2");
                opName = "!=";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt5: {
                Token(begin->GetBlock1().GetAlt5().GetRule_distinct_from_op1().GetToken1());
                opName = begin->GetBlock1().GetAlt5().GetRule_distinct_from_op1().HasBlock2() ? "IsNotDistinctFrom" : "IsDistinctFrom";
                Ctx.IncrementMonCounter("sql_binary_operations", opName);
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }

        partialResult = BuildBinaryOp(Ctx, Ctx.Pos(), opName, partialResult, SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
        ++begin;
    }

    return partialResult;
}

TNodePtr TSqlExpression::SqlInExpr(const TRule_in_expr& node, const TTrailingQuestions& tail) {
    TSqlExpression expr(Ctx, Mode);
    expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::InStatement);
    auto result = expr.UnaryExpr(node.GetRule_in_unary_subexpr1(), tail);
    return result;
}

TNodePtr TSqlExpression::SmartParenthesis(const TRule_smart_parenthesis& node) {
    TVector<TNodePtr> exprs;
    Token(node.GetToken1());
    const TPosition pos(Ctx.Pos());
    const bool isTuple = node.HasBlock3();
    bool expectTuple = SmartParenthesisMode == ESmartParenthesis::InStatement;
    EExpr mode = EExpr::Regular;
    if (SmartParenthesisMode == ESmartParenthesis::SqlLambdaParams) {
        mode = EExpr::SqlLambdaParams;
        expectTuple = true;
    }
    if (node.HasBlock2() && !NamedExprList(node.GetBlock2().GetRule_named_expr_list1(), exprs, mode)) {
        return {};
    }

    bool topLevelGroupBy = MaybeUnnamedSmartParenOnTop && SmartParenthesisMode == ESmartParenthesis::GroupBy;

    bool hasAliases = false;
    bool hasUnnamed = false;
    for (const auto& expr: exprs) {
        if (expr->GetLabel()) {
            hasAliases = true;
        } else {
            hasUnnamed = true;
        }
        if (hasAliases && hasUnnamed && !topLevelGroupBy) {
            Ctx.IncrementMonCounter("sql_errors", "AnonymousStructMembers");
            Ctx.Error(pos) << "Structure does not allow anonymous members";
            return nullptr;
        }
    }
    if (exprs.size() == 1 && hasUnnamed && !isTuple && !expectTuple) {
        return exprs.back();
    }
    if (topLevelGroupBy) {
        if (isTuple) {
            Ctx.IncrementMonCounter("sql_errors", "SimpleTupleInGroupBy");
            Token(node.GetBlock3().GetToken1());
            Ctx.Error() << "Unexpected trailing comma in grouping elements list";
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ListOfNamedNode");
        return BuildListOfNamedNodes(pos, std::move(exprs));
    }
    Ctx.IncrementMonCounter("sql_features", hasUnnamed ? "SimpleTuple" : "SimpleStruct");
    return (hasUnnamed || expectTuple || exprs.size() == 0) ? BuildTuple(pos, exprs) : BuildStructure(pos, exprs);
}

} // namespace NSQLTranslationV1
