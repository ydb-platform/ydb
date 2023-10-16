#include "ast_builder.h"
#include "ast_nodes.h"
#include "parse_double.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/rewrapper/proto/serialization.pb.h>
#include <ydb/library/yql/ast/yql_ast_escaping.h>

#include <util/generic/singleton.h>
#include <util/system/compiler.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/charset/utf8.h>
#include <util/system/cpu_id.h>

#include <cmath>

using namespace NYql;
using namespace NYql::NJsonPath;
using namespace NJsonPathGenerated;
using namespace NReWrapper;

namespace {

constexpr ui32 RegexpLibId = NReWrapper::TSerialization::YDB_REWRAPPER_LIB_ID;

TPosition GetPos(const TToken& token) {
    return TPosition(token.GetColumn(), token.GetLine());
}

bool TryStringContent(const TString& str, TString& result, TString& error, bool onlyDoubleQuoted = true) {
    result.clear();
    error.clear();

    const bool doubleQuoted = str.StartsWith('"') && str.EndsWith('"');
    const bool singleQuoted = str.StartsWith('\'') && str.EndsWith('\'');
    if (!doubleQuoted && !singleQuoted) {
        error = "String must be quoted";
        return false;
    }
    if (singleQuoted && onlyDoubleQuoted) {
        error = "Only double quoted strings allowed";
        return false;
    }

    char quoteChar = doubleQuoted ? '"' : '\'';
    size_t readBytes = 0;
    TStringBuf atom(str);
    atom.Skip(1);
    TStringOutput sout(result);
    result.reserve(str.size());

    auto unescapeResult = UnescapeArbitraryAtom(atom, quoteChar, &sout, &readBytes);

    if (unescapeResult == EUnescapeResult::OK) {
        return true;
    } else {
        error = UnescapeResultToString(unescapeResult);
        return false;
    }
}

}

TAstBuilder::TAstBuilder(TIssues& issues)
    : Issues(issues)
{
}

void TAstBuilder::Error(TPosition pos, const TStringBuf message) {
    Issues.AddIssue(pos, message);
    Issues.back().SetCode(TIssuesIds::JSONPATH_PARSE_ERROR, TSeverityIds::S_ERROR);
}

TArrayAccessNode::TSubscript TAstBuilder::BuildArraySubscript(const TRule_array_subscript& node) {
    TAstNodePtr from = BuildExpr(node.GetRule_expr1());
    TAstNodePtr to = nullptr;
    if (node.HasBlock2()) {
        to = BuildExpr(node.GetBlock2().GetRule_expr2());
    }
    return {from, to};
}

TAstNodePtr TAstBuilder::BuildArrayAccessor(const TRule_array_accessor& node, TAstNodePtr input) {
    TVector<TArrayAccessNode::TSubscript> subscripts;
    subscripts.reserve(1 + node.Block3Size());

    subscripts.push_back(BuildArraySubscript(node.GetRule_array_subscript2()));
    for (size_t i = 0; i < node.Block3Size(); i++) {
        subscripts.push_back(BuildArraySubscript(node.GetBlock3(i).GetRule_array_subscript2()));
    }

    return new TArrayAccessNode(GetPos(node.GetToken1()), subscripts, input);
}

TAstNodePtr TAstBuilder::BuildWildcardArrayAccessor(const TRule_wildcard_array_accessor& node, TAstNodePtr input) {
    return new TWildcardArrayAccessNode(GetPos(node.GetToken1()), input);
}

TString TAstBuilder::BuildIdentifier(const TRule_identifier& node) {
    switch (node.GetAltCase()) {
        case TRule_identifier::kAltIdentifier1:
            return node.GetAlt_identifier1().GetToken1().GetValue();
        case TRule_identifier::kAltIdentifier2:
            return node.GetAlt_identifier2().GetRule_keyword1().GetToken1().GetValue();
        case TRule_identifier::ALT_NOT_SET:
            Y_ABORT("Alternative for 'identifier' rule is not set");
    }
}

TAstNodePtr TAstBuilder::BuildMemberAccessor(const TRule_member_accessor& node, TAstNodePtr input) {
    TString name;
    const auto& nameBlock = node.GetBlock2();
    switch (nameBlock.GetAltCase()) {
        case TRule_member_accessor_TBlock2::kAlt1:
            name = BuildIdentifier(nameBlock.GetAlt1().GetRule_identifier1());
            break;
        case TRule_member_accessor_TBlock2::kAlt2: {
            const auto& token = nameBlock.GetAlt2().GetToken1();
            TString error;
            if (!TryStringContent(token.GetValue(), name, error, /* onlyDoubleQuoted */ false)) {
                Error(GetPos(token), error);
                return nullptr;
            }
            break;
        }
        case TRule_member_accessor_TBlock2::ALT_NOT_SET:
            Y_ABORT("Alternative for 'member_accessor' rule is not set");
    }

    return new TMemberAccessNode(GetPos(node.GetToken1()), name, input);
}

TAstNodePtr TAstBuilder::BuildWildcardMemberAccessor(const TRule_wildcard_member_accessor& node, TAstNodePtr input) {
    const auto& token = node.GetToken2();
    return new TWildcardMemberAccessNode(GetPos(token), input);
}

TAstNodePtr TAstBuilder::BuildFilter(const TRule_filter& node, TAstNodePtr input) {
    const auto predicate = BuildExpr(node.GetRule_expr3());
    return new TFilterPredicateNode(GetPos(node.GetToken2()), predicate, input);
}

TAstNodePtr TAstBuilder::BuildMethod(const TRule_method& node, TAstNodePtr input) {
    const auto& token = node.GetToken2();
    const auto pos = GetPos(token);
    const auto& value = token.GetValue();
    auto type = EMethodType::Double;
    if (value == "abs") {
        type = EMethodType::Abs;
    } else if (value == "floor") {
        type = EMethodType::Floor;
    } else if (value == "ceiling") {
        type = EMethodType::Ceiling;
    } else if (value == "type") {
        type = EMethodType::Type;
    } else if (value == "size") {
        type = EMethodType::Size;
    } else if (value == "keyvalue") {
        type = EMethodType::KeyValue;
    }

    return new TMethodCallNode(pos, type, input);
}

TAstNodePtr TAstBuilder::BuildAccessorOp(const TRule_accessor_op& node, TAstNodePtr input) {
    switch (node.GetAltCase()) {
        case TRule_accessor_op::kAltAccessorOp1:
            return BuildMemberAccessor(node.GetAlt_accessor_op1().GetRule_member_accessor1(), input);
        case TRule_accessor_op::kAltAccessorOp2:
            return BuildWildcardMemberAccessor(node.GetAlt_accessor_op2().GetRule_wildcard_member_accessor1(), input);
        case TRule_accessor_op::kAltAccessorOp3:
            return BuildArrayAccessor(node.GetAlt_accessor_op3().GetRule_array_accessor1(), input);
        case TRule_accessor_op::kAltAccessorOp4:
            return BuildWildcardArrayAccessor(node.GetAlt_accessor_op4().GetRule_wildcard_array_accessor1(), input);
        case TRule_accessor_op::kAltAccessorOp5:
            return BuildFilter(node.GetAlt_accessor_op5().GetRule_filter1(), input);
        case TRule_accessor_op::kAltAccessorOp6:
            return BuildMethod(node.GetAlt_accessor_op6().GetRule_method1(), input);
        case TRule_accessor_op::ALT_NOT_SET:
            Y_ABORT("Alternative for 'accessor_op' rule is not set");
    }
}

TAstNodePtr TAstBuilder::BuildPrimary(const TRule_primary& node) {
    switch (node.GetAltCase()) {
        case TRule_primary::kAltPrimary1: {
            const auto& token = node.GetAlt_primary1().GetToken1();
            const auto& numberString = token.GetValue();
            const double parsedValue = ParseDouble(numberString);
            if (Y_UNLIKELY(std::isnan(parsedValue))) {
                Y_ABORT("Invalid number was allowed by JsonPath grammar");
            }
            if (Y_UNLIKELY(std::isinf(parsedValue))) {
                Error(GetPos(token), "Number literal is infinity");
                return nullptr;
            }
            return new TNumberLiteralNode(GetPos(token), parsedValue);
        }
        case TRule_primary::kAltPrimary2: {
            const auto& token = node.GetAlt_primary2().GetToken1();
            return new TContextObjectNode(GetPos(token));
        }
        case TRule_primary::kAltPrimary3: {
            const auto& token = node.GetAlt_primary3().GetToken1();
            return new TLastArrayIndexNode(GetPos(token));
        }
        case TRule_primary::kAltPrimary4: {
            const auto& primary = node.GetAlt_primary4().GetBlock1();
            const auto input = BuildExpr(primary.GetRule_expr2());
            if (primary.HasBlock4()) {
                const auto& token = primary.GetBlock4().GetToken1();
                return new TIsUnknownPredicateNode(GetPos(token), input);
            }
            return input;
        }
        case TRule_primary::kAltPrimary5: {
            const auto& token = node.GetAlt_primary5().GetToken1();
            return new TVariableNode(GetPos(token), token.GetValue().substr(1));
        }
        case TRule_primary::kAltPrimary6: {
            const auto& token = node.GetAlt_primary6().GetToken1();
            return new TBooleanLiteralNode(GetPos(token), true);
        }
        case TRule_primary::kAltPrimary7: {
            const auto& token = node.GetAlt_primary7().GetToken1();
            return new TBooleanLiteralNode(GetPos(token), false);
        }
        case TRule_primary::kAltPrimary8: {
            const auto& token = node.GetAlt_primary8().GetToken1();
            return new TNullLiteralNode(GetPos(token));
        }
        case TRule_primary::kAltPrimary9: {
            const auto& token = node.GetAlt_primary9().GetToken1();
            TString value;
            TString error;
            if (!TryStringContent(token.GetValue(), value, error)) {
                Error(GetPos(token), error);
                return nullptr;
            }
            return new TStringLiteralNode(GetPos(token), value);
        }
        case TRule_primary::kAltPrimary10: {
            const auto& token = node.GetAlt_primary10().GetToken1();
            return new TFilterObjectNode(GetPos(token));
        }
        case TRule_primary::ALT_NOT_SET:
            Y_ABORT("Alternative for 'primary' rule is not set");
    }
}

TAstNodePtr TAstBuilder::BuildAccessorExpr(const TRule_accessor_expr& node) {
    TAstNodePtr input = BuildPrimary(node.GetRule_primary1());
    for (size_t i = 0; i < node.Block2Size(); i++) {
        input = BuildAccessorOp(node.GetBlock2(i).GetRule_accessor_op1(), input);
    }
    return input;
}

TAstNodePtr TAstBuilder::BuildPlainExpr(const TRule_plain_expr& node) {
    return BuildAccessorExpr(node.GetRule_accessor_expr1());
}

TAstNodePtr TAstBuilder::BuildLikeRegexExpr(const TRule_like_regex_expr& node, TAstNodePtr input) {
    const auto& regexToken = node.GetToken2();
    TString regex;
    TString error;
    if (!TryStringContent(regexToken.GetValue(), regex, error)) {
        Error(GetPos(regexToken), error);
        return nullptr;
    }

    ui32 parsedFlags = 0;
    if (node.HasBlock3()) {
        TString flags;
        const auto& flagsToken = node.GetBlock3().GetToken2();
        if (!TryStringContent(flagsToken.GetValue(), flags, error)) {
            Error(GetPos(flagsToken), error);
            return nullptr;
        }

        for (char flag : flags) {
            switch (flag) {
                case 'i':
                    parsedFlags |= FLAGS_CASELESS;
                    break;
                default:
                    Error(GetPos(flagsToken), TStringBuilder() << "Unsupported regex flag '" << flag << "'");
                    break;
            }
        }
    }

    IRePtr compiledRegex;
    try {
        compiledRegex = NDispatcher::Compile(regex, parsedFlags, RegexpLibId);
    } catch (const NReWrapper::TCompileException& e) {
        Error(GetPos(regexToken), e.AsStrBuf());
        return nullptr;
    }

    return new TLikeRegexPredicateNode(GetPos(node.GetToken1()), input, std::move(compiledRegex));
}

TAstNodePtr TAstBuilder::BuildPredicateExpr(const TRule_predicate_expr& node) {
    switch (node.GetAltCase()) {
        case TRule_predicate_expr::kAltPredicateExpr1: {
            const auto& predicate = node.GetAlt_predicate_expr1().GetBlock1();
            const auto input = BuildPlainExpr(predicate.GetRule_plain_expr1());
            if (!predicate.HasBlock2()) {
                return input;
            }

            const auto& block = predicate.GetBlock2();
            switch (block.GetAltCase()) {
                case TRule_predicate_expr_TAlt1_TBlock1_TBlock2::kAlt1: {
                    const auto& innerBlock = block.GetAlt1().GetRule_starts_with_expr1();
                    const auto& prefix = BuildPlainExpr(innerBlock.GetRule_plain_expr3());
                    return new TStartsWithPredicateNode(GetPos(innerBlock.GetToken1()), input, prefix);
                }
                case TRule_predicate_expr_TAlt1_TBlock1_TBlock2::kAlt2: {
                    return BuildLikeRegexExpr(block.GetAlt2().GetRule_like_regex_expr1(), input);
                }
                case TRule_predicate_expr_TAlt1_TBlock1_TBlock2::ALT_NOT_SET:
                    Y_ABORT("Alternative for inner block of 'predicate_expr' rule is not set");
            }
            Y_UNREACHABLE();
        }
        case TRule_predicate_expr::kAltPredicateExpr2: {
            const auto& predicate = node.GetAlt_predicate_expr2().GetBlock1();
            const auto input = BuildExpr(predicate.GetRule_expr3());
            return new TExistsPredicateNode(GetPos(predicate.GetToken1()), input);
        }
        case TRule_predicate_expr::ALT_NOT_SET:
            Y_ABORT("Alternative for 'predicate' rule is not set");
    }
    Y_UNREACHABLE();
}

TAstNodePtr TAstBuilder::BuildUnaryExpr(const TRule_unary_expr& node) {
    const auto predicateExpr = BuildPredicateExpr(node.GetRule_predicate_expr2());
    if (!node.HasBlock1()) {
        return predicateExpr;
    }

    const auto& opToken = node.GetBlock1().GetToken1();
    const auto& opValue = opToken.GetValue();
    auto operation = EUnaryOperation::Plus;
    if (opValue == "-") {
        operation = EUnaryOperation::Minus;
    } else if (opValue == "!") {
        operation = EUnaryOperation::Not;
    }
    return new TUnaryOperationNode(GetPos(opToken), operation, predicateExpr);
}

TAstNodePtr TAstBuilder::BuildMulExpr(const TRule_mul_expr& node) {
    TAstNodePtr result = BuildUnaryExpr(node.GetRule_unary_expr1());

    for (size_t i = 0; i < node.Block2Size(); i++) {
        const auto& block = node.GetBlock2(i);

        const auto& opToken = block.GetToken1();
        const auto& opValue = opToken.GetValue();
        auto operation = EBinaryOperation::Multiply;
        if (opValue == "/") {
            operation = EBinaryOperation::Divide;
        } else if (opValue == "%") {
            operation = EBinaryOperation::Modulo;
        }

        const auto rightOperand = BuildUnaryExpr(block.GetRule_unary_expr2());
        result = new TBinaryOperationNode(GetPos(opToken), operation, result, rightOperand);
    }

    return result;
}

TAstNodePtr TAstBuilder::BuildAddExpr(const TRule_add_expr& node) {
    TAstNodePtr result = BuildMulExpr(node.GetRule_mul_expr1());

    for (size_t i = 0; i < node.Block2Size(); i++) {
        const auto& block = node.GetBlock2(i);

        const auto& opToken = block.GetToken1();
        auto operation = EBinaryOperation::Add;
        if (opToken.GetValue() == "-") {
            operation = EBinaryOperation::Substract;
        }

        const auto rightOperand = BuildMulExpr(block.GetRule_mul_expr2());
        result = new TBinaryOperationNode(GetPos(opToken), operation, result, rightOperand);
    }

    return result;
}

TAstNodePtr TAstBuilder::BuildCompareExpr(const TRule_compare_expr& node) {
    TAstNodePtr result = BuildAddExpr(node.GetRule_add_expr1());

    if (node.HasBlock2()) {
        const auto& block = node.GetBlock2();

        const auto& opToken = block.GetToken1();
        const auto& opValue = opToken.GetValue();
        auto operation = EBinaryOperation::Less;
        if (opValue == "<=") {
            operation = EBinaryOperation::LessEqual;
        } else if (opValue == ">") {
            operation = EBinaryOperation::Greater;
        } else if (opValue == ">=") {
            operation = EBinaryOperation::GreaterEqual;
        }

        const auto rightOperand = BuildAddExpr(block.GetRule_add_expr2());
        result = new TBinaryOperationNode(GetPos(opToken), operation, result, rightOperand);
    }

    return result;
}

TAstNodePtr TAstBuilder::BuildEqualExpr(const TRule_equal_expr& node) {
    TAstNodePtr result = BuildCompareExpr(node.GetRule_compare_expr1());

    if (node.HasBlock2()) {
        const auto& block = node.GetBlock2();

        const auto& opToken = block.GetToken1();
        const auto& opValue = opToken.GetValue();
        auto operation = EBinaryOperation::Equal;
        if (opValue == "<>" || opValue == "!=") {
            operation = EBinaryOperation::NotEqual;
        }

        const auto rightOperand = BuildCompareExpr(block.GetRule_compare_expr2());
        result = new TBinaryOperationNode(GetPos(opToken), operation, result, rightOperand);
    }

    return result;
}

TAstNodePtr TAstBuilder::BuildAndExpr(const TRule_and_expr& node) {
    TAstNodePtr result = BuildEqualExpr(node.GetRule_equal_expr1());

    for (size_t i = 0; i < node.Block2Size(); i++) {
        const auto& block = node.GetBlock2(i);

        const auto& opToken = block.GetToken1();
        const auto rightOperand = BuildEqualExpr(block.GetRule_equal_expr2());
        result = new TBinaryOperationNode(GetPos(opToken), EBinaryOperation::And, result, rightOperand);
    }

    return result;
}

TAstNodePtr TAstBuilder::BuildOrExpr(const TRule_or_expr& node) {
    TAstNodePtr result = BuildAndExpr(node.GetRule_and_expr1());

    for (size_t i = 0; i < node.Block2Size(); i++) {
        const auto& block = node.GetBlock2(i);

        const auto& opToken = block.GetToken1();
        const auto rightOperand = BuildAndExpr(block.GetRule_and_expr2());
        result = new TBinaryOperationNode(GetPos(opToken), EBinaryOperation::Or, result, rightOperand);
    }

    return result;
}

TAstNodePtr TAstBuilder::BuildExpr(const TRule_expr& node) {
    return BuildOrExpr(node.GetRule_or_expr1());
}

TAstNodePtr TAstBuilder::BuildJsonPath(const TRule_jsonpath& node) {
    TPosition pos;
    auto mode = EJsonPathMode::Lax;
    if (node.HasBlock1()) {
        const auto& modeToken = node.GetBlock1().GetToken1();
        pos = GetPos(modeToken);
        if (modeToken.GetValue() == "strict") {
            mode = EJsonPathMode::Strict;
        }
    }

    const auto expr = BuildExpr(node.GetRule_expr2());
    return new TRootNode(pos, expr, mode);
}

TAstNodePtr TAstBuilder::Build(const TJsonPathParserAST& ast) {
    return BuildJsonPath(ast.GetRule_jsonpath());
}

namespace NYql::NJsonPath {

ui32 GetReLibId() {
    return RegexpLibId;
}

}
