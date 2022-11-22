#include "executor.h"
#include "parse_double.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/minikql/dom/node.h>

#include <util/generic/scope.h>
#include <util/generic/maybe.h>
#include <util/system/compiler.h>

#include <cmath>

namespace NYql::NJsonPath {

using namespace NJson;
using namespace NUdf;
using namespace NDom;

namespace {

bool IsObjectOrArray(const TValue& value) {
    return value.IsArray() || value.IsObject();
}

TIssue MakeError(TPosition pos, TIssueCode code, const TStringBuf message) {
    TIssue error(pos, message);
    error.SetCode(code, TSeverityIds::S_ERROR);
    return error;
}

TIssue MakeError(const TJsonPathItem& item, TIssueCode code, const TStringBuf message) {
    return MakeError(item.Pos, code, message);
}

}

TResult::TResult(TJsonNodes&& nodes)
    : Result(std::move(nodes))
{
}

TResult::TResult(const TJsonNodes& nodes)
    : Result(nodes)
{
}

TResult::TResult(TIssue&& issue)
    : Result(std::move(issue))
{
}

const TJsonNodes& TResult::GetNodes() const {
    return std::get<TJsonNodes>(Result);
}

TJsonNodes& TResult::GetNodes() {
    return std::get<TJsonNodes>(Result);
}

const TIssue& TResult::GetError() const {
    return std::get<TIssue>(Result);
}

bool TResult::IsError() const {
    return std::holds_alternative<TIssue>(Result);
}

TExecutor::TExecutor(
    const TJsonPathPtr path,
    const TJsonNodes& input,
    const TVariablesMap& variables,
    const IValueBuilder* valueBuilder)
    : Reader(path)
    , Input(input)
    , Variables(variables)
    , ValueBuilder(valueBuilder)
{
}

bool TExecutor::IsZero(double value) {
    return -EPSILON <= value && value <= EPSILON;
}

bool TExecutor::IsLess(double a, double b) {
    return (b - a) > EPSILON;
}

bool TExecutor::IsGreater(double a, double b) {
    return (a - b) > EPSILON;
}

bool TExecutor::IsEqual(double a, double b) {
    return IsZero(a - b);
}

bool TExecutor::IsStrict() const {
    return Reader.GetMode() == EJsonPathMode::Strict;
}

bool TExecutor::IsLax() const {
    return Reader.GetMode() == EJsonPathMode::Lax;
}

TResult TExecutor::Execute() {
    return Execute(Reader.ReadFirst());
}

TResult TExecutor::Execute(const TJsonPathItem& item) {
    switch (item.Type) {
        case EJsonPathItemType::MemberAccess:
            return MemberAccess(item);
        case EJsonPathItemType::WildcardMemberAccess:
            return WildcardMemberAccess(item);
        case EJsonPathItemType::ContextObject:
            return ContextObject();
        case EJsonPathItemType::Variable:
            return Variable(item);
        case EJsonPathItemType::NumberLiteral:
            return NumberLiteral(item);
        case EJsonPathItemType::ArrayAccess:
            return ArrayAccess(item);
        case EJsonPathItemType::WildcardArrayAccess:
            return WildcardArrayAccess(item);
        case EJsonPathItemType::LastArrayIndex:
            return LastArrayIndex(item);
        case EJsonPathItemType::UnaryMinus:
        case EJsonPathItemType::UnaryPlus:
            return UnaryArithmeticOp(item);
        case EJsonPathItemType::BinaryAdd:
        case EJsonPathItemType::BinarySubstract:
        case EJsonPathItemType::BinaryMultiply:
        case EJsonPathItemType::BinaryDivide:
        case EJsonPathItemType::BinaryModulo:
            return BinaryArithmeticOp(item);
        case EJsonPathItemType::BinaryAnd:
        case EJsonPathItemType::BinaryOr:
            return BinaryLogicalOp(item);
        case EJsonPathItemType::UnaryNot:
            return UnaryLogicalOp(item);
        case EJsonPathItemType::BooleanLiteral:
            return BooleanLiteral(item);
        case EJsonPathItemType::NullLiteral:
            return NullLiteral();
        case EJsonPathItemType::StringLiteral:
            return StringLiteral(item);
        case EJsonPathItemType::FilterObject:
            return FilterObject(item);
        case EJsonPathItemType::FilterPredicate:
            return FilterPredicate(item);
        case EJsonPathItemType::BinaryLess:
        case EJsonPathItemType::BinaryLessEqual:
        case EJsonPathItemType::BinaryGreater:
        case EJsonPathItemType::BinaryGreaterEqual:
        case EJsonPathItemType::BinaryEqual:
        case EJsonPathItemType::BinaryNotEqual:
            return CompareOp(item);
        case EJsonPathItemType::AbsMethod:
        case EJsonPathItemType::FloorMethod:
        case EJsonPathItemType::CeilingMethod:
            return NumericMethod(item);
        case EJsonPathItemType::DoubleMethod:
            return DoubleMethod(item);
        case EJsonPathItemType::TypeMethod:
            return TypeMethod(item);
        case EJsonPathItemType::SizeMethod:
            return SizeMethod(item);
        case EJsonPathItemType::KeyValueMethod:
            return KeyValueMethod(item);
        case EJsonPathItemType::StartsWithPredicate:
            return StartsWithPredicate(item);
        case EJsonPathItemType::IsUnknownPredicate:
            return IsUnknownPredicate(item);
        case EJsonPathItemType::ExistsPredicate:
            return ExistsPredicate(item);
        case EJsonPathItemType::LikeRegexPredicate:
            return LikeRegexPredicate(item);
    }
}

TResult TExecutor::ContextObject() {
    return Input;
}

TResult TExecutor::Variable(const TJsonPathItem& item) {
    const auto it = Variables.find(item.GetString());
    if (it == Variables.end()) {
        return MakeError(item, TIssuesIds::JSONPATH_UNDEFINED_VARIABLE, TStringBuilder() << "Undefined variable '" << item.GetString() << "'");
    }

    return TJsonNodes({it->second});
}

TResult TExecutor::LastArrayIndex(const TJsonPathItem& item) {
    if (ArraySubscriptSource.empty()) {
        return MakeError(item, TIssuesIds::JSONPATH_LAST_OUTSIDE_OF_ARRAY_SUBSCRIPT, "'last' is only allowed inside array subscripts");
    }

    const auto& array = ArraySubscriptSource.top();
    const i64 arraySize = array.GetSize();

    // NOTE: For empty arrays `last` equals `-1`. This is intended, PostgreSQL 12 has the same behaviour
    return TJsonNodes({TValue(MakeDouble(static_cast<double>(arraySize - 1)))});
}

TResult TExecutor::NumberLiteral(const TJsonPathItem& item) {
    return TJsonNodes({TValue(MakeDouble(item.GetNumber()))});
}

TResult TExecutor::MemberAccess(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        if (!node.IsObject()) {
            if (IsStrict()) {
                return MakeError(item, TIssuesIds::JSONPATH_EXPECTED_OBJECT, "Expected object");
            } else {
                continue;
            }
        }

        if (const auto payload = node.Lookup(item.GetString())) {
            result.push_back(*payload);
            continue;
        }

        if (IsStrict()) {
            return MakeError(item, TIssuesIds::JSONPATH_MEMBER_NOT_FOUND, "Member not found");
        }
    }

    return std::move(result);
}

TResult TExecutor::WildcardMemberAccess(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        if (!node.IsObject()) {
            if (IsStrict()) {
                return MakeError(item, TIssuesIds::JSONPATH_EXPECTED_OBJECT, "Expected object");
            } else {
                continue;
            }
        }

        TValue key;
        TValue value;
        auto it = node.GetObjectIterator();
        while (it.Next(key, value)) {
            result.push_back(value);
        }
    }

    return std::move(result);
}

TMaybe<TIssue> TExecutor::EnsureSingleSubscript(TPosition pos, const TJsonNodes& index, i64& result) {
    if (index.size() != 1) {
        return MakeError(pos, TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX, "Expected single number item for array index");
    }

    const auto& indexValue = index[0];
    if (!indexValue.IsNumber()) {
        return MakeError(pos, TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX, "Array index must be number");
    }

    result = static_cast<i64>(std::floor(indexValue.GetNumber()));
    return Nothing();
}

TMaybe<TIssue> TExecutor::EnsureArraySubscripts(const TJsonPathItem& item, TVector<TArraySubscript>& result) {
    for (const auto& subscript : item.GetSubscripts()) {
        const auto& fromItem = Reader.ReadFromSubscript(subscript);
        const auto fromResult = Execute(fromItem);
        if (fromResult.IsError()) {
            return fromResult.GetError();
        }

        i64 fromIndex = 0;
        TMaybe<TIssue> error = EnsureSingleSubscript(fromItem.Pos, fromResult.GetNodes(), fromIndex);
        if (error) {
            return error;
        }

        if (!subscript.IsRange()) {
            result.emplace_back(fromIndex, fromItem.Pos);
            continue;
        }

        const auto& toItem = Reader.ReadToSubscript(subscript);
        const auto toResult = Execute(toItem);
        if (toResult.IsError()) {
            return toResult.GetError();
        }

        i64 toIndex = 0;
        error = EnsureSingleSubscript(toItem.Pos, toResult.GetNodes(), toIndex);
        if (error) {
            return error;
        }

        result.emplace_back(fromIndex, fromItem.Pos, toIndex, toItem.Pos);
    }
    return Nothing();
}

TResult TExecutor::ArrayAccess(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : OptionalArrayWrapNodes(input.GetNodes())) {
        if (!node.IsArray()) {
            return MakeError(item, TIssuesIds::JSONPATH_EXPECTED_ARRAY, "Expected array");
        }

        ArraySubscriptSource.push(node);
        Y_DEFER {
            ArraySubscriptSource.pop();
        };

        // Check for "hard" errors in array subscripts. These are forbidden even in lax mode
        // NOTE: We intentionally execute subscripts expressions for each array in the input
        // because they can contain `last` keyword which value is different for each array
        TVector<TArraySubscript> subscripts;
        TMaybe<TIssue> error = EnsureArraySubscripts(item, subscripts);
        if (error) {
            return std::move(*error);
        }

        const ui64 arraySize = node.GetSize();
        for (const auto& idx : subscripts) {
            // Check bounds for first subscript
            if (idx.GetFrom() < 0 || idx.GetFrom() >= static_cast<i64>(arraySize)) {
                if (IsStrict()) {
                    return MakeError(idx.GetFromPos(), TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS, "Array index out of bounds");
                } else {
                    continue;
                }
            }

            // If there is no second subcripts, just return corresponding array element
            if (!idx.IsRange()) {
                result.push_back(node.GetElement(idx.GetFrom()));
                continue;
            }

            // Check bounds for second subscript
            if (idx.GetTo() < 0 || idx.GetTo() >= static_cast<i64>(arraySize)) {
                if (IsStrict()) {
                    return MakeError(idx.GetToPos(), TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS, "Array index out of bounds");
                } else {
                    continue;
                }
            }

            // In strict mode invalid ranges are forbidden
            if (idx.GetFrom() > idx.GetTo() && IsStrict()) {
                return MakeError(idx.GetFromPos(), TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX_RANGE, "Range lower bound is greater than upper bound");
            }

            for (i64 i = idx.GetFrom(); i <= idx.GetTo(); i++) {
                result.push_back(node.GetElement(i));
            }
        }
    }
    return std::move(result);
}

TResult TExecutor::WildcardArrayAccess(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : OptionalArrayWrapNodes(input.GetNodes())) {
        if (!node.IsArray()) {
            return MakeError(item, TIssuesIds::JSONPATH_EXPECTED_ARRAY, "Expected array");
        }

        auto it = node.GetArrayIterator();
        TValue value;
        while (it.Next(value)) {
            result.push_back(value);
        }
    }
    return std::move(result);
}

TResult TExecutor::UnaryArithmeticOp(const TJsonPathItem& item) {
    const auto& operandItem = Reader.ReadInput(item);
    const auto operandsResult = Execute(operandItem);
    if (operandsResult.IsError()) {
        return operandsResult;
    }

    const auto& operands = operandsResult.GetNodes();
    TJsonNodes result;
    result.reserve(operands.size());
    for (const auto& operand : operands) {
        if (!operand.IsNumber()) {
            return MakeError(
                operandItem, TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE,
                TStringBuilder() << "Unsupported type for unary operations"
            );
        }

        if (item.Type == EJsonPathItemType::UnaryPlus) {
            result.push_back(operand);
            continue;
        }

        const auto value = operand.GetNumber();
        result.push_back(TValue(MakeDouble(-value)));
    }

    return std::move(result);
}

TMaybe<TIssue> TExecutor::EnsureBinaryArithmeticOpArgument(TPosition pos, const TJsonNodes& nodes, double& result) {
    if (nodes.size() != 1) {
        return MakeError(pos, TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT, "Expected exactly 1 item as an operand for binary operation");
    }

    const auto& value = nodes[0];
    if (!value.IsNumber()) {
        return MakeError(
            pos, TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE,
            TStringBuilder() << "Unsupported type for binary operations"
        );
    }

    result = value.GetNumber();
    return Nothing();
}

TResult TExecutor::BinaryArithmeticOp(const TJsonPathItem& item) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto leftResult = Execute(leftItem);
    if (leftResult.IsError()) {
        return leftResult;
    }

    double left = 0;
    TMaybe<TIssue> error = EnsureBinaryArithmeticOpArgument(leftItem.Pos, leftResult.GetNodes(), left);
    if (error) {
        return std::move(*error);
    }

    const auto& rightItem = Reader.ReadRightOperand(item);
    const auto rightResult = Execute(rightItem);
    if (rightResult.IsError()) {
        return rightResult;
    }

    double right = 0;
    error = EnsureBinaryArithmeticOpArgument(rightItem.Pos, rightResult.GetNodes(), right);
    if (error) {
        return std::move(*error);
    }

    double result = 0;
    switch (item.Type) {
        case EJsonPathItemType::BinaryAdd:
            result = left + right;
            break;
        case EJsonPathItemType::BinarySubstract:
            result = left - right;
            break;
        case EJsonPathItemType::BinaryMultiply:
            result = left * right;
            break;
        case EJsonPathItemType::BinaryDivide:
            if (IsZero(right)) {
                return MakeError(rightItem, TIssuesIds::JSONPATH_DIVISION_BY_ZERO, "Division by zero");
            }
            result = left / right;
            break;
        case EJsonPathItemType::BinaryModulo:
            if (IsZero(right)) {
                return MakeError(rightItem, TIssuesIds::JSONPATH_DIVISION_BY_ZERO, "Division by zero");
            }
            result = std::fmod(left, right);
            break;
        default:
            YQL_ENSURE(false, "Expected binary arithmetic operation");
    }

    if (Y_UNLIKELY(std::isinf(result))) {
        return MakeError(item, TIssuesIds::JSONPATH_BINARY_OPERATION_RESULT_INFINITY, "Binary operation result is infinity");
    }

    return TJsonNodes({TValue(MakeDouble(result))});
}

TMaybe<TIssue> TExecutor::EnsureLogicalOpArgument(TPosition pos, const TJsonNodes& nodes, TMaybe<bool>& result) {
    if (nodes.size() != 1) {
        return MakeError(pos, TIssuesIds::JSONPATH_INVALID_LOGICAL_OPERATION_ARGUMENT, "Expected exactly 1 item as an operand for logical operation");
    }

    const auto& value = nodes[0];
    if (value.IsNull()) {
        result = Nothing();
    } else if (value.IsBool()) {
        result = value.GetBool();
    } else {
        return MakeError(pos, TIssuesIds::JSONPATH_INVALID_LOGICAL_OPERATION_ARGUMENT, "Unsupported type for logical operation");
    }

    return Nothing();
}

TResult TExecutor::BinaryLogicalOp(const TJsonPathItem& item) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto leftResult = Execute(leftItem);
    if (leftResult.IsError()) {
        return leftResult;
    }

    TMaybe<bool> left;
    TMaybe<TIssue> error = EnsureLogicalOpArgument(leftItem.Pos, leftResult.GetNodes(), left);
    if (error) {
        return std::move(*error);
    }

    const auto& rightItem = Reader.ReadRightOperand(item);
    const auto rightResult = Execute(rightItem);
    if (rightResult.IsError()) {
        return rightResult;
    }

    TMaybe<bool> right;
    error = EnsureLogicalOpArgument(rightItem.Pos, rightResult.GetNodes(), right);
    if (error) {
        return std::move(*error);
    }

    switch (item.Type) {
        case EJsonPathItemType::BinaryAnd: {
            /*
                AND truth table (taken from SQL JSON standard)

                |  &&   | true  | false | null  |
                | ----- | ----- | ----- | ----- |
                | true  | true  | false | null  |
                | false | false | false | false |
                | null  | null  | false | null  |
            */
            if (left.Defined() && right.Defined()) {
                return TJsonNodes({TValue(MakeBool(*left && *right))});
            }

            const bool falseVsNull = !left.GetOrElse(true) && !right.Defined();
            const bool nullVsFalse = !right.GetOrElse(true) && !left.Defined();
            if (falseVsNull || nullVsFalse) {
                return TJsonNodes({TValue(MakeBool(false))});
            }
            return TJsonNodes({TValue(MakeEntity())});
        }
        case EJsonPathItemType::BinaryOr: {
            /*
                OR truth table (taken from SQL JSON standard)

                |  ||   | true  | false | null  |
                | ----- | ----- | ----- | ----- |
                | true  | true  | true  | true  |
                | false | true  | false | null  |
                | null  | true  | null  | null  |
            */
            if (left.Defined() && right.Defined()) {
                return TJsonNodes({TValue(MakeBool(*left || *right))});
            }

            const bool trueVsNull = left.GetOrElse(false) && !right.Defined();
            const bool nullVsTrue = right.GetOrElse(false) && !left.Defined();
            if (trueVsNull || nullVsTrue) {
                return TJsonNodes({TValue(MakeBool(true))});
            }
            return TJsonNodes({TValue(MakeEntity())});
        }
        default:
            YQL_ENSURE(false, "Expected binary logical operation");
    }
}

TResult TExecutor::UnaryLogicalOp(const TJsonPathItem& item) {
    /*
        NOT truth table (taken from SQL JSON standard)

        |   x   |   !x  |
        | ----- | ----- |
        | true  | false |
        | false | true  |
        | null  | null  |
    */
    const auto& operandItem = Reader.ReadInput(item);
    const auto operandResult = Execute(operandItem);
    if (operandResult.IsError()) {
        return operandResult;
    }

    TMaybe<bool> operand;
    TMaybe<TIssue> error = EnsureLogicalOpArgument(operandItem.Pos, operandResult.GetNodes(), operand);
    if (error) {
        return std::move(*error);
    }

    if (!operand.Defined()) {
        return TJsonNodes({TValue(MakeEntity())});
    }

    return TJsonNodes({TValue(MakeBool(!(*operand)))});
}

TResult TExecutor::BooleanLiteral(const TJsonPathItem& item) {
    return TJsonNodes({TValue(MakeBool(item.GetBoolean()))});
}

TResult TExecutor::NullLiteral() {
    return TJsonNodes({TValue(MakeEntity())});
}

TResult TExecutor::StringLiteral(const TJsonPathItem& item) {
    return TJsonNodes({TValue(MakeString(item.GetString(), ValueBuilder))});
}

TMaybe<bool> TExecutor::CompareValues(const TValue& left, const TValue& right, EJsonPathItemType operation) {
    if (IsObjectOrArray(left) || IsObjectOrArray(right)) {
        // Comparisons of objects and arrays are prohibited
        return Nothing();
    }

    if (left.IsNull() && right.IsNull()) {
        // null == null is true, but all other comparisons are false
        return operation == EJsonPathItemType::BinaryEqual;
    }

    if (left.IsNull() || right.IsNull()) {
        // All operations between null and non-null are false
        return false;
    }

    auto doCompare = [&operation](const auto& left, const auto& right) {
        switch (operation) {
            case EJsonPathItemType::BinaryEqual:
                return left == right;
            case EJsonPathItemType::BinaryNotEqual:
                return left != right;
            case EJsonPathItemType::BinaryLess:
                return left < right;
            case EJsonPathItemType::BinaryLessEqual:
                return left <= right;
            case EJsonPathItemType::BinaryGreater:
                return left > right;
            case EJsonPathItemType::BinaryGreaterEqual:
                return left >= right;
            default:
                YQL_ENSURE(false, "Expected compare operation");
        }
    };

    if (left.IsBool() && right.IsBool()) {
        return doCompare(left.GetBool(), right.GetBool());
    } else if (left.IsString() && right.IsString()) {
        // NOTE: Strings are compared as byte arrays.
        //       YQL does the same thing for UTF-8 strings and according to SQL/JSON
        //       standard JsonPath must use the same semantics.
        //
        //       However this is not correct in logical meaning. Let us consider strings:
        //        - U+00e9 (LATIN SMALL LETTER E WITH ACUTE), 'é'
        //        - U+0065 (LATIN SMALL LETTER E) U+0301 (COMBINING ACUTE ACCENT), `é`
        //       Even though these two strings are different byte sequences, they are identical
        //       from UTF-8 perspective.
        return doCompare(left.GetString(), right.GetString());
    }

    if (!left.IsNumber() || !right.IsNumber()) {
        return Nothing();
    }

    const auto leftNumber = left.GetNumber();
    const auto rightNumber = right.GetNumber();
    switch (operation) {
        case EJsonPathItemType::BinaryEqual:
            return IsEqual(leftNumber, rightNumber);
        case EJsonPathItemType::BinaryNotEqual:
            return !IsEqual(leftNumber, rightNumber);
        case EJsonPathItemType::BinaryLess:
            return IsLess(leftNumber, rightNumber);
        case EJsonPathItemType::BinaryLessEqual:
            return !IsGreater(leftNumber, rightNumber);
        case EJsonPathItemType::BinaryGreater:
            return IsGreater(leftNumber, rightNumber);
        case EJsonPathItemType::BinaryGreaterEqual:
            return !IsLess(leftNumber, rightNumber);
        default:
            YQL_ENSURE(false, "Expected compare operation");
    }
}

TResult TExecutor::CompareOp(const TJsonPathItem& item) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto leftResult = Execute(leftItem);
    if (leftResult.IsError()) {
        return TJsonNodes({TValue(MakeEntity())});
    }

    const auto& rightItem = Reader.ReadRightOperand(item);
    const auto rightResult = Execute(rightItem);
    if (rightResult.IsError()) {
        return TJsonNodes({TValue(MakeEntity())});
    }

    const auto leftNodes = OptionalUnwrapArrays(leftResult.GetNodes());
    const auto rightNodes = OptionalUnwrapArrays(rightResult.GetNodes());
    bool error = false;
    bool found = false;
    for (const auto& left : leftNodes) {
        for (const auto& right : rightNodes) {
            const auto result = CompareValues(left, right, item.Type);
            if (!result.Defined()) {
                error = true;
            } else {
                found |= *result;
            }

            if (IsLax() && (error || found)) {
                break;
            }
        }

        if (IsLax() && (error || found)) {
            break;
        }
    }

    if (error) {
        return TJsonNodes({TValue(MakeEntity())});
    }
    return TJsonNodes({TValue(MakeBool(found))});
}

TResult TExecutor::FilterObject(const TJsonPathItem& item) {
    if (CurrentFilterObject.empty()) {
        return MakeError(item, TIssuesIds::JSONPATH_FILTER_OBJECT_OUTSIDE_OF_FILTER, "'@' is only allowed inside filters");
    }

    return TJsonNodes({CurrentFilterObject.top()});
}

TResult TExecutor::FilterPredicate(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    const auto& predicateItem = Reader.ReadFilterPredicate(item);
    TJsonNodes result;
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        CurrentFilterObject.push(node);
        Y_DEFER {
            CurrentFilterObject.pop();
        };

        const auto predicateResult = Execute(predicateItem);
        if (predicateResult.IsError()) {
            continue;
        }

        const auto& predicateNodes = predicateResult.GetNodes();
        if (predicateNodes.size() != 1) {
            continue;
        }

        const auto& value = predicateNodes[0];
        if (value.IsBool() && value.GetBool()) {
            result.push_back(node);
            continue;
        }
    }
    return std::move(result);
}

TResult TExecutor::NumericMethod(const TJsonPathItem& item) {
    const auto& input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        if (!node.IsNumber()) {
            return MakeError(item, TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT, "Unsupported type for numeric method");
        }

        double applied = node.GetNumber();
        switch (item.Type) {
            case EJsonPathItemType::AbsMethod:
                applied = std::fabs(applied);
                break;
            case EJsonPathItemType::FloorMethod:
                applied = std::floor(applied);
                break;
            case EJsonPathItemType::CeilingMethod:
                applied = std::ceil(applied);
                break;
            default:
                YQL_ENSURE(false, "Expected numeric method");
        }
        result.push_back(TValue(MakeDouble(applied)));
    }
    return std::move(result);
}

TResult TExecutor::DoubleMethod(const TJsonPathItem& item) {
    const auto& input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        if (!node.IsString()) {
            return MakeError(item, TIssuesIds::JSONPATH_INVALID_DOUBLE_METHOD_ARGUMENT, "Unsupported type for double() method");
        }

        const double parsed = ParseDouble(node.GetString());
        if (std::isnan(parsed)) {
            return MakeError(item, TIssuesIds::JSONPATH_INVALID_NUMBER_STRING, "Error parsing number from string");
        }

        if (std::isinf(parsed)) {
            return MakeError(item, TIssuesIds::JSONPATH_INFINITE_NUMBER_STRING, "Parsed number is infinity");
        }

        result.push_back(TValue(MakeDouble(parsed)));
    }
    return std::move(result);
}

TResult TExecutor::TypeMethod(const TJsonPathItem& item) {
    const auto& input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : input.GetNodes()) {
        TStringBuf type;
        switch (node.GetType()) {
            case EValueType::Null:
                type = "null";
                break;
            case EValueType::Bool:
                type = "boolean";
                break;
            case EValueType::Number:
                type = "number";
                break;
            case EValueType::String:
                type = "string";
                break;
            case EValueType::Array:
                type = "array";
                break;
            case EValueType::Object:
                type = "object";
                break;
        }
        result.push_back(TValue(MakeString(type, ValueBuilder)));
    }
    return std::move(result);
}

TResult TExecutor::SizeMethod(const TJsonPathItem& item) {
    const auto& input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    for (const auto& node : input.GetNodes()) {
        ui64 size = 1;
        if (node.IsArray()) {
            size = node.GetSize();
        }
        result.push_back(TValue(MakeDouble(static_cast<double>(size))));
    }
    return std::move(result);
}

TResult TExecutor::KeyValueMethod(const TJsonPathItem& item) {
    const auto& input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }
    TJsonNodes result;
    TPair row[2];
    TPair& nameEntry = row[0];
    TPair& valueEntry = row[1];
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        if (!node.IsObject()) {
            return MakeError(item, TIssuesIds::JSONPATH_INVALID_KEYVALUE_METHOD_ARGUMENT, "Unsupported type for keyvalue() method");
        }

        TValue key;
        TValue value;
        auto it = node.GetObjectIterator();
        while (it.Next(key, value)) {
            nameEntry.first = MakeString("name", ValueBuilder);
            nameEntry.second = key.ConvertToUnboxedValue(ValueBuilder);

            valueEntry.first = MakeString("value", ValueBuilder);
            valueEntry.second = value.ConvertToUnboxedValue(ValueBuilder);

            result.push_back(TValue(MakeDict(row, 2)));
        }
    }
    return std::move(result);
}

TResult TExecutor::StartsWithPredicate(const TJsonPathItem& item) {
    const auto& input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }

    const auto& inputNodes = input.GetNodes();
    if (inputNodes.size() != 1) {
        return MakeError(item, TIssuesIds::JSONPATH_INVALID_STARTS_WITH_ARGUMENT, "Expected exactly 1 item as input argument for starts with predicate");
    }

    const auto& inputString = inputNodes[0];
    if (!inputString.IsString()) {
        return MakeError(item, TIssuesIds::JSONPATH_INVALID_STARTS_WITH_ARGUMENT, "Type of input argument for starts with predicate must be string");
    }

    const auto prefix = Execute(Reader.ReadPrefix(item));
    if (prefix.IsError()) {
        return prefix;
    }

    bool error = false;
    bool found = false;
    for (const auto& node : prefix.GetNodes()) {
        if (node.IsString()) {
            found |= inputString.GetString().StartsWith(node.GetString());
        } else {
            error = true;
        }

        if (IsLax() && (found || error)) {
            break;
        }
    }

    if (error) {
        return TJsonNodes({TValue(MakeEntity())});
    }
    return TJsonNodes({TValue(MakeBool(found))});
}

TResult TExecutor::IsUnknownPredicate(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }

    const auto& nodes = input.GetNodes();
    if (nodes.size() != 1) {
        return MakeError(item, TIssuesIds::JSONPATH_INVALID_IS_UNKNOWN_ARGUMENT, "Expected exactly 1 item as an argument for is unknown predicate");
    }

    const auto& node = nodes[0];
    if (node.IsNull()) {
        return TJsonNodes({TValue(MakeBool(true))});
    }

    if (!node.IsBool()) {
        return MakeError(item, TIssuesIds::JSONPATH_INVALID_IS_UNKNOWN_ARGUMENT, "is unknown predicate supports only bool and null types for its argument");
    }
    return TJsonNodes({TValue(MakeBool(false))});
}

TResult TExecutor::ExistsPredicate(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return TJsonNodes({TValue(MakeEntity())});
    }

    const auto& nodes = input.GetNodes();
    return TJsonNodes({TValue(MakeBool(!nodes.empty()))});
}

TResult TExecutor::LikeRegexPredicate(const TJsonPathItem& item) {
    const auto input = Execute(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }

    const auto& regex = item.GetRegex();
    bool error = false;
    bool found = false;
    for (const auto& node : OptionalUnwrapArrays(input.GetNodes())) {
        if (node.IsString()) {
            found |= regex->Matches(node.GetString());
        } else {
            error = true;
        }

        if (IsLax() && (found || error)) {
            break;
        }
    }

    if (error) {
        return TJsonNodes({TValue(MakeEntity())});
    }
    return TJsonNodes({TValue(MakeBool(found))});
}

TJsonNodes TExecutor::OptionalUnwrapArrays(const TJsonNodes& input) {
    if (IsStrict()) {
        return input;
    }

    TJsonNodes result;
    for (const auto& node : input) {
        if (!node.IsArray()) {
            result.push_back(node);
            continue;
        }

        auto it = node.GetArrayIterator();
        TValue value;
        while (it.Next(value)) {
            result.push_back(value);
        }
    }
    return result;
}

TJsonNodes TExecutor::OptionalArrayWrapNodes(const TJsonNodes& input) {
    if (IsStrict()) {
        return input;
    }

    TJsonNodes result;
    for (const auto& node : input) {
        if (node.IsArray()) {
            result.push_back(node);
            continue;
        }

        TUnboxedValue nodeCopy(node.ConvertToUnboxedValue(ValueBuilder));
        result.push_back(TValue(MakeList(&nodeCopy, 1, ValueBuilder)));
    }
    return result;
}

}

