#include "expr_builder_base.h"
#include "functions.h"
#include "helpers.h"
#include "query_helpers.h"

#include "query_visitors.h"

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;

using NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

EValueType GetType(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return EValueType::Null;
        },
        [] (i64) {
            return EValueType::Int64;
        },
        [] (ui64) {
            return EValueType::Uint64;
        },
        [] (double) {
            return EValueType::Double;
        },
        [] (bool) {
            return EValueType::Boolean;
        },
        [] (const std::string&) {
            return EValueType::String;
        });
}

TValue GetValue(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return MakeUnversionedSentinelValue(EValueType::Null);
        },
        [] (i64 value) {
            return MakeUnversionedInt64Value(value);
        },
        [] (ui64 value) {
            return MakeUnversionedUint64Value(value);
        },
        [] (double value) {
            return MakeUnversionedDoubleValue(value);
        },
        [] (bool value) {
            return MakeUnversionedBooleanValue(value);
        },
        [] (const std::string& value) {
            return MakeUnversionedStringValue(value);
        });
}

void BuildRow(
    TUnversionedRowBuilder* rowBuilder,
    const NAst::TLiteralValueTuple& tuple,
    const std::vector<EValueType>& argTypes,
    TStringBuf source)
{
    for (int i = 0; i < std::ssize(tuple); ++i) {
        auto valueType = GetType(tuple[i]);
        auto value = GetValue(tuple[i]);

        if (valueType == EValueType::Null) {
            value = MakeUnversionedSentinelValue(EValueType::Null);
        } else if (valueType != argTypes[i]) {
            if (IsArithmeticType(valueType) && IsArithmeticType(argTypes[i])) {
                value = CastValueWithCheck(value, argTypes[i]);
            } else {
                THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                    .With("source", source)
                    .With("actual_type", valueType)
                    .With("expected_type", argTypes[i]);
            }
        }
        rowBuilder->AddValue(value);
    }
}

TSharedRange<TRow> LiteralTupleListToRows(
    const NAst::TLiteralValueTupleList& literalTuples,
    const std::vector<EValueType>& argTypes,
    TStringBuf source)
{
    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRow> rows;
    rows.reserve(literalTuples.size());
    for (const auto& tuple : literalTuples) {
        if (tuple.size() != argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                .With("source", source);
        }

        BuildRow(&rowBuilder, tuple, argTypes, source);

        rows.push_back(rowBuffer->CaptureRow(rowBuilder.GetRow()));
        rowBuilder.Reset();
    }

    std::sort(rows.begin(), rows.end());
    return MakeSharedRange(std::move(rows), std::move(rowBuffer));
}

TSharedRange<TRowRange> LiteralRangesListToRows(
    const NAst::TLiteralValueRangeList& literalRanges,
    const std::vector<EValueType>& argTypes,
    TStringBuf source)
{
    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRowRange> ranges;
    for (const auto& range : literalRanges) {
        if (range.first.size() > argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                .With("source", source);
        }

        if (range.second.size() > argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                .With("source", source);
        }

        BuildRow(&rowBuilder, range.first, argTypes, source);
        auto lower = rowBuffer->CaptureRow(rowBuilder.GetRow());
        rowBuilder.Reset();

        BuildRow(&rowBuilder, range.second, argTypes, source);
        auto upper = rowBuffer->CaptureRow(rowBuilder.GetRow());
        rowBuilder.Reset();

        if (CompareRows(lower, upper, std::min(lower.GetCount(), upper.GetCount())) > 0) {
            THROW_ERROR_EXCEPTION("Lower bound is greater than upper")
                .With("lower", lower)
                .With("upper", upper);
        }

        ranges.emplace_back(lower, upper);
    }

    std::sort(ranges.begin(), ranges.end());

    for (int index = 1; index < std::ssize(ranges); ++index) {
        TRow previousUpper = ranges[index - 1].second;
        TRow currentLower = ranges[index].first;

        if (CompareRows(
            previousUpper,
            currentLower,
            std::min(previousUpper.GetCount(), currentLower.GetCount())) >= 0)
        {
            THROW_ERROR_EXCEPTION("Ranges are not disjoint")
                .With("first", ranges[index - 1])
                .With("second", ranges[index]);
        }
    }

    return MakeSharedRange(std::move(ranges), std::move(rowBuffer));
}

////////////////////////////////////////////////////////////////////////////////

struct TNotExpressionPropagator
    : public TRewriter<TNotExpressionPropagator>
{
    using TBase = TRewriter<TNotExpressionPropagator>;

    TConstExpressionPtr OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        auto& operand = unaryExpr->Operand;
        if (unaryExpr->Opcode == EUnaryOp::Not) {
            if (auto operandUnaryOp = operand->As<TUnaryOpExpression>()) {
                if (operandUnaryOp->Opcode == EUnaryOp::Not) {
                    return Visit(operandUnaryOp->Operand);
                }
            } else if (auto operandBinaryOp = operand->As<TBinaryOpExpression>()) {
                if (operandBinaryOp->Opcode == EBinaryOp::And) {
                    return Visit(New<TBinaryOpExpression>(
                        EValueType::Boolean,
                        EBinaryOp::Or,
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Lhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Lhs),
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Rhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Rhs)));
                } else if (operandBinaryOp->Opcode == EBinaryOp::Or) {
                    return Visit(New<TBinaryOpExpression>(
                        EValueType::Boolean,
                        EBinaryOp::And,
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Lhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Lhs),
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Rhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Rhs)));
                } else if (IsRelationalBinaryOp(operandBinaryOp->Opcode)) {
                    return Visit(New<TBinaryOpExpression>(
                        operandBinaryOp->GetWireType(),
                        GetInversedBinaryOpcode(operandBinaryOp->Opcode),
                        operandBinaryOp->Lhs,
                        operandBinaryOp->Rhs));
                }
            } else if (auto literal = operand->As<TLiteralExpression>()) {
                TValue value = literal->Value;
                value.Data.Boolean = !value.Data.Boolean;
                return New<TLiteralExpression>(
                    literal->GetWireType(),
                    value);
            }
        }

        return TBase::OnUnary(unaryExpr);
    }
};

struct TCastEliminator
    : public TRewriter<TCastEliminator>
{
    using TBase = TRewriter<TCastEliminator>;

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        if (IsUserCastFunction(functionExpr->FunctionName)) {
            YT_VERIFY(functionExpr->Arguments.size() == 1);

            if (*functionExpr->LogicalType == *functionExpr->Arguments[0]->LogicalType) {
                return Visit(functionExpr->Arguments[0]);
            }
        }

        if (functionExpr->FunctionName == "yson_string_to_any") {
            YT_VERIFY(functionExpr->Arguments.size() == 1);
            if (auto* literal = functionExpr->Arguments[0]->As<TLiteralExpression>()) {
                YT_VERIFY(literal->Value.Type() == EValueType::String);
                auto asUnversionedValue = TValue(literal->Value);
                asUnversionedValue.Type = EValueType::Any;
                ValidateYson(TYsonStringBuf(asUnversionedValue.AsStringBuf()), /*nestingLevelLimit*/ 256);
                return New<TLiteralExpression>(EValueType::Any, TOwningValue(asUnversionedValue));
            }
        }

        return TBase::OnFunction(functionExpr);
    }
};

struct TExpressionSimplifier
    : public TRewriter<TExpressionSimplifier>
{
    using TBase = TRewriter<TExpressionSimplifier>;

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        if (functionExpr->FunctionName == "if") {
            if (auto functionCondition = functionExpr->Arguments[0]->As<TFunctionExpression>()) {
                auto reference1 = functionExpr->Arguments[2]->As<TReferenceExpression>();
                if (functionCondition->FunctionName == "is_null" && reference1) {
                    auto reference0 = functionCondition->Arguments[0]->As<TReferenceExpression>();
                    if (reference0 && reference1->ColumnName == reference0->ColumnName) {
                        return New<TFunctionExpression>(
                            functionExpr->GetWireType(),
                            "if_null",
                            std::vector<TConstExpressionPtr>{
                                functionCondition->Arguments[0],
                                functionExpr->Arguments[1],
                            });
                    }
                }
            }
        }

        return TBase::OnFunction(functionExpr);
    }
};

TConstExpressionPtr ApplyRewriters(TConstExpressionPtr expr)
{
    expr = TCastEliminator().Visit(expr);
    expr = TExpressionSimplifier().Visit(expr);
    expr = TNotExpressionPropagator().Visit(expr);
    return expr;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<NAst::TReference> TryReinterpretAsMemberAccess(
    const NAst::TReference& reference)
{
    if (!reference.TableName) {
        return std::nullopt;
    }

    NAst::TReference result(*reference.TableName);

    auto& accessor = result.CompositeTypeAccessor.NestedStructOrTupleItemAccessor;
    accessor.reserve(reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor.size() + 1);

    accessor.push_back(TStructMemberAccessor(reference.ColumnName));
    accessor.insert(
        accessor.end(),
        reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor.begin(),
        reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor.end());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TValue> FoldConstants(
    EUnaryOp opcode,
    const TConstExpressionPtr& operand)
{
    if (auto literalExpr = operand->As<TLiteralExpression>()) {
        if (opcode == EUnaryOp::Plus) {
            return static_cast<TValue>(literalExpr->Value);
        } else if (opcode == EUnaryOp::Minus) {
            TValue value = literalExpr->Value;
            switch (value.Type) {
                case EValueType::Int64:
                    value.Data.Int64 = -value.Data.Int64;
                    break;
                case EValueType::Uint64:
                    value.Data.Uint64 = -value.Data.Uint64;
                    break;
                case EValueType::Double:
                    value.Data.Double = -value.Data.Double;
                    break;
                case EValueType::Null:
                    break;
                default:
                    YT_ABORT();
            }
            return value;
        } else if (opcode == EUnaryOp::BitNot) {
            TValue value = literalExpr->Value;
            switch (value.Type) {
                case EValueType::Int64:
                    value.Data.Int64 = ~value.Data.Int64;
                    break;
                case EValueType::Uint64:
                    value.Data.Uint64 = ~value.Data.Uint64;
                    break;
                case EValueType::Null:
                    break;
                default:
                    YT_ABORT();
            }
            return value;
        }
    }
    return std::nullopt;
}

std::optional<TValue> FoldConstants(
    EBinaryOp opcode,
    const TConstExpressionPtr& lhsExpr,
    const TConstExpressionPtr& rhsExpr)
{
    auto lhsLiteral = lhsExpr->As<TLiteralExpression>();
    auto rhsLiteral = rhsExpr->As<TLiteralExpression>();
    if (lhsLiteral && rhsLiteral) {
        auto lhs = static_cast<TValue>(lhsLiteral->Value);
        auto rhs = static_cast<TValue>(rhsLiteral->Value);

        auto checkType = [&] {
            if (lhs.Type != rhs.Type) {
                if (IsArithmeticType(lhs.Type) && IsArithmeticType(rhs.Type)) {
                    auto targetType = std::max(lhs.Type, rhs.Type);
                    lhs = CastValueWithCheck(lhs, targetType);
                    rhs = CastValueWithCheck(rhs, targetType);
                } else {
                    ThrowTypeMismatchError(lhs.Type, rhs.Type, "", InferName(lhsExpr), InferName(rhsExpr));
                }
            }
        };

        auto checkTypeIfNotNull = [&] {
            if (lhs.Type != EValueType::Null && rhs.Type != EValueType::Null) {
                checkType();
            }
        };

        #define CHECK_TYPE() \
            if (lhs.Type == EValueType::Null) { \
                return MakeUnversionedSentinelValue(EValueType::Null); \
            } \
            if (rhs.Type == EValueType::Null) { \
                return MakeUnversionedSentinelValue(EValueType::Null); \
            } \
            checkType();

        auto evaluateLogicalOp = [&] (bool parameter) {
            YT_VERIFY(lhs.Type == EValueType::Null || lhs.Type == EValueType::Boolean);
            YT_VERIFY(rhs.Type == EValueType::Null || rhs.Type == EValueType::Boolean);

            if (lhs.Type == EValueType::Null) {
                if (rhs.Type != EValueType::Null && rhs.Data.Boolean == parameter) {
                    return rhs;
                } else {
                    return lhs;
                }
            } else if (lhs.Data.Boolean == parameter) {
                return lhs;
            } else {
                return rhs;
            }
        };

        switch (opcode) {
            case EBinaryOp::Plus:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                        lhs.Data.Int64 += rhs.Data.Int64;
                        return lhs;
                    case EValueType::Uint64:
                        lhs.Data.Uint64 += rhs.Data.Uint64;
                        return lhs;
                    case EValueType::Double:
                        lhs.Data.Double += rhs.Data.Double;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::Minus:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                        lhs.Data.Int64 -= rhs.Data.Int64;
                        return lhs;
                    case EValueType::Uint64:
                        lhs.Data.Uint64 -= rhs.Data.Uint64;
                        return lhs;
                    case EValueType::Double:
                        lhs.Data.Double -= rhs.Data.Double;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::Concatenate:
                break;
            case EBinaryOp::Multiply:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                        lhs.Data.Int64 *= rhs.Data.Int64;
                        return lhs;
                    case EValueType::Uint64:
                        lhs.Data.Uint64 *= rhs.Data.Uint64;
                        return lhs;
                    case EValueType::Double:
                        lhs.Data.Double *= rhs.Data.Double;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::Divide:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                        if (rhs.Data.Int64 == 0) {
                            THROW_ERROR_EXCEPTION("Division by zero");
                        }

                        if (lhs.Data.Int64 == std::numeric_limits<i64>::min() && rhs.Data.Int64 == -1) {
                            THROW_ERROR_EXCEPTION("Division of INT_MIN by -1");
                        }

                        lhs.Data.Int64 /= rhs.Data.Int64;
                        return lhs;
                    case EValueType::Uint64:
                        if (rhs.Data.Uint64 == 0) {
                            THROW_ERROR_EXCEPTION("Division by zero");
                        }
                        lhs.Data.Uint64 /= rhs.Data.Uint64;
                        return lhs;
                    case EValueType::Double:
                        lhs.Data.Double /= rhs.Data.Double;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::Modulo:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                    case EValueType::Uint64:
                        return EvaluateModulo(lhs, rhs);
                    default:
                        break;
                }
                break;
            case EBinaryOp::LeftShift:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                        lhs.Data.Int64 <<= rhs.Data.Int64;
                        return lhs;
                    case EValueType::Uint64:
                        lhs.Data.Uint64 <<= rhs.Data.Uint64;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::RightShift:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Int64:
                        lhs.Data.Int64 >>= rhs.Data.Int64;
                        return lhs;
                    case EValueType::Uint64:
                        lhs.Data.Uint64 >>= rhs.Data.Uint64;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::BitOr:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Uint64:
                        lhs.Data.Uint64 = lhs.Data.Uint64 | rhs.Data.Uint64;
                        return lhs;
                    case EValueType::Int64:
                        lhs.Data.Int64 = lhs.Data.Int64 | rhs.Data.Int64;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::BitAnd:
                CHECK_TYPE();
                switch (lhs.Type) {
                    case EValueType::Uint64:
                        lhs.Data.Uint64 = lhs.Data.Uint64 & rhs.Data.Uint64;
                        return lhs;
                    case EValueType::Int64:
                        lhs.Data.Int64 = lhs.Data.Int64 & rhs.Data.Int64;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::And:
                return evaluateLogicalOp(false);
                break;
            case EBinaryOp::Or:
                return evaluateLogicalOp(true);
                break;
            case EBinaryOp::Equal:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) == 0);
                break;
            case EBinaryOp::NotEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) != 0);
                break;
            case EBinaryOp::Less:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) < 0);
                break;
            case EBinaryOp::Greater:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) > 0);
                break;
            case EBinaryOp::LessOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) <= 0);
                break;
            case EBinaryOp::GreaterOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) >= 0);
                break;
            default:
                break;
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

void TReferenceResolver::AddTable(TNameSource nameSource)
{
    NameSources_.push_back(std::move(nameSource));
}

TLogicalTypePtr TReferenceResolver::Resolve(const NAst::TColumnReference& reference)
{
    TLogicalTypePtr type;
    int sourceIndex = 0;

    auto lookupIt = Lookup_.find(reference);
    if (lookupIt != Lookup_.end()) {
        type = lookupIt->second.Type;
        sourceIndex = lookupIt->second.SourceIndex;
    }

    for (; sourceIndex < std::ssize(NameSources_); ++sourceIndex) {
        const auto& [schema, alias, mapping, selfColumns, foreignColumns, sharedColumns, _] = NameSources_[sourceIndex];

        auto formattedName = InferColumnName(reference);

        if (type && selfColumns) {
            selfColumns->insert(formattedName);
        }

        if (alias == reference.TableName) {
            if (auto* column = schema.FindColumn(reference.ColumnName)) {
                if (type) {
                    if (!sharedColumns.contains(formattedName)) {
                        THROW_ERROR_EXCEPTION("Ambiguous resolution for column %Qv",
                            formattedName);
                    }
                } else {
                    if (mapping) {
                        mapping->push_back(TColumnDescriptor{
                            formattedName,
                            schema.GetColumnIndex(*column)
                        });
                    }

                    if (foreignColumns) {
                        foreignColumns->insert(formattedName);
                    }

                    type = column->LogicalType();
                }
            }
        }
    }

    if (type) {
        lookupIt = Lookup_.emplace(reference, TResolvedInfo{type, 0}).first;
        lookupIt->second.SourceIndex = std::ssize(NameSources_);
    }

    return type;
}

void TReferenceResolver::PopulateAllColumns()
{
    for (const auto& nameSource : NameSources_) {
        for (const auto& column : nameSource.Schema.Columns()) {
            Resolve(NAst::TReference(column.Name(), nameSource.Alias));
        }
    }
}

void TReferenceResolver::Finish()
{
    for (const auto& nameSource : NameSources_) {
        if (nameSource.Mapping) {
            std::sort(nameSource.Mapping->begin(), nameSource.Mapping->end(),
                [] (const TColumnDescriptor& lhs, const TColumnDescriptor& rhs) {
                    return lhs.Index < rhs.Index;
                });

            // Repeated columns can occur if column is used in join equation and further in query.
            nameSource.Mapping->erase(std::unique(
                nameSource.Mapping->begin(),
                nameSource.Mapping->end(),
                [] (const TColumnDescriptor& lhs, const TColumnDescriptor& rhs) {
                    return lhs.Index == rhs.Index;
                }), nameSource.Mapping->end());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpressionBuilder::TExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const TPreparePlanFragmentContext& context)
    : Source_(source)
    , Functions_(functions)
    , Context_(context)
{ }

const NLogging::TLogger& TExpressionBuilder::GetLogger() const
{
    return Context_.Logger;
}

const TPreparePlanFragmentContext& TExpressionBuilder::GetContext() const
{
    return Context_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
