#include "range_inferrer.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TKeyTriePtr ExtractMultipleConstraints(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer,
    const TConstRangeExtractorMapPtr& rangeExtractors)
{
    if (!expr) {
        return TKeyTrie::Universal();
    }

    if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            return IntersectKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer, rangeExtractors),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer, rangeExtractors));
        } if (opcode == EBinaryOp::Or) {
            return UniteKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer, rangeExtractors),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer, rangeExtractors));
        } else {
            if (rhsExpr->As<TReferenceExpression>()) {
                // Ensure that references are on the left.
                std::swap(lhsExpr, rhsExpr);
                opcode = GetReversedBinaryOpcode(opcode);
            }

            auto referenceExpr = lhsExpr->As<TReferenceExpression>();
            auto constantExpr = rhsExpr->As<TLiteralExpression>();

            auto result = TKeyTrie::Universal();

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    auto value = TValue(constantExpr->Value);

                    result = New<TKeyTrie>(0);

                    auto& bounds = result->Bounds;
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            result->Offset = keyPartIndex;
                            result->Next.emplace_back(value, TKeyTrie::Universal());
                            break;
                        case EBinaryOp::NotEqual:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        case EBinaryOp::Less:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);

                            break;
                        case EBinaryOp::LessOrEqual:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, true);

                            break;
                        case EBinaryOp::Greater:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        case EBinaryOp::GreaterOrEqual:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(value, true);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        default:
                            break;
                    }
                }
            }

            return result;
        }
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        auto found = rangeExtractors->find(functionExpr->FunctionName);
        if (found == rangeExtractors->end()) {
            return TKeyTrie::Universal();
        }

        auto rangeExtractor = found->second;

        return rangeExtractor(
            functionExpr,
            keyColumns,
            rowBuffer);
    } else if (auto inExpr = expr->As<TInExpression>()) {
        int argsSize = inExpr->Arguments.size();

        std::vector<int> keyMapping(keyColumns.size(), -1);
        for (int index = 0; index < argsSize; ++index) {
            auto referenceExpr = inExpr->Arguments[index]->As<TReferenceExpression>();
            if (referenceExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0 && keyMapping[keyPartIndex] == -1) {
                    keyMapping[keyPartIndex] = index;
                }
            }
        }

        std::vector<TKeyTriePtr> keyTries;
        for (int rowIndex = 0; rowIndex < std::ssize(inExpr->Values); ++rowIndex) {
            auto literalTuple = inExpr->Values[rowIndex];

            auto rowConstraint = TKeyTrie::Universal();
            for (int keyIndex = keyMapping.size() - 1; keyIndex >= 0; --keyIndex) {
                auto index = keyMapping[keyIndex];
                if (index >= 0) {
                    auto valueConstraint = New<TKeyTrie>(keyIndex);
                    valueConstraint->Next.emplace_back(literalTuple[index], std::move(rowConstraint));
                    rowConstraint = std::move(valueConstraint);
                }
            }

            keyTries.push_back(rowConstraint);
        }

        return UniteKeyTrie(keyTries);
    } else if (auto betweenExpr = expr->As<TBetweenExpression>()) {
        int argsSize = betweenExpr->Arguments.size();

        std::vector<int> keyMapping(keyColumns.size(), -1);
        for (int index = 0; index < argsSize; ++index) {
            auto referenceExpr = betweenExpr->Arguments[index]->As<TReferenceExpression>();
            if (referenceExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0 && keyMapping[keyPartIndex] == -1) {
                    keyMapping[keyPartIndex] = index;
                }
            }
        }

        std::vector<TKeyTriePtr> keyTries;
        for (int rowIndex = 0; rowIndex < std::ssize(betweenExpr->Ranges); ++rowIndex) {
            auto literalRange = betweenExpr->Ranges[rowIndex];

            auto lower = literalRange.first;
            auto upper = literalRange.second;

            size_t prefix = 0;
            while (prefix < lower.GetCount() && prefix < upper.GetCount() && lower[prefix] == upper[prefix]) {
                ++prefix;
            }

            int rangeColumnIndex = -1;
            auto rowConstraint = TKeyTrie::Universal();
            for (int keyIndex = keyMapping.size() - 1; keyIndex >= 0; --keyIndex) {
                auto index = keyMapping[keyIndex];
                if (index >= 0 && index < static_cast<int>(prefix)) {
                    auto valueConstraint = New<TKeyTrie>(keyIndex);
                    valueConstraint->Next.emplace_back(lower[index], std::move(rowConstraint));
                    rowConstraint = std::move(valueConstraint);
                }

                if (index == static_cast<int>(prefix)) {
                    rangeColumnIndex = keyIndex;
                }
            }

            if (rangeColumnIndex != -1) {
                auto rangeConstraint = New<TKeyTrie>(rangeColumnIndex);
                auto& bounds = rangeConstraint->Bounds;

                bounds.emplace_back(
                    lower.GetCount() > prefix
                        ? lower[prefix]
                        : MakeUnversionedSentinelValue(EValueType::Min),
                    true);

                bounds.emplace_back(
                    upper.GetCount() > prefix
                        ? upper[prefix]
                        : MakeUnversionedSentinelValue(EValueType::Max),
                    true);

                rowConstraint = IntersectKeyTrie(rowConstraint, rangeConstraint);
            }

            keyTries.push_back(rowConstraint);
        }

        return UniteKeyTrie(keyTries);
    } else if (auto literalExpr = expr->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean ? TKeyTrie::Universal() : TKeyTrie::Empty();
        }
    }

    return TKeyTrie::Universal();
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TRangeInferrer CreateRangeInferrer(
    TConstExpressionPtr /*predicate*/,
    const TTableSchemaPtr& /*schema*/,
    const TKeyColumns& /*keyColumns*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstRangeExtractorMapPtr& /*rangeExtractors*/,
    const TQueryOptions& /*options*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/range_inferrer.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
