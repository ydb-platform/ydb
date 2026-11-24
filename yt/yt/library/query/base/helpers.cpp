#include "helpers.h"
#include "private.h"
#include "query.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NQueryClient {

using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

#ifdef _asan_enabled_
static const int MinimumStackFreeSpace = 128_KB;
#else
static const int MinimumStackFreeSpace = 16_KB;
#endif

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth()
{
    if (!NConcurrency::CheckFreeStackSpace(MinimumStackFreeSpace)) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::QueryExpressionDepthLimitExceeded,
            "Expression depth causes stack overflow");
    }
}

////////////////////////////////////////////////////////////////////////////////

TLogger MakeQueryLogger(TGuid queryId)
{
    return QueryClientLogger().WithTag("FragmentId", queryId);
}

TLogger MakeQueryLogger(TConstBaseQueryPtr query)
{
    return MakeQueryLogger(query->Id);
}

////////////////////////////////////////////////////////////////////////////////

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    TStringBuf source,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv", source)
        .With("lhs_source", lhsSource)
        .With("rhs_source", rhsSource)
        .With("lhs_type", lhsType)
        .With("rhs_type", rhsType);
}

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(TRange<std::string> keyColumns, const std::string& columnName)
{
    for (int index = 0; index < std::ssize(keyColumns); ++index) {
        if (keyColumns[index] == columnName) {
            return index;
        }
    }
    return -1;
}

TLogicalTypePtr ToQLType(const TLogicalTypePtr& columnType)
{
    if (IsV1Type(columnType)) {
        const auto wireType = GetWireType(columnType);
        return MakeLogicalType(GetLogicalType(wireType), !columnType->IsNullable());
    } else {
        return columnType;
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillExpressionConstraintSignature(
    TMutableRange<EConstraintKind> signature,
    const TConstExpressionPtr& expression,
    const TKeyColumns& keyColumns)
{
    YT_VERIFY(signature.size() == keyColumns.size());

    if (const auto* binaryOpExpr = expression->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            FillExpressionConstraintSignature(signature, lhsExpr, keyColumns);
            auto rhsSignature = GetExpressionConstraintSignature(rhsExpr, keyColumns);

            for (int index = 0; index < std::ssize(keyColumns); ++index) {
                signature[index] = std::max(signature[index], rhsSignature[index]);
            }
        } else if (opcode == EBinaryOp::Or) {
            FillExpressionConstraintSignature(signature, lhsExpr, keyColumns);
            auto rhsSignature = GetExpressionConstraintSignature(rhsExpr, keyColumns);

            for (int index = 0; index < std::ssize(keyColumns); ++index) {
                signature[index] = std::min(signature[index], rhsSignature[index]);
            }
        } else {
            if (rhsExpr->As<TReferenceExpression>()) {
                // Ensure that references are on the left.
                std::swap(lhsExpr, rhsExpr);
                opcode = GetReversedBinaryOpcode(opcode);
            }

            const auto* referenceExpr = lhsExpr->As<TReferenceExpression>();
            const auto* constantExpr = rhsExpr->As<TLiteralExpression>();

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            signature[keyPartIndex] = EConstraintKind::Exact;
                            break;
                        case EBinaryOp::Less:
                        case EBinaryOp::LessOrEqual:
                        case EBinaryOp::Greater:
                        case EBinaryOp::GreaterOrEqual:
                            signature[keyPartIndex] = EConstraintKind::Range;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    } else if (const auto* inExpr = expression->As<TInExpression>()) {
        for (const auto& argument : inExpr->Arguments) {
            const auto* reference = argument->As<TReferenceExpression>();
            if (!reference) {
                continue;
            }

            auto keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, reference->ColumnName);
            if (keyPartIndex < 0) {
                continue;
            }

            signature[keyPartIndex] = EConstraintKind::Exact;
        }
    } else if (const auto* betweenExpr = expression->As<TBetweenExpression>()) {
        const auto& expressions = betweenExpr->Arguments;

        const auto* referenceExpr = expressions.front()->As<TReferenceExpression>();
        if (!referenceExpr) {
            return;
        }

        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex < 0) {
            return;
        }

        signature[keyPartIndex] = EConstraintKind::Range;
    } else if (const auto* literalExpr = expression->As<TLiteralExpression>()) {
        const auto& value = TUnversionedValue(literalExpr->Value);
        if (value.Type == EValueType::Boolean) {
            if (value.Data.Boolean) {
                for (auto& signaturePart : signature) {
                    signaturePart = EConstraintKind::None;
                }
            } else {
                for (auto& signaturePart : signature) {
                    signaturePart = EConstraintKind::Exact;
                }
            }
        }
    }
}

std::vector<EConstraintKind> GetExpressionConstraintSignature(
    const TConstExpressionPtr& expression,
    const TKeyColumns& keyColumns)
{
    if (keyColumns.empty()) {
        return {};
    }

    auto signature = std::vector(keyColumns.size(), EConstraintKind::None);

    if (!expression) {
        return signature;
    }

    FillExpressionConstraintSignature(signature, expression, keyColumns);

    return signature;
}

int GetConstraintSignatureScore(const std::vector<EConstraintKind>& signature)
{
    int score = 0;
    for (const auto& kind : signature) {
        if (kind == EConstraintKind::Exact) {
            score += 2;
            continue;
        } else if (kind == EConstraintKind::Range) {
            score += 1;
        }
        break;
    }

    return score;
}

bool CanOmitOrderBy(int keyPrefix, TRange<TOrderItem> orderItems, TRange<std::string> keyColumns)
{
    for (const auto& item : orderItems) {
        if (item.Descending) {
            return false;
        }

        const auto* referenceExpr = item.Expression->As<TReferenceExpression>();

        if (!referenceExpr) {
            return false;
        }

        auto columnIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);

        if (columnIndex == -1) {
            return false;
        }

        if (keyPrefix < columnIndex) {
            return false;
        }

        if (keyPrefix == columnIndex) {
            ++keyPrefix;
        }
    }

    return true;
}

bool CanReverseScanForOrderBy(int fixedKeyPrefix, TRange<TOrderItem> orderItems, TRange<std::string> keyColumns, const TConstGroupClausePtr& groupClause)
{
    if (groupClause) {
        return false;
    }

    int currentKeyIndex = fixedKeyPrefix;

    for (const auto& item : orderItems) {
        if (currentKeyIndex >= std::ssize(keyColumns)) {
            break;
        }

        const auto* referenceExpr = item.Expression->As<TReferenceExpression>();
        if (!referenceExpr) {
            return false;
        }

        auto columnIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);

        if (columnIndex >= 0 && columnIndex < fixedKeyPrefix) {
            continue;
        }

        if (columnIndex != currentKeyIndex) {
            return false;
        }

        if (!item.Descending) {
            return false;
        }

        ++currentKeyIndex;
    }

    return currentKeyIndex > fixedKeyPrefix;
}

////////////////////////////////////////////////////////////////////////////////

std::string ToLower(TStringBuf original)
{
    auto lower = std::string(original);

    std::transform(
        lower.begin(),
        lower.end(),
        lower.begin(),
        [] (unsigned char c) { return std::tolower(c); });

    return lower;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
