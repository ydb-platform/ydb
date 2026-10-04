#include "yql_generic_match_predicate.h"

#include <cstring>

namespace NYql::NGenericPushDown {

    namespace {

        enum Triple {
            True = 1,
            False = 2,
            Unknown = 3
        };

        Triple Neg(Triple value) {
            switch (value) {
                case Triple::True:
                    return Triple::False;
                case Triple::False:
                    return Triple::True;
                case Triple::Unknown:
                    return Triple::Unknown;
            }
        }

        Triple And(Triple l, Triple r) {
            if (l == Triple::True && r == Triple::True) {
                return Triple::True;
            }

            if (l == Triple::False || r == Triple::False) {
                return Triple::False;
            }

            return Triple::Unknown;
        }

        Triple Or(Triple l, Triple r) {
            if (l == Triple::True || r == Triple::True) {
                return Triple::True;
            }

            if (l == Triple::False && r == Triple::False) {
                return Triple::False;
            }

            return Triple::Unknown;
        }

        bool GetColumn(const NYql::NConnector::NApi::TExpression& expression, TString& columnName) {
            switch (expression.payload_case()) {
                case NYql::NConnector::NApi::TExpression::kColumn:
                    columnName = expression.column();
                    return true;
                case NYql::NConnector::NApi::TExpression::kTypedValue:
                case NYql::NConnector::NApi::TExpression::kArithmeticalExpression:
                case NYql::NConnector::NApi::TExpression::kNull:
                case NYql::NConnector::NApi::TExpression::kCoalesce:
                case NYql::NConnector::NApi::TExpression::kIf:
                case NYql::NConnector::NApi::TExpression::kCast:
                case NYql::NConnector::NApi::TExpression::kUnwrap:
                case NYql::NConnector::NApi::TExpression::kMinOf:
                case NYql::NConnector::NApi::TExpression::kMaxOf:
                case NYql::NConnector::NApi::TExpression::kCurrentUtcTimestamp:
                case NYql::NConnector::NApi::TExpression::kPredicate:
                case NYql::NConnector::NApi::TExpression::kStructMember:
                case NYql::NConnector::NApi::TExpression::kTupleNth:
                case NYql::NConnector::NApi::TExpression::PAYLOAD_NOT_SET:
                    return false;
            }
        }

        bool GetTypedValue(const NYql::NConnector::NApi::TExpression& expression, Ydb::TypedValue& typedValue) {
            switch (expression.payload_case()) {
                case NYql::NConnector::NApi::TExpression::kTypedValue:
                    typedValue = expression.typed_value();
                    return true;
                case NYql::NConnector::NApi::TExpression::kColumn:
                case NYql::NConnector::NApi::TExpression::kArithmeticalExpression:
                case NYql::NConnector::NApi::TExpression::kNull:
                case NYql::NConnector::NApi::TExpression::kCoalesce:
                case NYql::NConnector::NApi::TExpression::kIf:
                case NYql::NConnector::NApi::TExpression::kCast:
                case NYql::NConnector::NApi::TExpression::kUnwrap:
                case NYql::NConnector::NApi::TExpression::kMinOf:
                case NYql::NConnector::NApi::TExpression::kMaxOf:
                case NYql::NConnector::NApi::TExpression::kCurrentUtcTimestamp:
                case NYql::NConnector::NApi::TExpression::kPredicate:
                case NYql::NConnector::NApi::TExpression::kStructMember:
                case NYql::NConnector::NApi::TExpression::kTupleNth:
                case NYql::NConnector::NApi::TExpression::PAYLOAD_NOT_SET:
                    return false;
            }
        }

        Triple BetweenTimestamp(const TMaybe<TColumnStatistics>& statistics, const Ydb::TypedValue& least, const Ydb::TypedValue& greatest, int64_t multiplier) {
            if (!statistics || !statistics->Timestamp || !statistics->Timestamp->lowValue || !statistics->Timestamp->highValue) {
                return Triple::Unknown;
            }
            auto& timestampStatistics = *statistics->Timestamp;
            if (!least.type().has_type_id()) {
                return Triple::Unknown;
            }
            if (!greatest.type().has_type_id()) {
                return Triple::Unknown;
            }
            if (statistics->ColumnType.type_id() != least.type().type_id() || statistics->ColumnType.type_id() != greatest.type().type_id()) {
                return Triple::Unknown;
            }
            auto leastTimestamp = TInstant::FromValue(least.value().int64_value() * multiplier);
            auto greatestTimestamp = TInstant::FromValue(greatest.value().int64_value() * multiplier);
            return timestampStatistics.lowValue <= greatestTimestamp && timestampStatistics.highValue >= leastTimestamp ? Triple::True : Triple::False;
        }

        Triple ComparatorTimestamp(const TMaybe<TColumnStatistics>& lValue, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, const Ydb::TypedValue& rValue, int64_t multiplier) {
            if (!lValue || !lValue->Timestamp || !lValue->Timestamp->lowValue || !lValue->Timestamp->highValue) {
                return Triple::Unknown;
            }
            if (!rValue.type().has_type_id()) {
                return Triple::Unknown;
            }
            if (lValue->ColumnType.type_id() != rValue.type().type_id()) {
                return Triple::Unknown;
            }
            auto& timestampStatistics = *lValue->Timestamp;
            auto rightValueTimestamp = TInstant::FromValue(rValue.value().int64_value() * multiplier);
            switch (operation) {
                case ::NYql::NConnector::NApi::TPredicate::TComparison::EQ:
                    return timestampStatistics.lowValue <= rightValueTimestamp && rightValueTimestamp <= timestampStatistics.highValue ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::LE:
                    return timestampStatistics.lowValue <= rightValueTimestamp ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::L:
                    return timestampStatistics.lowValue < rightValueTimestamp ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::GE:
                    return rightValueTimestamp <= timestampStatistics.highValue ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::G:
                    return rightValueTimestamp < timestampStatistics.highValue ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::NE:
                    return rightValueTimestamp < timestampStatistics.lowValue || timestampStatistics.highValue < rightValueTimestamp ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::IND:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::ID:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::STARTS_WITH:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::ENDS_WITH:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::CONTAINS:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MIN_SENTINEL_DO_NOT_USE_:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MAX_SENTINEL_DO_NOT_USE_:
                    return Triple::Unknown;
            }
        }

        Triple ComparatorTimestamp(const Ydb::TypedValue& lValue, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, const TMaybe<TColumnStatistics>& rValue, int64_t multiplier) {
            if (!rValue || !rValue->Timestamp || !rValue->Timestamp->lowValue || !rValue->Timestamp->highValue) {
                return Triple::Unknown;
            }
            if (!lValue.type().has_type_id()) {
                return Triple::Unknown;
            }
            if (lValue.type().type_id() != rValue->ColumnType.type_id()) {
                return Triple::Unknown;
            }
            auto& timestampStatistics = *rValue->Timestamp;
            auto leftValueTimestamp = TInstant::FromValue(lValue.value().int64_value() * multiplier);
            switch (operation) {
                case ::NYql::NConnector::NApi::TPredicate::TComparison::EQ:
                    return timestampStatistics.lowValue <= leftValueTimestamp && leftValueTimestamp <= timestampStatistics.highValue ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::LE:
                    return leftValueTimestamp <= timestampStatistics.highValue ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::L:
                    return leftValueTimestamp < timestampStatistics.highValue ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::GE:
                    return timestampStatistics.lowValue <= leftValueTimestamp ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::G:
                    return timestampStatistics.lowValue < leftValueTimestamp ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::NE:
                    return leftValueTimestamp < timestampStatistics.lowValue || timestampStatistics.highValue < leftValueTimestamp ? Triple::True : Triple::False;
                case ::NYql::NConnector::NApi::TPredicate::TComparison::IND:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::ID:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::STARTS_WITH:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::ENDS_WITH:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::CONTAINS:
                case ::NYql::NConnector::NApi::TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MIN_SENTINEL_DO_NOT_USE_:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MAX_SENTINEL_DO_NOT_USE_:
                    return Triple::Unknown;
            }
        }

        TMaybe<TString> TypedValueToUuidBytes(const Ydb::TypedValue& value) {
            if (!value.type().has_type_id() || value.type().type_id() != Ydb::Type::UUID) {
                return {};
            }
            TString bytes;
            bytes.resize(16);
            const ui64 low = value.value().low_128();
            const ui64 high = value.value().high_128();
            // Byte-by-byte copy to avoid endianness issues.
            // low_128 = bytes 0..7, high_128 = bytes 8..15 (little-endian interpretation).
            for (int i = 0; i < 8; ++i) {
                bytes[i] = static_cast<char>(low >> (8 * i));
                bytes[8 + i] = static_cast<char>(high >> (8 * i));
            }
            return bytes;
        }

        ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation SwapComparison(
            ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation
        ) {
            using TComparison = ::NYql::NConnector::NApi::TPredicate::TComparison;
            switch (operation) {
                case TComparison::LE:
                    return TComparison::GE;
                case TComparison::L:
                    return TComparison::G;
                case TComparison::GE:
                    return TComparison::LE;
                case TComparison::G:
                    return TComparison::L;
                default:
                    return operation;
            }
        }

        template <typename T>
        Triple CompareMinMax(T lo, T hi, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, T c) {
            using TComparison = ::NYql::NConnector::NApi::TPredicate::TComparison;
            switch (operation) {
                case TComparison::EQ:
                    return lo <= c && c <= hi ? Triple::True : Triple::False;
                case TComparison::NE:
                    return (lo == hi && lo == c) ? Triple::False : Triple::True;
                case TComparison::LE:
                    return lo <= c ? Triple::True : Triple::False;
                case TComparison::L:
                    return lo < c ? Triple::True : Triple::False;
                case TComparison::GE:
                    return c <= hi ? Triple::True : Triple::False;
                case TComparison::G:
                    return c < hi ? Triple::True : Triple::False;
                case TComparison::IND:
                case TComparison::ID:
                case TComparison::STARTS_WITH:
                case TComparison::ENDS_WITH:
                case TComparison::CONTAINS:
                case TComparison::COMPARISON_OPERATION_UNSPECIFIED:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MIN_SENTINEL_DO_NOT_USE_:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MAX_SENTINEL_DO_NOT_USE_:
                    return Triple::Unknown;
            }
            return Triple::Unknown;
        }

        Triple CompareUuidStats(const TMaybe<TColumnStatistics>& statistics, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, const Ydb::TypedValue& typedValue) {
            if (!statistics || !statistics->UuidStats || !statistics->UuidStats->lowValue || !statistics->UuidStats->highValue) {
                return Triple::Unknown;
            }
            if (statistics->UuidStats->lowValue->size() != 16 || statistics->UuidStats->highValue->size() != 16) {
                return Triple::Unknown;
            }
            const auto constant = TypedValueToUuidBytes(typedValue);
            if (!constant || constant->size() != 16) {
                return Triple::Unknown;
            }
            return CompareMinMax(*statistics->UuidStats->lowValue, *statistics->UuidStats->highValue, operation, *constant);
        }

        Triple MatchBetween(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate::TBetween& between) {
            TString columnName;
            if (!GetColumn(between.value(), columnName)) { // TODO: ArithmeticalExpression
                return Triple::Unknown;
            }

            auto it = columns.find(columnName);
            if (it == columns.end()) {
                return Triple::Unknown;
            }

            Ydb::TypedValue least;
            if (!GetTypedValue(between.least(), least)) { // TODO: ArithmeticalExpression
                return Triple::Unknown;
            }

            Ydb::TypedValue greatest;
            if (!GetTypedValue(between.greatest(), greatest)) { // TODO: ArithmeticalExpression
                return Triple::Unknown;
            }

            auto& statistics = it->second;
            if (!statistics.ColumnType.has_type_id()) { // TODO: OptionalType
                return Triple::Unknown;
            }

            switch (statistics.ColumnType.type_id()) {
                case Ydb::Type::TIMESTAMP:
                    return BetweenTimestamp(statistics, least, greatest, 1);
                case Ydb::Type::DATETIME:
                    return BetweenTimestamp(statistics, least, greatest, 1000000);
                case Ydb::Type::DATE:
                    return BetweenTimestamp(statistics, least, greatest, 24 * 3600 * 1000000LL);
                case Ydb::Type::UUID: {
                    if (!statistics.UuidStats || !statistics.UuidStats->lowValue || !statistics.UuidStats->highValue) {
                        return Triple::Unknown;
                    }
                    const auto leastUuid = TypedValueToUuidBytes(least);
                    const auto greatestUuid = TypedValueToUuidBytes(greatest);
                    if (!leastUuid || !greatestUuid) {
                        return Triple::Unknown;
                    }
                    if (*leastUuid > *greatestUuid) {
                        return Triple::False;
                    }
                    return *statistics.UuidStats->lowValue <= *greatestUuid && *statistics.UuidStats->highValue >= *leastUuid
                        ? Triple::True : Triple::False;
                }
                // TODO: other types
                default:
                    return Triple::Unknown;
            }
        }

        Triple MatchComparison(const Ydb::TypedValue& lValue, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, const TMaybe<TColumnStatistics>& rValue) {
            switch (rValue->ColumnType.type_id()) {
                case Ydb::Type::TIMESTAMP:
                    return ComparatorTimestamp(lValue, operation, rValue, 1);
                case Ydb::Type::DATETIME:
                    return ComparatorTimestamp(lValue, operation, rValue, 1000000);
                case Ydb::Type::DATE:
                    return ComparatorTimestamp(lValue, operation, rValue, 24 * 3600 * 1000000LL);
                case Ydb::Type::UUID:
                    return CompareUuidStats(rValue, SwapComparison(operation), lValue);
                // TODO: other types
                default:
                    return Triple::Unknown;
            }
        }

        Triple MatchComparison(const TMaybe<TColumnStatistics>& lValue, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, const Ydb::TypedValue& rValue) {
            switch (lValue->ColumnType.type_id()) {
                case Ydb::Type::TIMESTAMP:
                    return ComparatorTimestamp(lValue, operation, rValue, 1);
                case Ydb::Type::DATETIME:
                    return ComparatorTimestamp(lValue, operation, rValue, 1000000);
                case Ydb::Type::DATE:
                    return ComparatorTimestamp(lValue, operation, rValue, 24 * 3600 * 1000000LL);
                case Ydb::Type::UUID:
                    return CompareUuidStats(lValue, operation, rValue);
                // TODO: other types
                default:
                    return Triple::Unknown;
            }
        }

        Triple MatchComparison(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate::TComparison& comparison) {
            switch (comparison.left_value().payload_case()) {
                case NYql::NConnector::NApi::TExpression::kColumn: {
                    TString columnName;
                    if (!GetColumn(comparison.left_value(), columnName)) { // TODO: ArithmeticalExpression
                        return Triple::Unknown;
                    }

                    auto it = columns.find(columnName);
                    if (it == columns.end()) {
                        return Triple::Unknown;
                    }

                    Ydb::TypedValue rightValue;
                    if (!GetTypedValue(comparison.right_value(), rightValue)) { // TODO: ArithmeticalExpression
                        return Triple::Unknown;
                    }

                    auto& statistics = it->second;
                    if (!statistics.ColumnType.has_type_id()) { // TODO: OptionalType
                        return Triple::Unknown;
                    }

                    return MatchComparison(statistics, comparison.operation(), rightValue);
                }
                case NYql::NConnector::NApi::TExpression::kTypedValue: {
                    TString columnName;
                    if (!GetColumn(comparison.right_value(), columnName)) { // TODO: ArithmeticalExpression
                        return Triple::Unknown;
                    }

                    auto it = columns.find(columnName);
                    if (it == columns.end()) {
                        return Triple::Unknown;
                    }

                    Ydb::TypedValue leftValue;
                    if (!GetTypedValue(comparison.left_value(), leftValue)) { // TODO: ArithmeticalExpression
                        return Triple::Unknown;
                    }

                    auto& statistics = it->second;
                    if (!statistics.ColumnType.has_type_id()) { // TODO: OptionalType
                        return Triple::Unknown;
                    }

                    return MatchComparison(leftValue, comparison.operation(), statistics);
                }
                case NYql::NConnector::NApi::TExpression::kArithmeticalExpression:
                case NYql::NConnector::NApi::TExpression::kNull:
                case NYql::NConnector::NApi::TExpression::kCoalesce:
                case NYql::NConnector::NApi::TExpression::kIf:
                case NYql::NConnector::NApi::TExpression::kCast:
                case NYql::NConnector::NApi::TExpression::kUnwrap:
                case NYql::NConnector::NApi::TExpression::kMinOf:
                case NYql::NConnector::NApi::TExpression::kMaxOf:
                case NYql::NConnector::NApi::TExpression::kCurrentUtcTimestamp:
                case NYql::NConnector::NApi::TExpression::kPredicate:
                case NYql::NConnector::NApi::TExpression::kStructMember:
                case NYql::NConnector::NApi::TExpression::kTupleNth:
                case NYql::NConnector::NApi::TExpression::PAYLOAD_NOT_SET:
                    return Triple::Unknown;
            }
        }

        Triple MatchPredicateImpl(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate& predicate) {
            switch (predicate.payload_case()) {
                case NYql::NConnector::NApi::TPredicate::kNegation:
                    return Neg(MatchPredicateImpl(columns, predicate.negation().operand()));
                case NYql::NConnector::NApi::TPredicate::kConjunction: {
                    if (predicate.conjunction().operands_size() == 0) {
                        return Triple::True;
                    }
                    Triple result = MatchPredicateImpl(columns, predicate.conjunction().operands(0));
                    for (int i = 1; i < predicate.conjunction().operands_size(); i++) {
                        auto r = MatchPredicateImpl(columns, predicate.conjunction().operands(i));
                        result = And(result, r);
                    }
                    return result;
                }
                case NYql::NConnector::NApi::TPredicate::kDisjunction: {
                    if (predicate.disjunction().operands_size() == 0) {
                        return Triple::True;
                    }
                    Triple result = MatchPredicateImpl(columns, predicate.disjunction().operands(0));
                    for (int i = 1; i < predicate.disjunction().operands_size(); i++) {
                        auto r = MatchPredicateImpl(columns, predicate.disjunction().operands(i));
                        result = Or(result, r);
                    }
                    return result;
                }
                case NYql::NConnector::NApi::TPredicate::kBetween: {
                    return MatchBetween(columns, predicate.between());
                }
                case NYql::NConnector::NApi::TPredicate::kComparison: {
                    return MatchComparison(columns, predicate.comparison());
                }
                case NConnector::NApi::TPredicate::PAYLOAD_NOT_SET:
                    return Triple::Unknown;
                default:
                    break;
            }

            return Triple::Unknown;
        }

    } // namespace

    bool MatchPredicate(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate& predicate) {
        return MatchPredicateImpl(columns, predicate) != Triple::False;
    }

} // namespace NYql::NGenericPushDown
