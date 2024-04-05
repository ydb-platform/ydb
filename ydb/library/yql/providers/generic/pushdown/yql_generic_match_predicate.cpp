#include "yql_generic_match_predicate.h"

namespace NYql {

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

        Triple ComparatorTimestamp(const TMaybe<TColumnStatistics>& statistics, ::NYql::NConnector::NApi::TPredicate::TComparison::EOperation operation, const Ydb::TypedValue& rightValue, int64_t multiplier) {
            if (!statistics || !statistics->Timestamp || !statistics->Timestamp->lowValue || !statistics->Timestamp->highValue) {
                return Triple::Unknown;
            }
            auto& timestampStatistics = *statistics->Timestamp;
            if (!rightValue.type().has_type_id()) {
                return Triple::Unknown;
            }
            if (statistics->ColumnType.type_id() != rightValue.type().type_id()) {
                return Triple::Unknown;
            }
            auto rightValueTimestamp = TInstant::FromValue(rightValue.value().int64_value() * multiplier);
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
                case ::NYql::NConnector::NApi::TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MIN_SENTINEL_DO_NOT_USE_:
                case ::NYql::NConnector::NApi::TPredicate_TComparison_EOperation_TPredicate_TComparison_EOperation_INT_MAX_SENTINEL_DO_NOT_USE_:
                    return Triple::Unknown;
            }
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
                // TODO: other types
                default:
                    return Triple::Unknown;
            }
        }

        Triple MatchComparison(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate::TComparison& comparison) {
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

            switch (statistics.ColumnType.type_id()) {
                case Ydb::Type::TIMESTAMP:
                    return ComparatorTimestamp(statistics, comparison.operation(), rightValue, 1);
                case Ydb::Type::DATETIME:
                    return ComparatorTimestamp(statistics, comparison.operation(), rightValue, 1000000);
                case Ydb::Type::DATE:
                    return ComparatorTimestamp(statistics, comparison.operation(), rightValue, 24 * 3600 * 1000000LL);
                // TODO: other types
                default:
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

    }

    bool MatchPredicate(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate& predicate) {
        return MatchPredicateImpl(columns, predicate) != Triple::False;
    }

} // namespace NYql
