#include <ydb/library/yql/providers/generic/pushdown/yql_generic_match_predicate.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

namespace {

    NYql::NConnector::NApi::TPredicate BuildPredicate(const TString& text) {
        NYql::NConnector::NApi::TPredicate predicate;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(text, &predicate));
        return predicate;
    }

    NYql::NGenericPushDown::TColumnStatistics BuildTimestampStats(const TInstant& from, const TInstant& to) {
        NYql::NGenericPushDown::TColumnStatistics statistics;
        statistics.ColumnType.set_type_id(::Ydb::Type::TIMESTAMP);
        statistics.Timestamp.ConstructInPlace();
        statistics.Timestamp->lowValue = from;
        statistics.Timestamp->highValue = to;
        return statistics;
    }

}

Y_UNIT_TEST_SUITE(MatchPredicate) {
    Y_UNIT_TEST(EmptyMatch) {
        UNIT_ASSERT(MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{}, NYql::NConnector::NApi::TPredicate{}));
    }

    Y_UNIT_TEST(EmptyWhere) {
        UNIT_ASSERT(MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", NYql::NGenericPushDown::TColumnStatistics{}},
                                                                                             {"col2", NYql::NGenericPushDown::TColumnStatistics{}}}},
                                   NYql::NConnector::NApi::TPredicate{}));
    }

    Y_UNIT_TEST(Between) {
        UNIT_ASSERT(MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildTimestampStats(TInstant::ParseIso8601("2024-03-01T00:00:00Z"), TInstant::ParseIso8601("2024-03-01T23:59:59Z"))}}},
                                   BuildPredicate(
                                       R"proto(
                                between {
                                    value {
                                        column: "col1"
                                    }
                                    least {
                                        typed_value {
                                            type {
                                                type_id: TIMESTAMP
                                            }
                                            value {
                                                int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                            }
                                        }
                                    }
                                    greatest {
                                        typed_value {
                                            type {
                                                type_id: TIMESTAMP
                                            }
                                            value {
                                                int64_value: 1709294401000000 # 2024-03-01T12:00:01.000Z
                                            }
                                        }
                                    }
                                }
                            )proto")));
    }

    Y_UNIT_TEST(Less) {
        UNIT_ASSERT(MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildTimestampStats(TInstant::ParseIso8601("2024-03-01T00:00:00Z"), TInstant::ParseIso8601("2024-03-01T23:59:59Z"))}}},
                                   BuildPredicate(
                                       R"proto(
                                comparison {
                                    operation: L
                                    left_value {
                                        column: "col1"
                                    }
                                    right_value {
                                        typed_value {
                                            type {
                                                type_id: TIMESTAMP
                                            }
                                            value {
                                                int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                            }
                                        }
                                    }
                                }
                            )proto")));
    }

    Y_UNIT_TEST(NotLess) {
        UNIT_ASSERT(!MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildTimestampStats(TInstant::ParseIso8601("2024-03-02T00:00:00Z"), TInstant::ParseIso8601("2024-03-02T23:59:59Z"))}}},
                                    BuildPredicate(
                                        R"proto(
                                    comparison {
                                        operation: L
                                        left_value {
                                            column: "col1"
                                        }
                                        right_value {
                                            typed_value {
                                                type {
                                                    type_id: TIMESTAMP
                                                }
                                                value {
                                                    int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                                }
                                            }
                                        }
                                    }
                                )proto")));
    }

    Y_UNIT_TEST(RightColumn) {
        UNIT_ASSERT(MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildTimestampStats(TInstant::ParseIso8601("2024-03-01T00:00:00Z"), TInstant::ParseIso8601("2024-03-01T23:59:59Z"))}}},
                                   BuildPredicate(
                                       R"proto(
                                comparison {
                                    operation: G
                                    left_value {
                                        typed_value {
                                            type {
                                                type_id: TIMESTAMP
                                            }
                                            value {
                                                int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                            }
                                        }
                                    }
                                    right_value {
                                        column: "col1"
                                    }
                                }
                            )proto")));
    }
}
