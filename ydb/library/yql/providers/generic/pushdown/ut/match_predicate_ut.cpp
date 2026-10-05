#include <ydb/library/yql/providers/generic/pushdown/yql_generic_match_predicate.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

#include <cstring>

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

    TString UuidBytesFromHalves(ui64 low, ui64 high) {
        TString bytes;
        bytes.resize(16);
        memcpy(bytes.begin(), &low, sizeof(ui64));
        memcpy(bytes.begin() + sizeof(ui64), &high, sizeof(ui64));
        return bytes;
    }

    NYql::NGenericPushDown::TColumnStatistics BuildUuidStats(const TString& from, const TString& to) {
        NYql::NGenericPushDown::TColumnStatistics statistics;
        statistics.ColumnType.set_type_id(::Ydb::Type::UUID);
        statistics.UuidStats.ConstructInPlace();
        statistics.UuidStats->lowValue = from;
        statistics.UuidStats->highValue = to;
        return statistics;
    }

    TString ComparisonPredicate(const TString& column, const TString& operation, const TString& typeId, const TString& valueField) {
        return TStringBuilder()
            << "comparison {\n"
            << "    operation: " << operation << "\n"
            << "    left_value { column: \"" << column << "\" }\n"
            << "    right_value {\n"
            << "        typed_value {\n"
            << "            type { type_id: " << typeId << " }\n"
            << "            value { " << valueField << " }\n"
            << "        }\n"
            << "    }\n"
            << "}\n";
    }

    std::pair<ui64, ui64> UuidHalves(const TString& bytes) {
        ui64 low = 0;
        ui64 high = 0;
        memcpy(&low, bytes.data(), sizeof(ui64));
        memcpy(&high, bytes.data() + sizeof(ui64), sizeof(ui64));
        return {low, high};
    }

    bool MatchUuid(const TString& lo, const TString& hi, const TString& operation, const TString& constant) {
        const auto halves = UuidHalves(constant);
        return MatchPredicate(
            TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildUuidStats(lo, hi)}}},
            BuildPredicate(ComparisonPredicate(
                "col1",
                operation,
                "UUID",
                TStringBuilder() << "low_128: " << halves.first << " high_128: " << halves.second)));
    }

} // namespace

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

    Y_UNIT_TEST(BetweenReversed) {
        UNIT_ASSERT(!MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildTimestampStats(TInstant::ParseIso8601("2024-03-01T00:00:00Z"), TInstant::ParseIso8601("2024-03-01T23:59:59Z"))}}},
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
                                                 int64_value: 1709294401000000 # 2024-03-01T12:00:01.000Z
                                             }
                                         }
                                     }
                                     greatest {
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

    Y_UNIT_TEST(BetweenReversedNoStatistics) {
        UNIT_ASSERT(!MatchPredicate(TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{},
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
                                                 int64_value: 1709294401000000 # 2024-03-01T12:00:01.000Z
                                             }
                                         }
                                     }
                                     greatest {
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

    Y_UNIT_TEST(UuidStatsComparators) {
        const TString lo = TString(16, '\x10');
        const TString hi = TString(16, '\x20');
        const TString inside = TString(16, '\x15');
        const TString below = TString(16, '\x00');
        const TString above = TString(16, '\x30');
        const TString point = TString(16, '\x05');

        const auto insideH = UuidHalves(inside);
        UNIT_ASSERT_VALUES_EQUAL(inside.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(UuidBytesFromHalves(insideH.first, insideH.second), inside);

        UNIT_ASSERT(MatchUuid(lo, hi, "EQ", inside));
        UNIT_ASSERT(!MatchUuid(lo, hi, "EQ", below));
        UNIT_ASSERT(!MatchUuid(lo, hi, "EQ", above));
        UNIT_ASSERT(MatchUuid(lo, hi, "EQ", lo));
        UNIT_ASSERT(MatchUuid(lo, hi, "EQ", hi));
        UNIT_ASSERT(!MatchUuid(lo, hi, "L", lo));
        UNIT_ASSERT(!MatchUuid(lo, hi, "G", hi));
        UNIT_ASSERT(!MatchUuid(point, point, "NE", point));
        UNIT_ASSERT(MatchUuid(lo, hi, "NE", inside));

        const auto belowH = UuidHalves(below);
        const auto missHiH = UuidHalves(TString(16, '\x05'));
        const auto aboveH = UuidHalves(above);
        UNIT_ASSERT(!MatchPredicate(
            TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildUuidStats(lo, hi)}}},
            BuildPredicate(
                TStringBuilder() << "between {\n"
                                 << "    value { column: \"col1\" }\n"
                                 << "    least { typed_value { type { type_id: UUID } value { low_128: " << belowH.first
                                 << " high_128: " << belowH.second << " } } }\n"
                                 << "    greatest { typed_value { type { type_id: UUID } value { low_128: " << missHiH.first
                                 << " high_128: " << missHiH.second << " } } }\n"
                                 << "}\n")));
        UNIT_ASSERT(MatchPredicate(
            TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildUuidStats(lo, hi)}}},
            BuildPredicate(
                TStringBuilder() << "between {\n"
                                 << "    value { column: \"col1\" }\n"
                                 << "    least { typed_value { type { type_id: UUID } value { low_128: " << insideH.first
                                 << " high_128: " << insideH.second << " } } }\n"
                                 << "    greatest { typed_value { type { type_id: UUID } value { low_128: " << aboveH.first
                                 << " high_128: " << aboveH.second << " } } }\n"
                                 << "}\n")));

        UNIT_ASSERT(MatchPredicate(
            TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildUuidStats(lo, hi)}}},
            BuildPredicate(ComparisonPredicate("col1", "EQ", "INT64", "int64_value: 1"))));
    }

    Y_UNIT_TEST(UuidStatsWrongLengthKeepsGroup) {
        // UuidStats with lowValue/highValue not 16 bytes -> Unknown -> keep group.
        const TString shortLo(8, '\x10');
        const TString shortHi(8, '\x20');
        const TString inside(16, '\x15');
        const auto insideH = UuidHalves(inside);

        UNIT_ASSERT(MatchPredicate(
            TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", BuildUuidStats(shortLo, shortHi)}}},
            BuildPredicate(ComparisonPredicate(
                "col1",
                "EQ",
                "UUID",
                TStringBuilder() << "low_128: " << insideH.first << " high_128: " << insideH.second))));
    }

    Y_UNIT_TEST(UuidStatsMissingMinMaxKeepsGroup) {
        // UuidStats present but lowValue/highValue not set -> Unknown -> keep group.
        NYql::NGenericPushDown::TColumnStatistics stats;
        stats.ColumnType.set_type_id(::Ydb::Type::UUID);
        stats.UuidStats.ConstructInPlace();
        // lowValue and highValue are TMaybe<TString>, not set.

        const TString inside(16, '\x15');
        const auto insideH = UuidHalves(inside);
        UNIT_ASSERT(MatchPredicate(
            TMap<TString, NYql::NGenericPushDown::TColumnStatistics>{{{"col1", stats}}},
            BuildPredicate(ComparisonPredicate(
                "col1",
                "EQ",
                "UUID",
                TStringBuilder() << "low_128: " << insideH.first << " high_128: " << insideH.second))));
    }
} // Y_UNIT_TEST_SUITE(MatchPredicate)
