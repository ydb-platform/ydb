#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NYql::NGenericPushDown {

    struct TBooleanColumnStatsData {
        TMaybe<i64> numTrues;
        TMaybe<i64> numFalses;
        TMaybe<i64> numNulls;
        TMaybe<TString> bitVectors;
    };

    struct TDoubleColumnStatsData {
        TMaybe<double> lowValue;
        TMaybe<double> highValue;
        TMaybe<i64> numNulls;
        TMaybe<i64> numDVs;
        TMaybe<TString> bitVectors;
        TMaybe<TString> histogram;
    };

    struct TLongColumnStatsData {
        TMaybe<i64> lowValue;
        TMaybe<i64> highValue;
        TMaybe<i64> numNulls;
        TMaybe<i64> numDVs;
        TMaybe<TString> bitVectors;
        TMaybe<TString> histogram;
    };

    struct TStringColumnStatsData {
        TMaybe<i64> maxColLen;
        TMaybe<double> avgColLen;
        TMaybe<i64> numNulls;
        TMaybe<i64> numDVs;
        TMaybe<TString> bitVectors;
    };

    struct TBinaryColumnStatsData {
        TMaybe<i64> maxColLen;
        TMaybe<double> avgColLen;
        TMaybe<i64> numNulls;
        TMaybe<TString> bitVectors;
    };

    struct TDecimal {
        TMaybe<i16> scale;
        TMaybe<TString> unscaled;
    };

    struct TDecimalColumnStatsData {
        TMaybe<TDecimal> lowValue;
        TMaybe<TDecimal> highValue;
        TMaybe<i64> numNulls;
        TMaybe<i64> numDVs;
        TMaybe<TString> bitVectors;
        TMaybe<TString> histogram;
    };

    struct TTimestampColumnStatsData {
        TMaybe<TInstant> lowValue;
        TMaybe<TInstant> highValue;
        TMaybe<i64> numNulls;
        TMaybe<i64> numDVs;
        TMaybe<TString> bitVectors;
        TMaybe<TString> histogram;
    };

    struct TColumnStatistics {
        TString ColumnName;
        Ydb::Type ColumnType;
        TMaybe<TBooleanColumnStatsData> BooleanStats;
        TMaybe<TBooleanColumnStatsData> DoubleStats;
        TMaybe<TLongColumnStatsData> LongStats;
        TMaybe<TStringColumnStatsData> StringStats;
        TMaybe<TBinaryColumnStatsData> BinaryStats;
        TMaybe<TDecimalColumnStatsData> DecimalStats;
        TMaybe<TTimestampColumnStatsData> Timestamp;
    };

}
