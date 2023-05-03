#pragma once

#include "percentile.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/load_test.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/types.h>

#include <array>

namespace NKikimr {

template<typename T>
struct TAggregatedField {
    T MinValue = 0;
    T MaxValue = 0;
    double AvgValue = 0;
};

template<typename T>
IOutputStream& operator<<(IOutputStream& output, const TAggregatedField<T>& field) {
    output << field.MinValue << ", " << field.AvgValue << ", " << field.MaxValue;
    return output;
}

template<typename T>
TString PrintValueFixedPrecision(const T& x) {
    return ToString(x);
}

template<>
inline TString PrintValueFixedPrecision(const double& x) {
    if (fabs(x) < 0.01) {
        return Sprintf("%.4lf", x);
    } else if (fabs(x) < 0.1) {
        return Sprintf("%.3lf", x);
    } else {
        return Sprintf("%.2lf", x);
    }
}

template<typename T>
void PrintFieldToHtml(const TAggregatedField<T>& field, IOutputStream& output) {
    output << "<span title=\"";
    output << PrintValueFixedPrecision(field.MinValue) <<
        " &le; " << PrintValueFixedPrecision(field.AvgValue) <<
        " &le; " << PrintValueFixedPrecision(field.MaxValue);
    output << "\">" << PrintValueFixedPrecision(field.AvgValue) << "</span>";
}

struct TAggregatedStats {
    ui32 TotalNodes = 0;
    ui32 SuccessNodes = 0;
    TAggregatedField<ui64> Transactions;
    TAggregatedField<double> TransactionsPerSecond;
    TAggregatedField<double> ErrorsPerSecond;
    std::array<TAggregatedField<double>, EPL_COUNT> Percentiles;
};

IOutputStream& operator<<(IOutputStream& output, const TAggregatedStats& stats);

template<typename T>
class TFieldAggregator {
    T MinValue = 0;
    T MaxValue = 0;
    double Sum = 0;
    ui32 Count = 0;
public:
    void Add(T value) {
        if (Count) {
            MinValue = Min<T>(MinValue, value);
            MaxValue = Max<T>(MaxValue, value);
        } else {
            MinValue = value;
            MaxValue = value;
        }
        Sum += value;
        ++Count;
    }

    TAggregatedField<T> Get() const {
        double avgValue = Count == 0 ? 0 : (Sum / Count);
        return TAggregatedField<T>{
            .MinValue = MinValue,
            .MaxValue = MaxValue,
            .AvgValue = avgValue
        };
    }
};

class TStatsAggregator {
    ui32 TotalNodes;
    ui32 SuccessNodes;
    TFieldAggregator<ui64> Transactions;
    TFieldAggregator<double> TransactionsPerSecond;
    TFieldAggregator<double> ErrorsPerSecond;
    std::array<TFieldAggregator<double>, EPL_COUNT> Percentiles;

public:
    TStatsAggregator(ui32 totalNodes)
        : TotalNodes(totalNodes)
        , SuccessNodes(0)
    {}

    void Add(const TEvNodeFinishResponse::TNodeStats& stats);
    TAggregatedStats Get() const;
};

void PrintStartFinishToHtml(const TInstant& start, const TInstant& finish, IOutputStream& output);

struct TAggregatedResult {
    TString Uuid;
    TInstant Start;
    TInstant Finish;
    TAggregatedStats Stats;
    TString Config;
};

void PrintUuidToHtml(const TString& uuid, IOutputStream& output);

IOutputStream& operator<<(IOutputStream& output, const TAggregatedResult& result);

bool LoadResultFromResponseProto(const NKikimrKqp::TQueryResponse& response, TVector<TAggregatedResult>& results);

}  // namespace NKikimr
