#pragma once

#include "percentile.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/load_test.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/types.h>

#include <array>

namespace NKikimr {

template<typename T>
struct TAggregatedField {
    T MinValue;
    T MaxValue;
    T AvgValue;
};

template<typename T>
IOutputStream& operator<<(IOutputStream& output, const TAggregatedField<T>& field) {
    output << field.MinValue << ", " << field.AvgValue << ", " << field.MaxValue;
    return output;
}

template<typename T>
void PrintFieldToHtml(const TAggregatedField<T>& field, IOutputStream& output) {
    output << "<abbr title=\"";
    output << field.MinValue << " &le; " << field.AvgValue << " &le; " << field.MaxValue;
    output << "\">" << field.AvgValue << "</abbr>";
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
    T Sum = 0;
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
        T avgValue = Count == 0 ? 0 : (Sum / Count);
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
