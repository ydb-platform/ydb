#pragma once

#include <ydb/core/statistics/events.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

namespace NKikimr::NStat {

class TSelectBuilder;

// Base class for classes that manage evaluation of column statistics
// of a particular type in TAnalyzeActor.
class IColumnStatisticEval {
public:
    using TPtr = std::unique_ptr<IColumnStatisticEval>;

    static TVector<EStatType> SupportedTypes();
    static TPtr MaybeCreate(
        EStatType,
        const NKikimrStat::TSimpleColumnStatistics&,
        const NScheme::TTypeInfo&);
    static bool AreMinMaxNeeded(const NScheme::TTypeInfo&);

    virtual EStatType GetType() const = 0;
    virtual size_t EstimateSize() const = 0;
    virtual void AddAggregations(const TString& columnName, TSelectBuilder&) = 0;
    virtual TString ExtractData(const TVector<NYdb::TValue>& aggColumns) const = 0;
    virtual ~IColumnStatisticEval() = default;
};

}
