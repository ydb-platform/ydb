#pragma once

#include <ydb/core/statistics/events.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

namespace NKikimr::NStat {

class TSelectBuilder;

class TSimpleColumnStatisticEval {
    NScheme::TTypeInfo Type;
    TString PgTypeMod;

    std::optional<ui32> CountDistinctSeq;
    std::optional<ui32> MinSeq;
    std::optional<ui32> MaxSeq;
public:
    using TPtr = std::unique_ptr<TSimpleColumnStatisticEval>;

    explicit TSimpleColumnStatisticEval(NScheme::TTypeInfo type, TString pgTypeMod)
        : Type(std::move(type))
        , PgTypeMod(std::move(pgTypeMod))
    {}

    EStatType GetType() const;
    size_t EstimateSize() const;
    void AddAggregations(const TString& columnName, TSelectBuilder& builder);
    NKikimrStat::TSimpleColumnStatistics Extract(
        ui64 rowCount, const TVector<NYdb::TValue>& aggColumns) const;
};

// Base class for classes that manage evaluation of column statistics
// of a particular type in TAnalyzeActor.
class IStage2ColumnStatisticEval {
public:
    using TPtr = std::unique_ptr<IStage2ColumnStatisticEval>;

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
    virtual ~IStage2ColumnStatisticEval() = default;
};

}
