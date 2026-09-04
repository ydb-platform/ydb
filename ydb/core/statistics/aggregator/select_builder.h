#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NStat {

// YQL literal for a ui64 value, e.g. 1000000ul.  Join(',', ...) calls Out on each
// parameter, so we return a TString to use the existing Out<TString> overload.
inline TString Ui64Literal(ui64 v) {
    return TString(TStringBuilder() << v << "ul");
}

// YQL literal for a ui32 value, e.g. 1000000u.  Without the suffix YQL infers a
// bare integer literal as Int32, while the UDAF's CreateState reads it with
// Get<ui32>(); the mismatch is harmless at today's magnitudes but wrong if the
// value ever crosses 2^31.
inline TString Ui32Literal(ui32 v) {
    return TString(TStringBuilder() << v << "u");
}

// How a tuple of columns is encoded before being fed to a UDAF.
enum class ETupleEncoding {
    StablePickle, // canonical bytes: equal values -> equal bytes. For hash sketches.
    PresortKey,  // memcomparable bytes: memcmp == value order. For histograms.
};

// Class that is used to build internal SELECT queries used to calculate column statistics.
class TSelectBuilder {
public:
    // If isIntermediateAggregation is true, results of several SELECTs over different
    // parts of the table are expected to be combined into the final result.
    // UDAFs won't finalize their result and will return an intermediate aggregation state
    // that can be merged with intermediate states.
    explicit TSelectBuilder(bool isIntermediateAggregation)
        : IsIntermediateAggregation_(isIntermediateAggregation)
    {}

    bool IsIntermediateAggregation() const { return IsIntermediateAggregation_; }

    ui32 AddBuiltinAggregation(std::optional<TString> columnName, TString aggName);

    template<typename... TArgs>
    ui32 AddUDAFAggregation(TString columnName, const TStringBuf& udafName, TArgs&&... params);

    // Aggregates a tuple of columns instead of a single column, for statistics computed
    // over a tuple of columns.  The encoding controls how the tuple is serialised before
    // being passed to the UDAF.
    template<typename... TArgs>
    ui32 AddUDAFAggregationTuple(std::vector<TString> columnNames, ETupleEncoding encoding,
                                 const TStringBuf& udafName, TArgs&&... params);

    TString Build(const TStringBuf& table, std::optional<ui64> tabletId) const;

    size_t ColumnCount() const {
        return Columns.size();
    }

private:
    ui32 AddFactory(const TStringBuf& udafName, size_t paramCount);

private:
    bool IsIntermediateAggregation_;

    struct TFactory {
        TFactory(ui32 id, const TStringBuf& udaf, size_t paramCount)
            : Id(id), Udaf(udaf), ParamCount(paramCount)
        {}

        ui32 Id = 0;
        TString Udaf;
        size_t ParamCount = 0;
    };

    THashMap<TString, TFactory> Udaf2Factory;

    struct TAggColumn {
        ui32 Seq = 0;
        std::optional<TString> ColumnName;
        std::optional<std::vector<TString>> TupleColumnNames;
        ETupleEncoding TupleEncoding = ETupleEncoding::StablePickle;
        std::optional<TString> AggName;
        std::optional<ui32> UdafFactory;
        TString Params;
    };

    TVector<TAggColumn> Columns;

    // Assigns the column its sequence number, appends it, and returns that number.
    ui32 PushColumn(TAggColumn column) {
        column.Seq = static_cast<ui32>(Columns.size());
        Columns.push_back(std::move(column));
        return Columns.back().Seq;
    }
};

template<>
inline ui32 TSelectBuilder::AddUDAFAggregation(TString columnName, const TStringBuf& udafName) {
    return PushColumn(TAggColumn{
        .ColumnName = std::move(columnName),
        .UdafFactory = AddFactory(udafName, 0),
    });
}

template<typename... TArgs>
ui32 TSelectBuilder::AddUDAFAggregation(TString columnName, const TStringBuf& udafName, TArgs&&... params) {
    auto factory = AddFactory(udafName, sizeof...(params));
    return PushColumn(TAggColumn{
        .ColumnName = std::move(columnName),
        .UdafFactory = factory,
        // TODO: parameters escaping/binding
        .Params = Join(',', params...),
    });
}

template<typename... TArgs>
ui32 TSelectBuilder::AddUDAFAggregationTuple(std::vector<TString> columnNames, ETupleEncoding encoding,
                                             const TStringBuf& udafName, TArgs&&... params) {
    auto factory = AddFactory(udafName, sizeof...(params));
    return PushColumn(TAggColumn{
        .TupleColumnNames = std::move(columnNames),
        .TupleEncoding = encoding,
        .UdafFactory = factory,
        // TODO: parameters escaping/binding
        .Params = Join(',', params...),
    });
}

} // NKikimr::NStat
