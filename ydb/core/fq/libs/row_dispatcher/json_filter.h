#pragma once

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <yql/essentials/public/udf/udf_value.h>

namespace NFq {

class TJsonFilter {
public:
    using TCallback = std::function<void(ui64, const TString&)>;

public:
    TJsonFilter(
        const TVector<TString>& columns,
        const TVector<TString>& types,
        const TString& whereFilter,
        TCallback callback,
        const TPurecalcCompileSettings& purecalcSettings);

    ~TJsonFilter();

    void Push(const TVector<ui64>& offsets, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 rowsOffset, ui64 numberRows);
    TString GetSql();

    std::unique_ptr<TEvRowDispatcher::TEvPurecalcCompileRequest> GetCompileRequest();  // Should be called exactly once
    void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonFilter> NewJsonFilter(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    const TString& whereFilter,
    TJsonFilter::TCallback callback,
    const TPurecalcCompileSettings& purecalcSettings);

} // namespace NFq
