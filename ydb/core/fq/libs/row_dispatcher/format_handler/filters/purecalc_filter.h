#pragma once

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <yql/essentials/public/udf/udf_value.h>

namespace NFq::NRowDispatcher {

class IPurecalcFilterConsumer : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPurecalcFilterConsumer>;

public:
    virtual const TVector<TSchemaColumn>& GetColumns() const = 0;
    virtual const TString& GetWhereFilter() const = 0;
    virtual TPurecalcCompileSettings GetPurecalcSettings() const = 0;

    virtual void OnFilteredData(ui64 rowId) = 0;
};

class IPurecalcFilter : public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<IPurecalcFilter>;

public:
    virtual void FilterData(const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows) = 0;

    virtual std::unique_ptr<TEvRowDispatcher::TEvPurecalcCompileRequest> GetCompileRequest() = 0;  // Should be called exactly once
    virtual void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) = 0;
};

TValueStatus<IPurecalcFilter::TPtr> CreatePurecalcFilter(IPurecalcFilterConsumer::TPtr consumer);

}  // namespace NFq::NRowDispatcher
