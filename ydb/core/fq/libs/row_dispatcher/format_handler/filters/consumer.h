#pragma once

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <yql/essentials/public/udf/udf_value.h>

namespace NFq::NRowDispatcher {

class IProcessedDataConsumer : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IProcessedDataConsumer>;

public:
    virtual const TVector<TSchemaColumn>& GetColumns() const = 0;
    virtual const TString& GetWatermarkExpr() const = 0;
    virtual const TString& GetFilterExpr() const = 0;
    virtual TPurecalcCompileSettings GetPurecalcSettings() const = 0;
    virtual bool IsStarted() const = 0;

    virtual NActors::TActorId GetClientId() const = 0;
    virtual const TVector<ui64>& GetColumnIds() const = 0;
    virtual std::optional<ui64> GetNextMessageOffset() const = 0;

    virtual void OnData(const NYql::NUdf::TUnboxedValue*) = 0;

    virtual void OnStart() = 0;
    virtual void OnError(TStatus status) = 0;
};

}  // namespace NFq::NRowDispatcher
