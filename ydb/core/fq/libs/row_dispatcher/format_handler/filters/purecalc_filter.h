#pragma once

#include "consumer.h"

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <yql/essentials/public/udf/udf_value.h>

namespace NFq::NRowDispatcher {

class IProgramCompileHandler : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IProgramCompileHandler>;

public:
    inline IProgramCompileHandler(TString name, IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder, ui64 cookie)
        : Name_(std::move(name))
        , Consumer_(std::move(consumer))
        , ProgramHolder_(std::move(programHolder))
        , Cookie_(cookie)
    {}

    [[nodiscard]] inline const TString& GetName() const {
        return Name_;
    }

    [[nodiscard]] inline IProcessedDataConsumer::TPtr GetConsumer() const {
        return Consumer_;
    }

    [[nodiscard]] inline IProgramHolder::TPtr GetProgram() const {
        return ProgramHolder_;
    }

    [[nodiscard]] inline ui64 GetCookie() const {
        return Cookie_;
    }

    virtual void Compile() = 0;
    virtual void AbortCompilation() = 0;

    virtual void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) = 0;
    virtual void OnCompileError(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) = 0;

protected:
    TString Name_;
    IProcessedDataConsumer::TPtr Consumer_;
    IProgramHolder::TPtr ProgramHolder_;
    ui64 Cookie_;
};

class IProgramRunHandler : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IProgramRunHandler>;

public:
    inline IProgramRunHandler(TString name, IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder)
        : Name_(std::move(name))
        , Consumer_(std::move(consumer))
        , ProgramHolder_(std::move(programHolder))
    {}

    [[nodiscard]] inline const TString& GetName() const {
        return Name_;
    }

    [[nodiscard]] inline IProcessedDataConsumer::TPtr GetConsumer() const {
        return Consumer_;
    }

    [[nodiscard]] inline IProgramHolder::TPtr GetProgramHolder() const {
        return ProgramHolder_;
    }

    virtual void ProcessData(const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows) const = 0;

protected:
    TString Name_;
    IProcessedDataConsumer::TPtr Consumer_;
    IProgramHolder::TPtr ProgramHolder_;
};

IProgramHolder::TPtr CreateFilterProgramHolder(IProcessedDataConsumer::TPtr consumer);

IProgramHolder::TPtr CreateWatermarkProgramHolder(IProcessedDataConsumer::TPtr consumer);

IProgramCompileHandler::TPtr CreateProgramCompileHandler(
    TString name,
    IProcessedDataConsumer::TPtr consumer,
    IProgramHolder::TPtr programHolder,
    ui64 cookie,
    NActors::TActorId compileServiceId,
    NActors::TActorId owner,
    NMonitoring::TDynamicCounterPtr counters
);

IProgramRunHandler::TPtr CreateProgramRunHandler(
    TString name,
    IProcessedDataConsumer::TPtr consumer,
    IProgramHolder::TPtr programHolder,
    NMonitoring::TDynamicCounterPtr counters
);

}  // namespace NFq::NRowDispatcher
