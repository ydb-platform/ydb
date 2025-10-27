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
    inline IProgramCompileHandler( IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder, ui64 cookie)
        : Consumer_(std::move(consumer))
        , ProgramHolder_(std::move(programHolder))
        , Cookie_(cookie)
    {}

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
    IProcessedDataConsumer::TPtr Consumer_;
    IProgramHolder::TPtr ProgramHolder_;
    ui64 Cookie_;
};

class IProgramRunHandler : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IProgramRunHandler>;

public:
    inline IProgramRunHandler(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder)
        : Consumer_(std::move(consumer))
        , ProgramHolder_(std::move(programHolder))
    {}

    [[nodiscard]] inline IProcessedDataConsumer::TPtr GetConsumer() const {
        return Consumer_;
    }

    [[nodiscard]] inline IProgramHolder::TPtr GetProgramHolder() const {
        return ProgramHolder_;
    }

    virtual void ProcessData(const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows) const = 0;

protected:
    IProcessedDataConsumer::TPtr Consumer_;
    IProgramHolder::TPtr ProgramHolder_;
};

IProgramHolder::TPtr CreateProgramHolder(IProcessedDataConsumer::TPtr consumer);

IProgramCompileHandler::TPtr CreateProgramCompileHandler(
    IProcessedDataConsumer::TPtr consumer,
    IProgramHolder::TPtr programHolder,
    ui64 cookie,
    NActors::TActorId compileServiceId,
    NActors::TActorId owner,
    NMonitoring::TDynamicCounterPtr counters
);

IProgramRunHandler::TPtr CreateProgramRunHandler(
    IProcessedDataConsumer::TPtr consumer,
    IProgramHolder::TPtr programHolder,
    NMonitoring::TDynamicCounterPtr counters
);

}  // namespace NFq::NRowDispatcher
