#pragma once

#include "client_common.h"

#include <yt/yt/flow/lib/client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TStartPipelineOptions
    : public TTimeoutOptions
{ };

struct TStopPipelineOptions
    : public TTimeoutOptions
{ };

struct TPausePipelineOptions
    : public TTimeoutOptions
{ };

struct TGetPipelineStatusOptions
    : public TTimeoutOptions
{ };

struct TPipelineStatus
{
    NFlow::EPipelineState State;
};

struct IFlowClient
{
    ~IFlowClient() = default;

    virtual TFuture<void> StartPipeline(
        const NYPath::TYPath& pipelinePath,
        const TStartPipelineOptions& options = {}) = 0;

    virtual TFuture<void> StopPipeline(
        const NYPath::TYPath& pipelinePath,
        const TStopPipelineOptions& options = {}) = 0;

    virtual TFuture<void> PausePipeline(
        const NYPath::TYPath& pipelinePath,
        const TPausePipelineOptions& options = {}) = 0;

    virtual TFuture<TPipelineStatus> GetPipelineStatus(
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineStatusOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
