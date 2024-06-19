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

struct TGetPipelineStateOptions
    : public TTimeoutOptions
{ };

struct TPipelineState
{
    NFlow::EPipelineState State;
};

struct TGetPipelineSpecOptions
    : public TTimeoutOptions
{ };

struct TGetPipelineSpecResult
{
    NFlow::TVersion Version;
    NYson::TYsonString Spec;
};

struct TSetPipelineSpecOptions
    : public TTimeoutOptions
{
    // Update spec even if pipeline is not stopped, just paused.
    bool Force = false;
    std::optional<NFlow::TVersion> ExpectedVersion;
};

struct TSetPipelineSpecResult
{
    NFlow::TVersion Version;
};

struct TGetPipelineDynamicSpecOptions
    : public TTimeoutOptions
{ };

struct TGetPipelineDynamicSpecResult
{
    NFlow::TVersion Version;
    NYson::TYsonString Spec;
};

struct TSetPipelineDynamicSpecOptions
    : public TTimeoutOptions
{
    std::optional<NFlow::TVersion> ExpectedVersion;
};

struct TSetPipelineDynamicSpecResult
{
    NFlow::TVersion Version;
};

struct TGetFlowViewOptions
    : public TTimeoutOptions
{ };

struct TGetFlowViewResult
{
    NYson::TYsonString FlowViewPart;
};

struct IFlowClient
{
    ~IFlowClient() = default;

    virtual TFuture<TGetPipelineSpecResult> GetPipelineSpec(
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineSpecOptions& options = {}) = 0;

    virtual TFuture<TSetPipelineSpecResult> SetPipelineSpec(
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineSpecOptions& options = {}) = 0;

    virtual TFuture<TGetPipelineDynamicSpecResult> GetPipelineDynamicSpec(
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineDynamicSpecOptions& options = {}) = 0;

    virtual TFuture<TSetPipelineDynamicSpecResult> SetPipelineDynamicSpec(
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineDynamicSpecOptions& options = {}) = 0;

    virtual TFuture<void> StartPipeline(
        const NYPath::TYPath& pipelinePath,
        const TStartPipelineOptions& options = {}) = 0;

    virtual TFuture<void> StopPipeline(
        const NYPath::TYPath& pipelinePath,
        const TStopPipelineOptions& options = {}) = 0;

    virtual TFuture<void> PausePipeline(
        const NYPath::TYPath& pipelinePath,
        const TPausePipelineOptions& options = {}) = 0;

    virtual TFuture<TPipelineState> GetPipelineState(
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineStateOptions& options = {}) = 0;

    virtual TFuture<TGetFlowViewResult> GetFlowView(
        const NYPath::TYPath& pipelinePath,
        const NYPath::TYPath& viewPath,
        const TGetFlowViewOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
