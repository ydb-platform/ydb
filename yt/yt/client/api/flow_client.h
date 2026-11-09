#pragma once

#include "client_common.h"

#include <yt/yt/flow/library/cpp/client/public.h>

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

DEFINE_ENUM(EDynamicSpecView,
    (Effective)
    (Base)
);

struct TGetPipelineDynamicSpecOptions
    : public TTimeoutOptions
{
    EDynamicSpecView View = EDynamicSpecView::Effective;
    bool Blame = false;
};

struct TGetPipelineDynamicSpecResult
{
    NFlow::TVersion Version;
    NYson::TYsonString Spec;
    NYson::TYsonString Blame;
};

struct TSetPipelineDynamicSpecOptions
    : public TTimeoutOptions
{
    std::optional<NFlow::TVersion> ExpectedVersion;
};

//! Require a nonempty reason for a DynamicSpec patch.
void ValidatePipelineDynamicSpecPatchComment(const std::optional<std::string>& comment);

struct TSetPipelineDynamicSpecResult
{
    NFlow::TVersion Version;
};

struct TPipelineDynamicSpecPatchChange
{
    NYPath::TYPath Path;
    NYson::TYsonString Value;
};

struct TPipelineDynamicSpecPatch
{
    NFlow::TVersion PatchId;
    std::vector<TPipelineDynamicSpecPatchChange> Changes;
    std::optional<std::string> Comment;
    TInstant Timestamp;
};

struct TGetPipelineDynamicSpecPatchesOptions
    : public TTimeoutOptions
{
    std::optional<NFlow::TVersion> Patch;
};

struct TGetPipelineDynamicSpecPatchesResult
{
    NFlow::TVersion Version;
    std::vector<TPipelineDynamicSpecPatch> Patches;
};

struct TPatchPipelineDynamicSpecOptions
    : public TTimeoutOptions
{
    std::optional<NYPath::TYPath> SetPath;
    NYson::TYsonString SetValue;
    std::vector<TPipelineDynamicSpecPatchChange> SetChanges;
    std::optional<NFlow::TVersion> Patch;
    std::optional<std::string> Comment;
    std::optional<NFlow::TVersion> ExpectedVersion;
};

struct TPatchPipelineDynamicSpecResult
{
    NFlow::TVersion Version;
    NFlow::TVersion PatchId;
};

struct TCancelPipelineDynamicSpecPatchOptions
    : public TTimeoutOptions
{
    std::optional<NFlow::TVersion> ExpectedVersion;
};

struct TResetPipelineDynamicSpecOptions
    : public TTimeoutOptions
{
    std::optional<NFlow::TVersion> ExpectedVersion;
};

struct TUpdatePipelineDynamicSpecPatchResult
{
    NFlow::TVersion Version;
};

void Serialize(const TPipelineDynamicSpecPatchChange& value, NYson::IYsonConsumer* consumer);
void Deserialize(TPipelineDynamicSpecPatchChange& value, NYTree::INodePtr node);
void Serialize(const TPipelineDynamicSpecPatch& value, NYson::IYsonConsumer* consumer);
void Deserialize(TPipelineDynamicSpecPatch& value, NYTree::INodePtr node);
void Serialize(const TGetPipelineDynamicSpecPatchesResult& value, NYson::IYsonConsumer* consumer);
void Deserialize(TGetPipelineDynamicSpecPatchesResult& value, NYTree::INodePtr node);

struct TGetFlowViewOptions
    : public TTimeoutOptions
{
    bool Cache = true;
};

struct TGetFlowViewResult
{
    NYson::TYsonString FlowViewPart;
};

struct TFlowExecuteOptions
    : public TTimeoutOptions
{ };

struct TFlowExecuteResult
{
    NYson::TYsonString Result;
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
        const TGetPipelineDynamicSpecOptions& options = {});

    virtual TFuture<TSetPipelineDynamicSpecResult> SetPipelineDynamicSpec(
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineDynamicSpecOptions& options = {}) = 0;

    TFuture<TGetPipelineDynamicSpecPatchesResult> GetPipelineDynamicSpecPatches(
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineDynamicSpecPatchesOptions& options = {});

    TFuture<TPatchPipelineDynamicSpecResult> PatchPipelineDynamicSpec(
        const NYPath::TYPath& pipelinePath,
        const TPatchPipelineDynamicSpecOptions& options);

    TFuture<TUpdatePipelineDynamicSpecPatchResult> CancelPipelineDynamicSpecPatch(
        const NYPath::TYPath& pipelinePath,
        NFlow::TVersion version,
        const TCancelPipelineDynamicSpecPatchOptions& options = {});

    TFuture<TUpdatePipelineDynamicSpecPatchResult> ResetPipelineDynamicSpec(
        const NYPath::TYPath& pipelinePath,
        const TResetPipelineDynamicSpecOptions& options = {});

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

    virtual TFuture<TFlowExecuteResult> FlowExecute(
        const NYPath::TYPath& pipelinePath,
        const std::string& command,
        const NYson::TYsonString& argument,
        const TFlowExecuteOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
