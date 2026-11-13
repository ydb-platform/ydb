#include "flow_client.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NFlow;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TOptions>
TFlowExecuteOptions MakeFlowExecuteOptions(const TOptions& options)
{
    TFlowExecuteOptions executeOptions;
    static_cast<TTimeoutOptions&>(executeOptions) = static_cast<const TTimeoutOptions&>(options);
    return executeOptions;
}

TFuture<TUpdatePipelineDynamicSpecPatchResult> ParseUpdateResult(
    const TFuture<TFlowExecuteResult>& future)
{
    return future.Apply(BIND([] (const TFlowExecuteResult& result) {
        return TUpdatePipelineDynamicSpecPatchResult{
            .Version = ConvertTo<IMapNodePtr>(result.Result)->GetChildValueOrThrow<TVersion>("version"),
        };
    }));
}

} // namespace

void ValidatePipelineDynamicSpecPatchComment(const std::optional<std::string>& comment)
{
    THROW_ERROR_EXCEPTION_UNLESS(comment && !comment->empty(),
        "A nonempty comment must be specified for a DynamicSpec patch");
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TPipelineDynamicSpecPatchChange& value, IYsonConsumer* consumer)
{
    // clang-format off
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("path").Value(value.Path)
            .Item("value").Value(value.Value)
        .EndMap();
    // clang-format on
}

void Deserialize(TPipelineDynamicSpecPatchChange& value, INodePtr node)
{
    auto map = node->AsMap();
    value.Path = map->GetChildValueOrThrow<TYPath>("path");
    value.Value = ConvertToYsonString(map->GetChildOrThrow("value"));
}

void Serialize(const TPipelineDynamicSpecPatch& value, IYsonConsumer* consumer)
{
    // clang-format off
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("patch_id").Value(value.PatchId)
            .Item("changes").Value(value.Changes)
            .Item("comment").Value(value.Comment)
            .Item("timestamp").Value(value.Timestamp)
        .EndMap();
    // clang-format on
}

void Deserialize(TPipelineDynamicSpecPatch& value, INodePtr node)
{
    auto map = node->AsMap();
    value.PatchId = map->GetChildValueOrThrow<TVersion>("patch_id");
    value.Comment = map->FindChild("comment")
        ? ConvertTo<std::optional<std::string>>(map->FindChild("comment"))
        : std::nullopt;
    value.Timestamp = map->GetChildValueOrThrow<TInstant>("timestamp");
    value.Changes = map->GetChildValueOrThrow<std::vector<TPipelineDynamicSpecPatchChange>>("changes");
}

void Serialize(const TGetPipelineDynamicSpecPatchesResult& value, IYsonConsumer* consumer)
{
    // clang-format off
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("version").Value(value.Version)
            .Item("patches").Value(value.Patches)
        .EndMap();
    // clang-format on
}

void Deserialize(TGetPipelineDynamicSpecPatchesResult& value, INodePtr node)
{
    auto map = node->AsMap();
    value.Version = map->GetChildValueOrThrow<TVersion>("version");
    value.Patches = map->GetChildValueOrThrow<std::vector<TPipelineDynamicSpecPatch>>("patches");
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TGetPipelineDynamicSpecResult> IFlowClient::GetPipelineDynamicSpec(
    const TYPath& pipelinePath,
    const TGetPipelineDynamicSpecOptions& options)
{
    // clang-format off
    auto argument = BuildYsonStringFluently()
        .BeginMap()
            .DoIf(options.View != EDynamicSpecView::Effective, [&] (auto fluent) {
                fluent.Item("view").Value(options.View);
            })
            .DoIf(options.Blame, [&] (auto fluent) {
                fluent.Item("blame").Value(true);
            })
        .EndMap();
    // clang-format on
    return FlowExecute(pipelinePath, "get-pipeline-dynamic-spec", argument, MakeFlowExecuteOptions(options))
        .Apply(BIND([] (const TFlowExecuteResult& result) {
            auto map = ConvertTo<IMapNodePtr>(result.Result);
            return TGetPipelineDynamicSpecResult{
                .Version = map->GetChildValueOrThrow<TVersion>("version"),
                .Spec = ConvertToYsonString(map->GetChildOrThrow("spec")),
                .Blame = map->FindChild("blame")
                    ? ConvertToYsonString(map->GetChildOrThrow("blame"))
                    : TYsonString{},
            };
        }));
}

TFuture<TGetPipelineDynamicSpecPatchesResult> IFlowClient::GetPipelineDynamicSpecPatches(
    const TYPath& pipelinePath,
    const TGetPipelineDynamicSpecPatchesOptions& options)
{
    // clang-format off
    auto argument = BuildYsonStringFluently()
        .BeginMap()
            .Item("patch").Value(options.Patch)
        .EndMap();
    // clang-format on
    return FlowExecute(pipelinePath, "get-pipeline-dynamic-spec-patches", argument, MakeFlowExecuteOptions(options))
        .Apply(BIND([] (const TFlowExecuteResult& result) {
            return ConvertTo<TGetPipelineDynamicSpecPatchesResult>(result.Result);
        }));
}

TFuture<TPatchPipelineDynamicSpecResult> IFlowClient::PatchPipelineDynamicSpec(
    const TYPath& pipelinePath,
    const TPatchPipelineDynamicSpecOptions& options)
{
    try {
        THROW_ERROR_EXCEPTION_UNLESS(options.ExpectedVersion, "expected_version is required for a DynamicSpec patch");
        THROW_ERROR_EXCEPTION_IF(options.SetPath && !options.SetChanges.empty(),
            "Specify either set_path or set_changes");
        THROW_ERROR_EXCEPTION_IF(!options.Patch && !options.SetPath && options.SetChanges.empty(),
            "A new patch requires set_path or set_changes");
        if (!options.Patch || options.SetPath || !options.SetChanges.empty() || options.Comment) {
            ValidatePipelineDynamicSpecPatchComment(options.Comment);
        }
        // clang-format off
        auto argument = BuildYsonStringFluently()
            .BeginMap()
                .Item("set_path").Value(options.SetPath)
                .DoIf(options.SetPath.has_value(), [&] (auto fluent) {
                    fluent.Item("set_value").Value(options.SetValue);
                })
                .DoIf(!options.SetChanges.empty(), [&] (auto fluent) {
                    fluent.Item("set_changes").Value(options.SetChanges);
                })
                .Item("patch").Value(options.Patch)
                .Item("comment").Value(options.Comment)
                .Item("expected_version").Value(options.ExpectedVersion)
            .EndMap();
        // clang-format on
        return FlowExecute(pipelinePath, "patch-pipeline-dynamic-spec", argument, MakeFlowExecuteOptions(options))
            .Apply(BIND([] (const TFlowExecuteResult& result) {
                auto map = ConvertTo<IMapNodePtr>(result.Result);
                return TPatchPipelineDynamicSpecResult{
                    .Version = map->GetChildValueOrThrow<TVersion>("version"),
                    .PatchId = map->GetChildValueOrThrow<TVersion>("patch_id"),
                };
            }));
    } catch (const std::exception& ex) {
        return MakeFuture<TPatchPipelineDynamicSpecResult>(TError(ex));
    }
}

TFuture<TUpdatePipelineDynamicSpecPatchResult> IFlowClient::CancelPipelineDynamicSpecPatch(
    const TYPath& pipelinePath,
    TVersion version,
    const TCancelPipelineDynamicSpecPatchOptions& options)
{
    try {
        THROW_ERROR_EXCEPTION_UNLESS(options.ExpectedVersion,
            "expected_version is required for a DynamicSpec patch operation");
        // clang-format off
        auto argument = BuildYsonStringFluently()
            .BeginMap()
                .Item("patch").Value(version)
                .Item("expected_version").Value(options.ExpectedVersion)
            .EndMap();
        // clang-format on
        return ParseUpdateResult(FlowExecute(
            pipelinePath,
            "cancel-pipeline-dynamic-spec-patch",
            argument,
            MakeFlowExecuteOptions(options)));
    } catch (const std::exception& ex) {
        return MakeFuture<TUpdatePipelineDynamicSpecPatchResult>(TError(ex));
    }
}

TFuture<TUpdatePipelineDynamicSpecPatchResult> IFlowClient::ResetPipelineDynamicSpec(
    const TYPath& pipelinePath,
    const TResetPipelineDynamicSpecOptions& options)
{
    try {
        THROW_ERROR_EXCEPTION_UNLESS(options.ExpectedVersion,
            "expected_version is required for a DynamicSpec patch operation");
        // clang-format off
        auto argument = BuildYsonStringFluently()
            .BeginMap()
                .Item("expected_version").Value(options.ExpectedVersion)
            .EndMap();
        // clang-format on
        return ParseUpdateResult(FlowExecute(
            pipelinePath,
            "reset-pipeline-dynamic-spec",
            argument,
            MakeFlowExecuteOptions(options)));
    } catch (const std::exception& ex) {
        return MakeFuture<TUpdatePipelineDynamicSpecPatchResult>(TError(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
