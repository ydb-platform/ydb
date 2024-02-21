#include "flow_commands.h"

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ExecuteGetPipelineSpecCommand(
    const ICommandContextPtr& context,
    const TYPath& specPath,
    const auto& specGetterOptions,
    auto specGetter)
{
    auto client = context->GetClient();
    auto result = WaitFor(specGetter(client, specGetterOptions))
        .ValueOrThrow();

    auto spec = SyncYPathGet(ConvertToNode(result.Spec), specPath);

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("spec").Value(spec)
                .Item("version").Value(result.Version)
            .EndMap();
        });
}

void ExecuteSetPipelineSpecCommand(
    const ICommandContextPtr& context,
    const TYPath& specPath,
    const auto& specGetterOptions,
    auto specGetter,
    const auto& specSetterOptions,
    auto specSetter)
{
    auto client = context->GetClient();
    auto spec = context->ConsumeInputValue();

    auto setResult = [&] {
        if (specPath.empty()) {
            return WaitFor(specSetter(client, spec, specSetterOptions))
                .ValueOrThrow();
        } else {
            auto getResult = WaitFor(specGetter(client, specGetterOptions))
                .ValueOrThrow();

            if (specSetterOptions.ExpectedVersion && getResult.Version != *specSetterOptions.ExpectedVersion) {
                THROW_ERROR_EXCEPTION(
                    NFlow::EErrorCode::SpecVersionMismatch,
                    "Spec version mismatch: expected %v, got %v",
                    *specSetterOptions.ExpectedVersion,
                    getResult.Version);
            }

            auto fullSpec = ConvertToNode(getResult.Spec);
            SyncYPathSet(fullSpec, specPath, spec);

            auto adjustedOptions = specSetterOptions;
            adjustedOptions.ExpectedVersion = getResult.Version;
            return WaitFor(specSetter(client, ConvertToYsonString(fullSpec), adjustedOptions))
                .ValueOrThrow();
        }
    }();

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("version").Value(setResult.Version)
            .EndMap();
        });
}

void ExecuteRemovePipelineSpecCommand(
    const ICommandContextPtr& context,
    const TYPath& specPath,
    const auto& specGetterOptions,
    auto specGetter,
    const auto& specSetterOptions,
    auto specSetter)
{
    auto client = context->GetClient();
    auto getResult = WaitFor(specGetter(client, specGetterOptions))
        .ValueOrThrow();

    if (specSetterOptions.ExpectedVersion && getResult.Version != *specSetterOptions.ExpectedVersion) {
        THROW_ERROR_EXCEPTION(
            NFlow::EErrorCode::SpecVersionMismatch,
            "Spec version mismatch: expected %v, got %v",
            *specSetterOptions.ExpectedVersion,
            getResult.Version);
    }

    auto fullSpec = ConvertToNode(getResult.Spec);
    SyncYPathRemove(fullSpec, specPath);

    auto adjustedOptions = specSetterOptions;
    adjustedOptions.ExpectedVersion = getResult.Version;
    auto setResult = WaitFor(specSetter(client, ConvertToYsonString(fullSpec), adjustedOptions))
        .ValueOrThrow();

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("version").Value(setResult.Version)
            .EndMap();
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TPipelineCommandBase::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_path", &TThis::PipelinePath);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_path", &TThis::SpecPath)
        .Default();
}

void TGetPipelineSpecCommand::DoExecute(ICommandContextPtr context)
{
    ExecuteGetPipelineSpecCommand(
        context,
        SpecPath,
        Options,
        [&] (const auto& client, const auto& options) { return client->GetPipelineSpec(PipelinePath, options); });
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_path", &TThis::SpecPath)
        .Default();
    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<std::optional<NFlow::TVersion>>(
        "expected_version",
        [] (TThis* command) -> auto& {
            return command->Options.ExpectedVersion;
        })
        .Optional(/*init*/ false);
}

void TSetPipelineSpecCommand::DoExecute(ICommandContextPtr context)
{
    ExecuteSetPipelineSpecCommand(
        context,
        SpecPath,
        TGetPipelineSpecOptions(),
        [&] (const auto& client, const auto& options) { return client->GetPipelineSpec(PipelinePath, options); },
        Options,
        [&] (const auto& client, const auto& spec, const auto& options) { return client->SetPipelineSpec(PipelinePath, spec, options); });
}

////////////////////////////////////////////////////////////////////////////////

void TRemovePipelineSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_path", &TThis::SpecPath);
    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<std::optional<NFlow::TVersion>>(
        "expected_version",
        [] (TThis* command) -> auto& {
            return command->Options.ExpectedVersion;
        })
        .Optional(/*init*/ false);
}

void TRemovePipelineSpecCommand::DoExecute(ICommandContextPtr context)
{
    ExecuteRemovePipelineSpecCommand(
        context,
        SpecPath,
        TGetPipelineSpecOptions(),
        [&] (const auto& client, const auto& options) { return client->GetPipelineSpec(PipelinePath, options); },
        Options,
        [&] (const auto& client, const auto& spec, const auto& options) { return client->SetPipelineSpec(PipelinePath, spec, options); });
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineDynamicSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_path", &TThis::SpecPath)
        .Default();
}

void TGetPipelineDynamicSpecCommand::DoExecute(ICommandContextPtr context)
{
    ExecuteGetPipelineSpecCommand(
        context,
        SpecPath,
        Options,
        [&] (const auto& client, const auto& options) { return client->GetPipelineDynamicSpec(PipelinePath, options); });
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineDynamicSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_path", &TThis::SpecPath)
        .Default();
    registrar.ParameterWithUniversalAccessor<std::optional<NFlow::TVersion>>(
        "expected_version",
        [] (TThis* command) -> auto& {
            return command->Options.ExpectedVersion;
        })
        .Optional(/*init*/ false);
}

void TSetPipelineDynamicSpecCommand::DoExecute(ICommandContextPtr context)
{
    ExecuteSetPipelineSpecCommand(
        context,
        SpecPath,
        TGetPipelineDynamicSpecOptions(),
        [&] (const auto& client, const auto& options) { return client->GetPipelineDynamicSpec(PipelinePath, options); },
        Options,
        [&] (const auto& client, const auto& spec, const auto& options) { return client->SetPipelineDynamicSpec(PipelinePath, spec, options); });
}

////////////////////////////////////////////////////////////////////////////////

void TRemovePipelineDynamicSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_path", &TThis::SpecPath);
    registrar.ParameterWithUniversalAccessor<std::optional<NFlow::TVersion>>(
        "expected_version",
        [] (TThis* command) -> auto& {
            return command->Options.ExpectedVersion;
        })
        .Optional(/*init*/ false);
}

void TRemovePipelineDynamicSpecCommand::DoExecute(ICommandContextPtr context)
{
    ExecuteRemovePipelineSpecCommand(
        context,
        SpecPath,
        TGetPipelineDynamicSpecOptions(),
        [&] (const auto& client, const auto& options) { return client->GetPipelineDynamicSpec(PipelinePath, options); },
        Options,
        [&] (const auto& client, const auto& spec, const auto& options) { return client->SetPipelineDynamicSpec(PipelinePath, spec, options); });
}

////////////////////////////////////////////////////////////////////////////////

void TStartPipelineCommand::Register(TRegistrar /*registrar*/)
{ }

void TStartPipelineCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    WaitFor(client->StartPipeline(PipelinePath, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TStopPipelineCommand::Register(TRegistrar /*registrar*/)
{ }

void TStopPipelineCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    WaitFor(client->StopPipeline(PipelinePath, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TPausePipelineCommand::Register(TRegistrar /*registrar*/)
{ }

void TPausePipelineCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    WaitFor(client->PausePipeline(PipelinePath, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
