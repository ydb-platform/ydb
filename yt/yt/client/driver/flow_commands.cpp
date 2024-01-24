#include "flow_commands.h"

namespace NYT::NDriver {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TStartPipelineCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_path", &TThis::PipelinePath);
}

void TStartPipelineCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    WaitFor(client->StartPipeline(PipelinePath))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TStopPipelineCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_path", &TThis::PipelinePath);
}

void TStopPipelineCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    WaitFor(client->StopPipeline(PipelinePath))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TPausePipelineCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_path", &TThis::PipelinePath);
}

void TPausePipelineCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    WaitFor(client->PausePipeline(PipelinePath))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
