#include "scheduler_commands.h"
#include "driver.h"

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, JobShellStructuredLogger, "JobShell");

////////////////////////////////////////////////////////////////////////////////

void TDumpJobContextCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);
    registrar.Parameter("path", &TThis::Path);
}

void TDumpJobContextCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->DumpJobContext(JobId, Path))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGetJobInputCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.ParameterWithUniversalAccessor<EJobSpecSource>(
        "job_spec_source",
        [] (TThis* command) -> auto& {
            return command->Options.JobSpecSource;
        })
        .Default(EJobSpecSource::Auto);
}

void TGetJobInputCommand::DoExecute(ICommandContextPtr context)
{
    auto jobInputReader = WaitFor(context->GetClient()->GetJobInput(JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    PipeInputToOutput(jobInputReader, output);
}

////////////////////////////////////////////////////////////////////////////////

void TGetJobInputPathsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.ParameterWithUniversalAccessor<EJobSpecSource>(
        "job_spec_source",
        [] (TThis* command) -> auto& {
            return command->Options.JobSpecSource;
        })
        .Default(EJobSpecSource::Auto);
}

void TGetJobInputPathsCommand::DoExecute(ICommandContextPtr context)
{
    auto inputPaths = WaitFor(context->GetClient()->GetJobInputPaths(JobId, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(std::move(inputPaths));
}

////////////////////////////////////////////////////////////////////////////////

void TGetJobSpecCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.ParameterWithUniversalAccessor<EJobSpecSource>(
        "job_spec_source",
        [] (TThis* command) -> auto& {
            return command->Options.JobSpecSource;
        })
        .Default(EJobSpecSource::Auto);

    registrar.ParameterWithUniversalAccessor<bool>(
        "omit_node_directory",
        [] (TThis* command) -> auto& {
            return command->Options.OmitNodeDirectory;
        })
        .Default(true);

    registrar.ParameterWithUniversalAccessor<bool>(
        "omit_input_table_specs",
        [] (TThis* command) -> auto& {
            return command->Options.OmitInputTableSpecs;
        })
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "omit_output_table_specs",
        [] (TThis* command) -> auto& {
            return command->Options.OmitOutputTableSpecs;
        })
        .Default(false);
}

void TGetJobSpecCommand::DoExecute(ICommandContextPtr context)
{
    auto jobSpec = WaitFor(context->GetClient()->GetJobSpec(JobId, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(std::move(jobSpec));
}

////////////////////////////////////////////////////////////////////////////////

void TGetJobStderrCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);
}

void TGetJobStderrCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetJobStderr(OperationIdOrAlias, JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    WaitFor(output->Write(result))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TGetJobFailContextCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);
}

void TGetJobFailContextCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetJobFailContext(OperationIdOrAlias, JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    WaitFor(output->Write(result))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TListOperationsCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "from_time",
        [] (TThis* command) -> auto& {
            return command->Options.FromTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "to_time",
        [] (TThis* command) -> auto& {
            return command->Options.ToTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "cursor_time",
        [] (TThis* command) -> auto& {
            return command->Options.CursorTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EOperationSortDirection>(
        "cursor_direction",
        [] (TThis* command) -> auto& {
            return command->Options.CursorDirection;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "user",
        [] (TThis* command) -> auto& {
            return command->Options.UserFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TListOperationsAccessFilterPtr>(
        "access",
        [] (TThis* command) -> auto& {
            return command->Options.AccessFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<EOperationState>>(
        "state",
        [] (TThis* command) -> auto& {
            return command->Options.StateFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<EOperationType>>(
        "type",
        [] (TThis* command) -> auto& {
            return command->Options.TypeFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "filter",
        [] (TThis* command) -> auto& {
            return command->Options.SubstrFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "pool_tree",
        [] (TThis* command) -> auto& {
            return command->Options.PoolTree;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "pool",
        [] (TThis* command) -> auto& {
            return command->Options.Pool;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "with_failed_jobs",
        [] (TThis* command) -> auto& {
            return command->Options.WithFailedJobs;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "include_archive",
        [] (TThis* command) -> auto& {
            return command->Options.IncludeArchive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "include_counters",
        [] (TThis* command) -> auto& {
            return command->Options.IncludeCounters;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<ui64>(
        "limit",
        [] (TThis* command) -> auto& {
            return command->Options.Limit;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<THashSet<TString>>>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TDuration>(
        "archive_fetching_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.ArchiveFetchingTimeout;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("enable_ui_mode", &TThis::EnableUIMode)
        .Optional();
}

void TListOperationsCommand::BuildOperations(const TListOperationsResult& result, TFluentMap fluent)
{
    bool needType = !Options.Attributes || Options.Attributes->contains("type");
    // COMPAT(levysotsky): "operation_type" is a deprecated synonym for "type".
    bool needOperationType = !Options.Attributes || Options.Attributes->contains("operation_type");

    if (EnableUIMode) {
        fluent
            .Item("operations")
                .BeginAttributes()
                    .Item("incomplete").Value(result.Incomplete)
                .EndAttributes()
                .DoListFor(result.Operations, [&] (TFluentList fluent, const TOperation& operation) {
                    fluent.Item().Do([&] (TFluentAny fluent) {
                        Serialize(operation, fluent.GetConsumer(), needType, needOperationType, /* idWithAttributes */ true);
                    });
                });
    } else {
        fluent
            .Item("operations")
                .DoListFor(result.Operations, [&] (TFluentList fluent, const TOperation& operation) {
                    fluent.Item().Do([&] (TFluentAny fluent) {
                        Serialize(operation, fluent.GetConsumer(), needType, needOperationType);
                    });
                })
            .Item("incomplete").Value(result.Incomplete);
    }
}

void TListOperationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListOperations(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Do(std::bind(&TListOperationsCommand::BuildOperations, this, result, std::placeholders::_1))
            .DoIf(result.PoolTreeCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("pool_tree_counts").BeginMap()
                .DoFor(*result.PoolTreeCounts, [] (TFluentMap fluent, const auto& item) {
                    fluent.Item(item.first).Value(item.second);
                })
                .EndMap();
            })
            .DoIf(result.PoolCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("pool_counts").BeginMap()
                .DoFor(*result.PoolCounts, [] (TFluentMap fluent, const auto& item) {
                    fluent.Item(item.first).Value(item.second);
                })
                .EndMap();
            })
            .DoIf(result.UserCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("user_counts").BeginMap()
                .DoFor(*result.UserCounts, [] (TFluentMap fluent, const auto& item) {
                    fluent.Item(item.first).Value(item.second);
                })
                .EndMap();
            })
            .DoIf(result.StateCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("state_counts").BeginMap()
                .DoFor(TEnumTraits<EOperationState>::GetDomainValues(), [&result] (TFluentMap fluent, const EOperationState& item) {
                    i64 count = (*result.StateCounts)[item];
                    if (count) {
                        fluent.Item(FormatEnum(item)).Value(count);
                    }
                })
                .EndMap();
            })
            .DoIf(result.TypeCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("type_counts").BeginMap()
                .DoFor(TEnumTraits<EOperationType>::GetDomainValues(), [&result] (TFluentMap fluent, const EOperationType& item) {
                    i64 count = (*result.TypeCounts)[item];
                    if (count) {
                        fluent.Item(FormatEnum(item)).Value(count);
                    }
                })
                .EndMap();
            })
            .OptionalItem("failed_jobs_count", result.FailedJobsCount)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TListJobsCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<EJobType>>(
        "type",
        [] (TThis* command) -> auto& {return command->Options.Type; })
        .Alias("job_type")
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<EJobState>>(
        "state",
        [] (TThis* command) -> auto& {return command->Options.State; })
        .Alias("job_state")
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "address",
        [] (TThis* command) -> auto& {return command->Options.Address; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "with_stderr",
        [] (TThis* command) -> auto& {return command->Options.WithStderr; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "with_spec",
        [] (TThis* command) -> auto& {return command->Options.WithSpec; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "with_fail_context",
        [] (TThis* command) -> auto& {return command->Options.WithFailContext; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "with_competitors",
        [] (TThis* command) -> auto& { return command->Options.WithCompetitors; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "with_monitoring_descriptor",
        [] (TThis* command) -> auto& { return command->Options.WithMonitoringDescriptor; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TJobId>(
        "job_competition_id",
        [] (TThis* command) -> auto& { return command->Options.JobCompetitionId; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "task_name",
        [] (TThis* command) -> auto& { return command->Options.TaskName; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EJobSortField>(
        "sort_field",
        [] (TThis* command) -> auto& { return command->Options.SortField; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EJobSortDirection>(
        "sort_order",
        [] (TThis* command) -> auto& { return command->Options.SortOrder; })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<i64>(
        "limit",
        [] (TThis* command) -> auto& {
            return command->Options.Limit;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<i64>(
        "offset",
        [] (TThis* command) -> auto& {
            return command->Options.Offset;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EDataSource>(
        "data_source",
        [] (TThis* command) -> auto& {
            return command->Options.DataSource;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "include_cypress",
        [] (TThis* command) -> auto& {
            return command->Options.IncludeCypress;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "include_controller_agent",
        [] (TThis* command) -> auto& {
            return command->Options.IncludeControllerAgent;
        })
        .Alias("include_runtime")
        .Alias("include_scheduler")
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "include_archive",
        [] (TThis* command) -> auto& {
            return command->Options.IncludeArchive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TDuration>(
        "running_jobs_lookbehind_period",
        [] (TThis* command) -> auto& {
            return command->Options.RunningJobsLookbehindPeriod;
        })
        .Optional(/*init*/ false);
}

void TListJobsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListJobs(OperationIdOrAlias, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("jobs").DoListFor(result.Jobs, [] (TFluentList fluent, const TJob& job) {
                fluent
                    .Item().Do([&] (TFluentAny innerFluent) {
                        Serialize(job, innerFluent.GetConsumer(), TStringBuf("id"));
                    });
            })
            .Item("cypress_job_count").Value(result.CypressJobCount)
            // COMPAT(asaitgalin): Remove it in favor of controller_agent_job_count
            .Item("scheduler_job_count").Value(result.ControllerAgentJobCount)
            .Item("controller_agent_job_count").Value(result.ControllerAgentJobCount)
            .Item("archive_job_count").Value(result.ArchiveJobCount)
            .Item("type_counts").DoMapFor(TEnumTraits<NJobTrackerClient::EJobType>::GetDomainValues(), [&] (TFluentMap fluent, const auto& item) {
                i64 count = result.Statistics.TypeCounts[item];
                if (count) {
                    fluent.Item(FormatEnum(item)).Value(count);
                }
            })
            .Item("state_counts").DoMapFor(TEnumTraits<NJobTrackerClient::EJobState>::GetDomainValues(), [&] (TFluentMap fluent, const auto& item) {
                i64 count = result.Statistics.StateCounts[item];
                if (count) {
                    fluent.Item(FormatEnum(item)).Value(count);
                }
            })
            .Item("errors").Value(result.Errors)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TGetJobCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.ParameterWithUniversalAccessor<std::optional<THashSet<TString>>>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);
}

void TGetJobCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetJob(OperationIdOrAlias, JobId, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

void TAbandonJobCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);
}

void TAbandonJobCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbandonJob(JobId))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TPollJobShellCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);
    registrar.Parameter("parameters", &TThis::Parameters);
    registrar.Parameter("shell_name", &TThis::ShellName)
        .Default();

    registrar.Postprocessor([] (TThis* command) {
        // Compatibility with initial job shell protocol.
        if (command->Parameters->GetType() == NYTree::ENodeType::String) {
            command->Parameters = NYTree::ConvertToNode(NYson::TYsonString(command->Parameters->AsString()->GetValue()));
        }
    });
}

void TPollJobShellCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResponse = context->GetClient()->PollJobShell(
        JobId,
        ShellName,
        ConvertToYsonString(Parameters),
        Options);
    auto response = WaitFor(asyncResponse)
        .ValueOrThrow();

    if (response.LoggingContext) {
        LogStructuredEventFluently(JobShellStructuredLogger(), NLogging::ELogLevel::Info)
            .Do([&] (TFluentMap fluent) {
                fluent.GetConsumer()->OnRaw(response.LoggingContext);
            })
            .Item("user").Value(context->Request().AuthenticatedUser)
            .DoIf(static_cast<bool>(context->Request().UserRemoteAddress), [&] (TFluentMap fluent) {
                fluent.Item("remote_address").Value(ToString(context->Request().UserRemoteAddress));
            });
    }

    ProduceSingleOutputValue(context, "result", response.Result);
}

////////////////////////////////////////////////////////////////////////////////

void TAbortJobCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.ParameterWithUniversalAccessor<std::optional<TDuration>>(
        "interrupt_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.InterruptTimeout;
        })
        .Optional(/*init*/ false);
}

void TAbortJobCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbortJob(JobId, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TDumpJobProxyLogCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);
    registrar.Parameter("operation_id", &TThis::OperationId);
    registrar.Parameter("path", &TThis::Path);
}

void TDumpJobProxyLogCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->DumpJobProxyLog(JobId, OperationId, Path))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TStartOperationCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType);
}

////////////////////////////////////////////////////////////////////////////////

void TStartOperationCommandBase::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
}

void TStartOperationCommandBase::DoExecute(ICommandContextPtr context)
{
    auto asyncOperationId = context->GetClient()->StartOperation(
        OperationType,
        ConvertToYsonString(Spec),
        Options);

    auto operationId = WaitFor(asyncOperationId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "operation_id", operationId);
}

////////////////////////////////////////////////////////////////////////////////

void TMapCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::Map);
}

////////////////////////////////////////////////////////////////////////////////

void TMergeCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::Merge);
}

////////////////////////////////////////////////////////////////////////////////

void TSortCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::Sort);
}

////////////////////////////////////////////////////////////////////////////////

void TEraseCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::Erase);
}

////////////////////////////////////////////////////////////////////////////////

void TReduceCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::Reduce);
}

////////////////////////////////////////////////////////////////////////////////

void TJoinReduceCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::JoinReduce);
}

////////////////////////////////////////////////////////////////////////////////

void TMapReduceCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::MapReduce);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyCommand::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("operation_type", &TThis::OperationType)
        .Default(EOperationType::RemoteCopy);
}

////////////////////////////////////////////////////////////////////////////////

void TAbortOperationCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "abort_message",
        [] (TThis* command) -> auto& {
            return command->Options.AbortMessage;
        })
        .Optional(/*init*/ false);
}

void TAbortOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbortOperation(OperationIdOrAlias, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendOperationCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "abort_running_jobs",
        [] (TThis* command) -> auto& {
            return command->Options.AbortRunningJobs;
        })
        .Optional(/*init*/ false);
}

void TSuspendOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendOperation(OperationIdOrAlias, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TResumeOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeOperation(OperationIdOrAlias))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCompleteOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->CompleteOperation(OperationIdOrAlias))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TUpdateOperationParametersCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters);
}

void TUpdateOperationParametersCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UpdateOperationParameters(
        OperationIdOrAlias,
        ConvertToYsonString(Parameters),
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TGetOperationCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<THashSet<TString>>>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "include_runtime",
        [] (TThis* command) -> auto& {
            return command->Options.IncludeRuntime;
        })
        .Alias("include_scheduler")
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TDuration>(
        "maximum_cypress_progress_age",
        [] (TThis* command) -> auto& {
            return command->Options.MaximumCypressProgressAge;
        })
        .Optional(/*init*/ false);
}

void TGetOperationCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetOperation(OperationIdOrAlias, Options);
    auto operation = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Do([&] (TFluentAny fluent) {
            Serialize(
                operation,
                fluent.GetConsumer(),
                /* needType */ true,
                /* needOperationType */ true);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
