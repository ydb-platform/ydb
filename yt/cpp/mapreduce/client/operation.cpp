#include "operation.h"

#include "abortable_registry.h"
#include "client.h"
#include "operation_helpers.h"
#include "operation_tracker.h"
#include "transaction.h"
#include "prepare_operation.h"
#include "retry_heavy_write_request.h"
#include "skiff.h"
#include "structured_table_formats.h"
#include "yt_poller.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/fluent.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/job_statistics.h>
#include <yt/cpp/mapreduce/interface/protobuf_format.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>
#include <yt/cpp/mapreduce/io/yamr_table_reader.h>
#include <yt/cpp/mapreduce/io/yamr_table_writer.h>
#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>
#include <yt/cpp/mapreduce/io/proto_table_reader.h>
#include <yt/cpp/mapreduce/io/proto_table_writer.h>
#include <yt/cpp/mapreduce/io/proto_helpers.h>
#include <yt/cpp/mapreduce/io/skiff_table_reader.h>

#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>
#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <library/cpp/yson/node/serialize.h>

#include <util/generic/hash_set.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <util/system/thread.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static const ui64 DefaultExrtaTmpfsSize = 1024LL * 1024LL;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMapReduceOperationIo
{
    TVector<TRichYPath> Inputs;
    TVector<TRichYPath> MapOutputs;
    TVector<TRichYPath> Outputs;

    TMaybe<TFormat> MapperInputFormat;
    TMaybe<TFormat> MapperOutputFormat;

    TMaybe<TFormat> ReduceCombinerInputFormat;
    TMaybe<TFormat> ReduceCombinerOutputFormat;

    TFormat ReducerInputFormat = TFormat::YsonBinary();
    TFormat ReducerOutputFormat = TFormat::YsonBinary();

    TVector<TSmallJobFile> MapperJobFiles;
    TVector<TSmallJobFile> ReduceCombinerJobFiles;
    TVector<TSmallJobFile> ReducerJobFiles;
};

template <typename T>
void VerifyHasElements(const TVector<T>& paths, TStringBuf name)
{
    if (paths.empty()) {
        ythrow TApiUsageError() << "no " << name << " table is specified";
    }
}

////////////////////////////////////////////////////////////////////////////////

TVector<TSmallJobFile> CreateFormatConfig(
    TMaybe<TSmallJobFile> inputConfig,
    const TMaybe<TSmallJobFile>& outputConfig)
{
    TVector<TSmallJobFile> result;
    if (inputConfig) {
        result.push_back(std::move(*inputConfig));
    }
    if (outputConfig) {
        result.push_back(std::move(*outputConfig));
    }
    return result;
}

template <typename T>
ENodeReaderFormat NodeReaderFormatFromHintAndGlobalConfig(const TUserJobFormatHintsBase<T>& formatHints)
{
    auto result = TConfig::Get()->NodeReaderFormat;
    if (formatHints.InputFormatHints_ && formatHints.InputFormatHints_->SkipNullValuesForTNode_) {
        Y_ENSURE_EX(
            result != ENodeReaderFormat::Skiff,
            TApiUsageError() << "skiff format doesn't support SkipNullValuesForTNode format hint");
        result = ENodeReaderFormat::Yson;
    }
    return result;
}

template <class TSpec>
const TVector<TStructuredTablePath>& GetStructuredInputs(const TSpec& spec)
{
    if constexpr (std::is_same_v<TSpec, TVanillaTask>) {
        static const TVector<TStructuredTablePath> empty;
        return empty;
    } else {
        return spec.GetStructuredInputs();
    }
}

template <class TSpec>
const TVector<TStructuredTablePath>& GetStructuredOutputs(const TSpec& spec)
{
    return spec.GetStructuredOutputs();
}

template <class TSpec>
const TMaybe<TFormatHints>& GetInputFormatHints(const TSpec& spec)
{
    if constexpr (std::is_same_v<TSpec, TVanillaTask>) {
        static const TMaybe<TFormatHints> empty = Nothing();
        return empty;
    } else {
        return spec.InputFormatHints_;
    }
}

template <class TSpec>
const TMaybe<TFormatHints>& GetOutputFormatHints(const TSpec& spec)
{
    return spec.OutputFormatHints_;
}

template <class TSpec>
ENodeReaderFormat GetNodeReaderFormat(const TSpec& spec, bool allowSkiff)
{
    if constexpr (std::is_same<TSpec, TVanillaTask>::value) {
        return ENodeReaderFormat::Yson;
    } else {
        return allowSkiff
            ? NodeReaderFormatFromHintAndGlobalConfig(spec)
            : ENodeReaderFormat::Yson;
    }
}

static void SortColumnsToNames(const TSortColumns& sortColumns, THashSet<TString>* result)
{
    auto names = sortColumns.GetNames();
    result->insert(names.begin(), names.end());
}

static THashSet<TString> SortColumnsToNames(const TSortColumns& sortColumns)
{
    THashSet<TString> columnNames;
    SortColumnsToNames(sortColumns, &columnNames);
    return columnNames;
}

THashSet<TString> GetColumnsUsedInOperation(const TJoinReduceOperationSpec& spec)
{
    return SortColumnsToNames(spec.JoinBy_);
}

THashSet<TString> GetColumnsUsedInOperation(const TReduceOperationSpec& spec) {
    auto result = SortColumnsToNames(spec.SortBy_);
    SortColumnsToNames(spec.ReduceBy_, &result);
    if (spec.JoinBy_) {
        SortColumnsToNames(*spec.JoinBy_, &result);
    }
    return result;
}

THashSet<TString> GetColumnsUsedInOperation(const TMapReduceOperationSpec& spec)
{
    auto result = SortColumnsToNames(spec.SortBy_);
    SortColumnsToNames(spec.ReduceBy_, &result);
    return result;
}

THashSet<TString> GetColumnsUsedInOperation(const TMapOperationSpec&)
{
    return THashSet<TString>();
}

THashSet<TString> GetColumnsUsedInOperation(const TVanillaTask&)
{
    return THashSet<TString>();
}

TStructuredJobTableList ApplyProtobufColumnFilters(
    const TStructuredJobTableList& tableList,
    const TOperationPreparer& preparer,
    const THashSet<TString>& columnsUsedInOperations,
    const TOperationOptions& options)
{
    bool hasInputQuery = options.Spec_.Defined() && options.Spec_->IsMap() && options.Spec_->HasKey("input_query");
    if (hasInputQuery) {
        return tableList;
    }

    auto isDynamic = BatchTransform(
        CreateDefaultRequestRetryPolicy(preparer.GetContext().Config),
        preparer.GetContext(),
        tableList,
        [&] (TRawBatchRequest& batch, const auto& table) {
            return batch.Get(preparer.GetTransactionId(), table.RichYPath->Path_ + "/@dynamic", TGetOptions());
        });

    auto newTableList = tableList;
    for (size_t tableIndex = 0; tableIndex < tableList.size(); ++tableIndex) {
        if (isDynamic[tableIndex].AsBool()) {
            continue;
        }
        auto& table = newTableList[tableIndex];
        Y_ABORT_UNLESS(table.RichYPath);
        if (table.RichYPath->Columns_) {
            continue;
        }
        if (!std::holds_alternative<TProtobufTableStructure>(table.Description)) {
            continue;
        }
        const auto& descriptor = std::get<TProtobufTableStructure>(table.Description).Descriptor;
        if (!descriptor) {
            continue;
        }
        auto fromDescriptor = NDetail::InferColumnFilter(*descriptor);
        if (!fromDescriptor) {
            continue;
        }
        THashSet<TString> columns(fromDescriptor->begin(), fromDescriptor->end());
        columns.insert(columnsUsedInOperations.begin(), columnsUsedInOperations.end());
        table.RichYPath->Columns(TVector<TString>(columns.begin(), columns.end()));
    }
    return newTableList;
}

template <class TSpec>
TSimpleOperationIo CreateSimpleOperationIo(
    const IStructuredJob& structuredJob,
    const TOperationPreparer& preparer,
    const TSpec& spec,
    const TOperationOptions& options,
    bool allowSkiff)
{
    if (!std::holds_alternative<TVoidStructuredRowStream>(structuredJob.GetInputRowStreamDescription())) {
        VerifyHasElements(GetStructuredInputs(spec), "input");
    }

    TUserJobFormatHints hints;
    hints.InputFormatHints_ = GetInputFormatHints(spec);
    hints.OutputFormatHints_ = GetOutputFormatHints(spec);
    ENodeReaderFormat nodeReaderFormat = GetNodeReaderFormat(spec, allowSkiff);

    return CreateSimpleOperationIoHelper(
        structuredJob,
        preparer,
        options,
        CanonizeStructuredTableList(preparer.GetContext(), GetStructuredInputs(spec)),
        CanonizeStructuredTableList(preparer.GetContext(), GetStructuredOutputs(spec)),
        hints,
        nodeReaderFormat,
        GetColumnsUsedInOperation(spec));
}

template <class T>
TSimpleOperationIo CreateSimpleOperationIo(
    const IJob& job,
    const TOperationPreparer& preparer,
    const TSimpleRawOperationIoSpec<T>& spec)
{
    auto getFormatOrDefault = [&] (const TMaybe<TFormat>& maybeFormat, const char* formatName) {
        if (maybeFormat) {
            return *maybeFormat;
        } else if (spec.Format_) {
            return *spec.Format_;
        } else {
            ythrow TApiUsageError() << "Neither " << formatName << "format nor default format is specified for raw operation";
        }
    };

    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetContext(), spec.GetInputs());
    auto outputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetContext(), spec.GetOutputs());

    VerifyHasElements(inputs, "input");
    VerifyHasElements(outputs, "output");

    TUserJobFormatHints hints;

    auto outputSchemas = PrepareOperation(
        job,
        TOperationPreparationContext(
            inputs,
            outputs,
            preparer.GetContext(),
            preparer.GetClientRetryPolicy(),
            preparer.GetTransactionId()),
        &inputs,
        &outputs,
        hints);

    Y_ABORT_UNLESS(outputs.size() == outputSchemas.size());
    for (int i = 0; i < static_cast<int>(outputs.size()); ++i) {
        if (!outputs[i].Schema_ && !outputSchemas[i].Columns().empty()) {
            outputs[i].Schema_ = outputSchemas[i];
        }
    }

    return TSimpleOperationIo {
        inputs,
        outputs,

        getFormatOrDefault(spec.InputFormat_, "input"),
        getFormatOrDefault(spec.OutputFormat_, "output"),

        TVector<TSmallJobFile>{},
    };
}

////////////////////////////////////////////////////////////////////////////////

TString GetJobStderrWithRetriesAndIgnoreErrors(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TJobId& jobId,
    const size_t stderrTailSize,
    const TGetJobStderrOptions& options = TGetJobStderrOptions())
{
    TString jobStderr;
    try {
        jobStderr = GetJobStderrWithRetries(
            retryPolicy,
            context,
            operationId,
            jobId,
            options);
    } catch (const TErrorResponse& e) {
        YT_LOG_ERROR("Cannot get job stderr (OperationId: %v, JobId: %v, Error: %v)",
            operationId,
            jobId,
            e.what());
    }
    if (jobStderr.size() > stderrTailSize) {
        jobStderr = jobStderr.substr(jobStderr.size() - stderrTailSize, stderrTailSize);
    }
    return jobStderr;
}

TVector<TFailedJobInfo> GetFailedJobInfo(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TGetFailedJobInfoOptions& options)
{
    const auto listJobsResult = ListJobs(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        context,
        operationId,
        TListJobsOptions()
            .State(EJobState::Failed)
            .Limit(options.MaxJobCount_));

    const auto stderrTailSize = options.StderrTailSize_;

    TVector<TFailedJobInfo> result;
    for (const auto& job : listJobsResult.Jobs) {
        auto& info = result.emplace_back();
        Y_ENSURE(job.Id);
        info.JobId = *job.Id;
        info.Error = job.Error.GetOrElse(TYtError(TString("unknown error")));
        if (job.StderrSize.GetOrElse(0) != 0) {
            // There are cases when due to bad luck we cannot read stderr even if
            // list_jobs reports that stderr_size > 0.
            //
            // Such errors don't have special error code
            // so we ignore all errors and try our luck on other jobs.
            info.Stderr = GetJobStderrWithRetriesAndIgnoreErrors(
                clientRetryPolicy->CreatePolicyForGenericRequest(),
                context,
                operationId,
                *job.Id,
                stderrTailSize);
        }
    }
    return result;
}

struct TGetJobsStderrOptions
{
    using TSelf = TGetJobsStderrOptions;

    // How many jobs to download. Which jobs will be chosen is undefined.
    FLUENT_FIELD_DEFAULT(ui64, MaxJobCount, 10);

    // How much of stderr should be downloaded.
    FLUENT_FIELD_DEFAULT(ui64, StderrTailSize, 64 * 1024);
};

static TVector<TString> GetJobsStderr(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TGetJobsStderrOptions& options = TGetJobsStderrOptions())
{
    const auto listJobsResult = ListJobs(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        context,
        operationId,
        TListJobsOptions().Limit(options.MaxJobCount_).WithStderr(true));
    const auto stderrTailSize = options.StderrTailSize_;
    TVector<TString> result;
    for (const auto& job : listJobsResult.Jobs) {
        result.push_back(
            // There are cases when due to bad luck we cannot read stderr even if
            // list_jobs reports that stderr_size > 0.
            //
            // Such errors don't have special error code
            // so we ignore all errors and try our luck on other jobs.
            GetJobStderrWithRetriesAndIgnoreErrors(
                clientRetryPolicy->CreatePolicyForGenericRequest(),
                context,
                operationId,
                *job.Id,
                stderrTailSize)
            );
    }
    return result;
}

int CountIntermediateTables(const TStructuredJobTableList& tables)
{
    int result = 0;
    for (const auto& table : tables) {
        if (table.RichYPath) {
            break;
        }
        ++result;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSimpleOperationIo CreateSimpleOperationIoHelper(
    const IStructuredJob& structuredJob,
    const TOperationPreparer& preparer,
    const TOperationOptions& options,
    TStructuredJobTableList structuredInputs,
    TStructuredJobTableList structuredOutputs,
    TUserJobFormatHints hints,
    ENodeReaderFormat nodeReaderFormat,
    const THashSet<TString>& columnsUsedInOperations)
{
    auto intermediateInputTableCount = CountIntermediateTables(structuredInputs);
    auto intermediateOutputTableCount = CountIntermediateTables(structuredOutputs);

    auto jobSchemaInferenceResult = PrepareOperation(
        structuredJob,
        TOperationPreparationContext(
            structuredInputs,
            structuredOutputs,
            preparer.GetContext(),
            preparer.GetClientRetryPolicy(),
            preparer.GetTransactionId()),
        &structuredInputs,
        &structuredOutputs,
        hints);

    TVector<TSmallJobFile> formatConfigList;
    TFormatBuilder formatBuilder(preparer.GetClientRetryPolicy(), preparer.GetContext(), preparer.GetTransactionId(), options);

    auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
        structuredJob,
        EIODirection::Input,
        structuredInputs,
        hints.InputFormatHints_,
        nodeReaderFormat,
        /* allowFormatFromTableAttribute = */ true);

    auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
        structuredJob,
        EIODirection::Output,
        structuredOutputs,
        hints.OutputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ false);

    const bool inferOutputSchema = options.InferOutputSchema_.GetOrElse(preparer.GetContext().Config->InferTableSchema);

    auto outputPaths = GetPathList(
        TStructuredJobTableList(structuredOutputs.begin() + intermediateOutputTableCount, structuredOutputs.end()),
        TVector<TTableSchema>(jobSchemaInferenceResult.begin() + intermediateOutputTableCount, jobSchemaInferenceResult.end()),
        inferOutputSchema);

    auto inputPaths = GetPathList(
        ApplyProtobufColumnFilters(
            TStructuredJobTableList(structuredInputs.begin() + intermediateInputTableCount, structuredInputs.end()),
            preparer,
            columnsUsedInOperations,
            options),
        /*schemaInferenceResult*/ Nothing(),
        /*inferSchema*/ false);

    return TSimpleOperationIo {
        inputPaths,
        outputPaths,

        inputFormat,
        outputFormat,

        CreateFormatConfig(inputFormatConfig, outputFormatConfig)
    };
}

EOperationBriefState CheckOperation(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TOperationId& operationId)
{
    auto attributes = GetOperation(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        context,
        operationId,
        TGetOperationOptions().AttributeFilter(TOperationAttributeFilter()
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::Result)));
    Y_ABORT_UNLESS(attributes.BriefState,
        "get_operation for operation %s has not returned \"state\" field",
        GetGuidAsString(operationId).Data());
    if (*attributes.BriefState == EOperationBriefState::Completed) {
        return EOperationBriefState::Completed;
    } else if (*attributes.BriefState == EOperationBriefState::Aborted || *attributes.BriefState == EOperationBriefState::Failed) {
        YT_LOG_ERROR("Operation %v %v (%v)",
            operationId,
            ToString(*attributes.BriefState),
            ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)));

        auto failedJobInfoList = GetFailedJobInfo(
            clientRetryPolicy,
            context,
            operationId,
            TGetFailedJobInfoOptions());

        Y_ABORT_UNLESS(attributes.Result && attributes.Result->Error);
        ythrow TOperationFailedError(
            *attributes.BriefState == EOperationBriefState::Aborted
            ? TOperationFailedError::Aborted
            : TOperationFailedError::Failed,
            operationId,
            *attributes.Result->Error,
            failedJobInfoList);
    }
    return EOperationBriefState::InProgress;
}

void WaitForOperation(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval =
        UseLocalModeOptimization(context, clientRetryPolicy)
        ? Min(TDuration::MilliSeconds(100), context.Config->OperationTrackerPollPeriod)
        : context.Config->OperationTrackerPollPeriod;

    while (true) {
        auto status = CheckOperation(clientRetryPolicy, context, operationId);
        if (status == EOperationBriefState::Completed) {
            YT_LOG_INFO("Operation %v completed (%v)",
                operationId,
                TOperationExecutionTimeTracker::Get()->Finish(operationId));
            break;
        }
        TWaitProxy::Get()->Sleep(checkOperationStateInterval);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TNode BuildAutoMergeSpec(const TAutoMergeSpec& options)
{
    TNode result;
    if (options.Mode_) {
        result["mode"] = ToString(*options.Mode_);
    }
    if (options.MaxIntermediateChunkCount_) {
        result["max_intermediate_chunk_count"] = *options.MaxIntermediateChunkCount_;
    }
    if (options.ChunkCountPerMergeJob_) {
        result["chunk_count_per_merge_job"] = *options.ChunkCountPerMergeJob_;
    }
    if (options.ChunkSizeThreshold_) {
        result["chunk_size_threshold"] = *options.ChunkSizeThreshold_;
    }
    return result;
}

TNode BuildJobProfilerSpec(const TJobProfilerSpec& profilerSpec)
{
    TNode result;
    if (profilerSpec.ProfilingBinary_) {
        result["binary"] = ToString(*profilerSpec.ProfilingBinary_);
    }
    if (profilerSpec.ProfilerType_) {
        result["type"] = ToString(*profilerSpec.ProfilerType_);
    }
    if (profilerSpec.ProfilingProbability_) {
        result["profiling_probability"] = *profilerSpec.ProfilingProbability_;
    }
    if (profilerSpec.SamplingFrequency_) {
        result["sampling_frequency"] = *profilerSpec.SamplingFrequency_;
    }

    return result;
}

// Returns undefined node if resources doesn't contain any meaningful field
TNode BuildSchedulerResourcesSpec(const TSchedulerResources& resources)
{
    TNode result;
    if (resources.UserSlots().Defined()) {
        result["user_slots"] = *resources.UserSlots();
    }
    if (resources.Cpu().Defined()) {
        result["cpu"] = *resources.Cpu();
    }
    if (resources.Memory().Defined()) {
        result["memory"] = *resources.Memory();
    }
    return result;
}

void BuildUserJobFluently(
    const TJobPreparer& preparer,
    const TMaybe<TFormat>& inputFormat,
    const TMaybe<TFormat>& outputFormat,
    TFluentMap fluent)
{
    const auto& userJobSpec = preparer.GetSpec();
    TMaybe<i64> memoryLimit = userJobSpec.MemoryLimit_;
    TMaybe<double> cpuLimit = userJobSpec.CpuLimit_;
    TMaybe<ui16> portCount = userJobSpec.PortCount_;

    // Use 1MB extra tmpfs size by default, it helps to detect job sandbox as tmp directory
    // for standard python libraries. See YTADMINREQ-14505 for more details.
    auto tmpfsSize = preparer.GetSpec().ExtraTmpfsSize_.GetOrElse(DefaultExrtaTmpfsSize);
    if (preparer.ShouldMountSandbox()) {
        tmpfsSize += preparer.GetTotalFileSize();
        if (tmpfsSize == 0) {
            // This can be a case for example when it is local mode and we don't upload binary.
            // NOTE: YT doesn't like zero tmpfs size.
            tmpfsSize = RoundUpFileSize(1);
        }
        memoryLimit = memoryLimit.GetOrElse(512ll << 20) + tmpfsSize;
    }

    fluent
        .Item("file_paths").List(preparer.GetFiles())
        .DoIf(!preparer.GetLayers().empty(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("layer_paths").List(preparer.GetLayers());
        })
        .Item("command").Value(preparer.GetCommand())
        .Item("class_name").Value(preparer.GetClassName())
        .DoIf(!userJobSpec.Environment_.empty(), [&] (TFluentMap fluentMap) {
            TNode environment;
            for (const auto& item : userJobSpec.Environment_) {
                environment[item.first] = item.second;
            }
            fluentMap.Item("environment").Value(environment);
        })
        .DoIf(userJobSpec.DiskSpaceLimit_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("disk_space_limit").Value(*userJobSpec.DiskSpaceLimit_);
        })
        .DoIf(inputFormat.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("input_format").Value(inputFormat->Config);
        })
        .DoIf(outputFormat.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("output_format").Value(outputFormat->Config);
        })
        .DoIf(memoryLimit.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("memory_limit").Value(*memoryLimit);
        })
        .DoIf(userJobSpec.MemoryReserveFactor_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("memory_reserve_factor").Value(*userJobSpec.MemoryReserveFactor_);
        })
        .DoIf(cpuLimit.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("cpu_limit").Value(*cpuLimit);
        })
        .DoIf(portCount.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("port_count").Value(*portCount);
        })
        .DoIf(userJobSpec.JobTimeLimit_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("job_time_limit").Value(userJobSpec.JobTimeLimit_->MilliSeconds());
        })
        .DoIf(userJobSpec.DiskRequest_.Defined(), [&] (TFluentMap fluentMap) {
            const auto& diskRequest = *userJobSpec.DiskRequest_;
            TNode diskRequestNode = TNode::CreateMap();
            if (diskRequest.DiskSpace_.Defined()) {
                diskRequestNode["disk_space"] = *diskRequest.DiskSpace_;
            }
            if (diskRequest.InodeCount_.Defined()) {
                diskRequestNode["inode_count"] = *diskRequest.InodeCount_;
            }
            if (diskRequest.Account_.Defined()) {
                diskRequestNode["account"] = *diskRequest.Account_;
            }
            if (diskRequest.MediumName_.Defined()) {
                diskRequestNode["medium_name"] = *diskRequest.MediumName_;
            }
            fluentMap.Item("disk_request").Value(diskRequestNode);
        })
        .DoIf(userJobSpec.NetworkProject_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("network_project").Value(*userJobSpec.NetworkProject_);
        })
        .DoIf(preparer.ShouldMountSandbox(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("tmpfs_path").Value(".");
            fluentMap.Item("tmpfs_size").Value(tmpfsSize);
            fluentMap.Item("copy_files").Value(true);
        })
        .Item("profilers")
            .BeginList()
                .DoFor(userJobSpec.JobProfilers_, [&] (TFluentList list, const auto& jobProfiler) {
                    list.Item().Value(BuildJobProfilerSpec(jobProfiler));
                })
            .EndList()
        .Item("redirect_stdout_to_stderr").Value(preparer.ShouldRedirectStdoutToStderr());
}

template <typename T>
void BuildCommonOperationPart(const TConfigPtr& config, const TOperationSpecBase<T>& baseSpec, const TOperationOptions& options, TFluentMap fluent)
{
    const TProcessState* properties = TProcessState::Get();
    TString pool = config->Pool;

    if (baseSpec.Pool_) {
        pool = *baseSpec.Pool_;
    }

    fluent
        .Item("started_by")
        .BeginMap()
            .Item("hostname").Value(properties->FqdnHostName)
            .Item("pid").Value(properties->Pid)
            .Item("user").Value(properties->UserName)
            .Item("command").List(properties->CensoredCommandLine)
            .Item("wrapper_version").Value(properties->ClientVersion)
        .EndMap()
        .DoIf(!pool.empty(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("pool").Value(pool);
        })
        .DoIf(baseSpec.Weight_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("weight").Value(*baseSpec.Weight_);
        })
        .DoIf(baseSpec.TimeLimit_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("time_limit").Value(baseSpec.TimeLimit_->MilliSeconds());
        })
        .DoIf(baseSpec.PoolTrees().Defined(), [&] (TFluentMap fluentMap) {
            TNode poolTreesSpec = TNode::CreateList();
            for (const auto& tree : *baseSpec.PoolTrees()) {
                poolTreesSpec.Add(tree);
            }
            fluentMap.Item("pool_trees").Value(poolTreesSpec);
        })
        .DoIf(baseSpec.ResourceLimits().Defined(), [&] (TFluentMap fluentMap) {
            auto resourceLimitsSpec = BuildSchedulerResourcesSpec(*baseSpec.ResourceLimits());
            if (!resourceLimitsSpec.IsUndefined()) {
                fluentMap.Item("resource_limits").Value(std::move(resourceLimitsSpec));
            }
        })
        .DoIf(options.SecureVault_.Defined(), [&] (TFluentMap fluentMap) {
            Y_ENSURE(options.SecureVault_->IsMap(),
                "SecureVault must be a map node, got " << options.SecureVault_->GetType());
            fluentMap.Item("secure_vault").Value(*options.SecureVault_);
        })
        .DoIf(baseSpec.Title_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("title").Value(*baseSpec.Title_);
        });
}

template <typename TSpec>
void BuildCommonUserOperationPart(const TSpec& baseSpec, TNode* spec)
{
    if (baseSpec.MaxFailedJobCount_.Defined()) {
        (*spec)["max_failed_job_count"] = *baseSpec.MaxFailedJobCount_;
    }
    if (baseSpec.FailOnJobRestart_.Defined()) {
        (*spec)["fail_on_job_restart"] = *baseSpec.FailOnJobRestart_;
    }
    if (baseSpec.StderrTablePath_.Defined()) {
        (*spec)["stderr_table_path"] = *baseSpec.StderrTablePath_;
    }
    if (baseSpec.CoreTablePath_.Defined()) {
        (*spec)["core_table_path"] = *baseSpec.CoreTablePath_;
    }
    if (baseSpec.WaitingJobTimeout_.Defined()) {
        (*spec)["waiting_job_timeout"] = baseSpec.WaitingJobTimeout_->MilliSeconds();
    }
}

template <typename TSpec>
void BuildJobCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.JobCount_.Defined()) {
        (*nodeSpec)["job_count"] = *spec.JobCount_;
    }
    if (spec.DataSizePerJob_.Defined()) {
        (*nodeSpec)["data_size_per_job"] = *spec.DataSizePerJob_;
    }
}

template <typename TSpec>
void BuildPartitionCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.PartitionCount_.Defined()) {
        (*nodeSpec)["partition_count"] = *spec.PartitionCount_;
    }
    if (spec.PartitionDataSize_.Defined()) {
        (*nodeSpec)["partition_data_size"] = *spec.PartitionDataSize_;
    }
}

template <typename TSpec>
void BuildDataSizePerSortJobPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.DataSizePerSortJob_.Defined()) {
        (*nodeSpec)["data_size_per_sort_job"] = *spec.DataSizePerSortJob_;
    }
}

template <typename TSpec>
void BuildPartitionJobCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.PartitionJobCount_.Defined()) {
        (*nodeSpec)["partition_job_count"] = *spec.PartitionJobCount_;
    }
    if (spec.DataSizePerPartitionJob_.Defined()) {
        (*nodeSpec)["data_size_per_partition_job"] = *spec.DataSizePerPartitionJob_;
    }
}

template <typename TSpec>
void BuildMapJobCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.MapJobCount_.Defined()) {
        (*nodeSpec)["map_job_count"] = *spec.MapJobCount_;
    }
    if (spec.DataSizePerMapJob_.Defined()) {
        (*nodeSpec)["data_size_per_map_job"] = *spec.DataSizePerMapJob_;
    }
}

template <typename TSpec>
void BuildIntermediateDataPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.IntermediateDataAccount_.Defined()) {
        (*nodeSpec)["intermediate_data_account"] = *spec.IntermediateDataAccount_;
    }
    if (spec.IntermediateDataReplicationFactor_.Defined()) {
        (*nodeSpec)["intermediate_data_replication_factor"] = *spec.IntermediateDataReplicationFactor_;
    }
}

////////////////////////////////////////////////////////////////////////////////

TNode MergeSpec(TNode dst, TNode spec, const TOperationOptions& options)
{
    MergeNodes(dst["spec"], spec);
    if (options.Spec_) {
        MergeNodes(dst["spec"], *options.Spec_);
    }
    return dst;
}

template <typename TSpec>
void CreateDebugOutputTables(const TSpec& spec, const TOperationPreparer& preparer)
{
    if (spec.StderrTablePath_.Defined()) {
        NYT::NDetail::Create(
            preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            preparer.GetContext(),
            TTransactionId(),
            *spec.StderrTablePath_,
            NT_TABLE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
    }
    if (spec.CoreTablePath_.Defined()) {
        NYT::NDetail::Create(
            preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            preparer.GetContext(),
            TTransactionId(),
            *spec.CoreTablePath_,
            NT_TABLE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
    }
}

void CreateOutputTable(
    const TOperationPreparer& preparer,
    const TRichYPath& path)
{
    Y_ENSURE(path.Path_, "Output table is not set");
    Create(
        preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
        preparer.GetContext(), preparer.GetTransactionId(), path.Path_, NT_TABLE,
        TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
}

void CreateOutputTables(
    const TOperationPreparer& preparer,
    const TVector<TRichYPath>& paths)
{
    for (auto& path : paths) {
        CreateOutputTable(preparer, path);
    }
}

void CheckInputTablesExist(
    const TOperationPreparer& preparer,
    const TVector<TRichYPath>& paths)
{
    Y_ENSURE(!paths.empty(), "Input tables are not set");
    for (auto& path : paths) {
        auto curTransactionId =  path.TransactionId_.GetOrElse(preparer.GetTransactionId());
        Y_ENSURE_EX(
            Exists(
                preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
                preparer.GetContext(),
                curTransactionId,
                path.Path_),
            TApiUsageError() << "Input table '" << path.Path_ << "' doesn't exist");
    }
}

void LogJob(const TOperationId& opId, const IJob* job, const char* type)
{
    if (job) {
        YT_LOG_INFO("Operation %v; %v = %v",
            opId,
            type,
            TJobFactory::Get()->GetJobName(job));
    }
}

void LogYPaths(const TOperationId& opId, const TVector<TRichYPath>& paths, const char* type)
{
    for (size_t i = 0; i < paths.size(); ++i) {
        YT_LOG_INFO("Operation %v; %v[%v] = %v",
            opId,
            type,
            i,
            paths[i].Path_);
    }
}

void LogYPath(const TOperationId& opId, const TRichYPath& path, const char* type)
{
    YT_LOG_INFO("Operation %v; %v = %v",
        opId,
        type,
        path.Path_);
}

TString AddModeToTitleIfDebug(const TString& title) {
#ifndef NDEBUG
    return title + " (debug build)";
#else
    return title;
#endif
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void DoExecuteMap(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TSimpleOperationIo& operationIo,
    TMapOperationSpecBase<T> spec,
    const IJobPtr& mapper,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, *preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(*preparer, operationIo.Inputs);
        CreateOutputTables(*preparer, operationIo.Outputs);
    }

    TJobPreparer map(
        *preparer,
        spec.MapperSpec_,
        *mapper,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(map.GetClassName()));

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("mapper").DoMap([&] (TFluentMap fluent) {
            BuildUserJobFluently(
                map,
                operationIo.InputFormat,
                operationIo.OutputFormat,
                fluent);
        })
        .DoIf(spec.AutoMerge_.Defined(), [&] (TFluentMap fluent) {
            auto autoMergeSpec = BuildAutoMergeSpec(*spec.AutoMerge_);
            if (!autoMergeSpec.IsUndefined()) {
                fluent.Item("auto_merge").Value(std::move(autoMergeSpec));
            }
        })
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .DoIf(spec.Ordered_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("ordered").Value(spec.Ordered_.GetRef());
        })
        .Do(std::bind(BuildCommonOperationPart<T>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    specNode["spec"]["job_io"]["control_attributes"]["enable_row_index"] = TNode(true);
    specNode["spec"]["job_io"]["control_attributes"]["enable_range_index"] = TNode(true);
    if (!preparer->GetContext().Config->TableWriter.Empty()) {
        specNode["spec"]["job_io"]["table_writer"] = preparer->GetContext().Config->TableWriter;
    }

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        operationIo,
        mapper
    ] () {
        auto operationId = preparer->StartOperation(operation, "map", spec);

        LogJob(operationId, mapper.Get(), "mapper");
        LogYPaths(operationId, operationIo.Inputs, "input");
        LogYPaths(operationId, operationIo.Outputs, "output");

        return operationId;
    };
    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteMap(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMapOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& mapper,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting map operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto operationIo = CreateSimpleOperationIo(*mapper, *preparer, spec, options, /* allowSkiff = */ true);
    DoExecuteMap(
        operation,
        preparer,
        operationIo,
        spec,
        mapper,
        options);
}

void ExecuteRawMap(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawMapOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& mapper,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting raw map operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto operationIo = CreateSimpleOperationIo(*mapper, *preparer, spec);
    DoExecuteMap(
        operation,
        preparer,
        operationIo,
        spec,
        mapper,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void DoExecuteReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TSimpleOperationIo& operationIo,
    TReduceOperationSpecBase<T> spec,
    const IJobPtr& reducer,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, *preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(*preparer, operationIo.Inputs);
        CreateOutputTables(*preparer, operationIo.Outputs);
    }

    TJobPreparer reduce(
        *preparer,
        spec.ReducerSpec_,
        *reducer,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(reduce.GetClassName()));

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap([&] (TFluentMap fluent) {
            BuildUserJobFluently(
                reduce,
                operationIo.InputFormat,
                operationIo.OutputFormat,
                fluent);
        })
        .DoIf(!spec.SortBy_.Parts_.empty(), [&] (TFluentMap fluent) {
            fluent.Item("sort_by").Value(spec.SortBy_);
        })
        .Item("reduce_by").Value(spec.ReduceBy_)
        .DoIf(spec.JoinBy_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("join_by").Value(spec.JoinBy_.GetRef());
        })
        .DoIf(spec.EnableKeyGuarantee_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("enable_key_guarantee").Value(spec.EnableKeyGuarantee_.GetRef());
        })
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
                .Item("enable_range_index").Value(true)
            .EndMap()
            .DoIf(!preparer->GetContext().Config->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(preparer->GetContext().Config->TableWriter);
            })
        .EndMap()
        .DoIf(spec.AutoMerge_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("auto_merge").Value(BuildAutoMergeSpec(*spec.AutoMerge_));
        })
        .Do(std::bind(BuildCommonOperationPart<T>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        operationIo,
        reducer
    ] () {
        auto operationId = preparer->StartOperation(operation, "reduce", spec);

        LogJob(operationId, reducer.Get(), "reducer");
        LogYPaths(operationId, operationIo.Inputs, "input");
        LogYPaths(operationId, operationIo.Outputs, "output");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TReduceOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& reducer,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting reduce operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto operationIo = CreateSimpleOperationIo(*reducer, *preparer, spec, options, /* allowSkiff = */ false);
    DoExecuteReduce(
        operation,
        preparer,
        operationIo,
        spec,
        reducer,
        options);
}

void ExecuteRawReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawReduceOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& reducer,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting raw reduce operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto operationIo = CreateSimpleOperationIo(*reducer, *preparer, spec);
    DoExecuteReduce(
        operation,
        preparer,
        operationIo,
        spec,
        reducer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void DoExecuteJoinReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TSimpleOperationIo& operationIo,
    TJoinReduceOperationSpecBase<T> spec,
    const IJobPtr& reducer,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, *preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(*preparer, operationIo.Inputs);
        CreateOutputTables(*preparer, operationIo.Outputs);
    }

    TJobPreparer reduce(
        *preparer,
        spec.ReducerSpec_,
        *reducer,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(reduce.GetClassName()));

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap([&] (TFluentMap fluent) {
            BuildUserJobFluently(
                reduce,
                operationIo.InputFormat,
                operationIo.OutputFormat,
                fluent);
        })
        .Item("join_by").Value(spec.JoinBy_)
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
                .Item("enable_range_index").Value(true)
            .EndMap()
            .DoIf(!preparer->GetContext().Config->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(preparer->GetContext().Config->TableWriter);
            })
        .EndMap()
        .Do(std::bind(BuildCommonOperationPart<T>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        reducer,
        operationIo
    ] () {
        auto operationId = preparer->StartOperation(operation, "join_reduce", spec);

        LogJob(operationId, reducer.Get(), "reducer");
        LogYPaths(operationId, operationIo.Inputs, "input");
        LogYPaths(operationId, operationIo.Outputs, "output");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteJoinReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TJoinReduceOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& reducer,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting join reduce operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto operationIo = CreateSimpleOperationIo(*reducer, *preparer, spec, options, /* allowSkiff = */ false);
    return DoExecuteJoinReduce(
        operation,
        preparer,
        operationIo,
        spec,
        reducer,
        options);
}

void ExecuteRawJoinReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawJoinReduceOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& reducer,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting raw join reduce operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto operationIo = CreateSimpleOperationIo(*reducer, *preparer, spec);
    return DoExecuteJoinReduce(
        operation,
        preparer,
        operationIo,
        spec,
        reducer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void DoExecuteMapReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMapReduceOperationIo& operationIo,
    TMapReduceOperationSpecBase<T> spec,
    const IJobPtr& mapper,
    const IJobPtr& reduceCombiner,
    const IJobPtr& reducer,
    const TOperationOptions& options)
{
    TVector<TRichYPath> allOutputs;
    allOutputs.insert(allOutputs.end(), operationIo.MapOutputs.begin(), operationIo.MapOutputs.end());
    allOutputs.insert(allOutputs.end(), operationIo.Outputs.begin(), operationIo.Outputs.end());

    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, *preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(*preparer, operationIo.Inputs);
        CreateOutputTables(*preparer, allOutputs);
    }

    TSortColumns sortBy = spec.SortBy_;
    TSortColumns reduceBy = spec.ReduceBy_;

    if (sortBy.Parts_.empty()) {
        sortBy = reduceBy;
    }

    const bool hasMapper = mapper != nullptr;
    const bool hasCombiner = reduceCombiner != nullptr;

    TVector<TRichYPath> files;

    TJobPreparer reduce(
        *preparer,
        spec.ReducerSpec_,
        *reducer,
        operationIo.Outputs.size(),
        operationIo.ReducerJobFiles,
        options);

    TString title;

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .DoIf(hasMapper, [&] (TFluentMap fluent) {
            TJobPreparer map(
                *preparer,
                spec.MapperSpec_,
                *mapper,
                1 + operationIo.MapOutputs.size(),
                operationIo.MapperJobFiles,
                options);
            fluent.Item("mapper").DoMap([&] (TFluentMap fluent) {
                BuildUserJobFluently(
                    std::cref(map),
                    *operationIo.MapperInputFormat,
                    *operationIo.MapperOutputFormat,
                    fluent);
            });

            title = "mapper:" + map.GetClassName() + " ";
        })
        .DoIf(hasCombiner, [&] (TFluentMap fluent) {
            TJobPreparer combine(
                *preparer,
                spec.ReduceCombinerSpec_,
                *reduceCombiner,
                size_t(1),
                operationIo.ReduceCombinerJobFiles,
                options);
            fluent.Item("reduce_combiner").DoMap([&] (TFluentMap fluent) {
                BuildUserJobFluently(
                    combine,
                    *operationIo.ReduceCombinerInputFormat,
                    *operationIo.ReduceCombinerOutputFormat,
                    fluent);
            });
            title += "combiner:" + combine.GetClassName() + " ";
        })
        .Item("reducer").DoMap([&] (TFluentMap fluent) {
            BuildUserJobFluently(
                reduce,
                operationIo.ReducerInputFormat,
                operationIo.ReducerOutputFormat,
                fluent);
        })
        .Item("sort_by").Value(sortBy)
        .Item("reduce_by").Value(reduceBy)
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(allOutputs)
        .Item("mapper_output_table_count").Value(operationIo.MapOutputs.size())
        .DoIf(spec.ForceReduceCombiners_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("force_reduce_combiners").Value(*spec.ForceReduceCombiners_);
        })
        .Item("map_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_row_index").Value(true)
                .Item("enable_range_index").Value(true)
            .EndMap()
            .DoIf(!preparer->GetContext().Config->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(preparer->GetContext().Config->TableWriter);
            })
        .EndMap()
        .Item("sort_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
            .DoIf(!preparer->GetContext().Config->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(preparer->GetContext().Config->TableWriter);
            })
        .EndMap()
        .Item("reduce_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
            .DoIf(!preparer->GetContext().Config->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(preparer->GetContext().Config->TableWriter);
            })
        .EndMap()
        .Do([&] (TFluentMap) {
            spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(title + "reducer:" + reduce.GetClassName()));
        })
        .Do(std::bind(BuildCommonOperationPart<T>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    if (spec.Ordered_) {
        specNode["spec"]["ordered"] = *spec.Ordered_;
    }

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildMapJobCountOperationPart(spec, &specNode["spec"]);
    BuildPartitionCountOperationPart(spec, &specNode["spec"]);
    BuildIntermediateDataPart(spec, &specNode["spec"]);
    BuildDataSizePerSortJobPart(spec, &specNode["spec"]);

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        mapper,
        reduceCombiner,
        reducer,
        inputs=operationIo.Inputs,
        allOutputs
    ] () {
        auto operationId = preparer->StartOperation(operation, "map_reduce", spec);

        LogJob(operationId, mapper.Get(), "mapper");
        LogJob(operationId, reduceCombiner.Get(), "reduce_combiner");
        LogJob(operationId, reducer.Get(), "reducer");
        LogYPaths(operationId, inputs, "input");
        LogYPaths(operationId, allOutputs, "output");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteMapReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMapReduceOperationSpec& spec_,
    const ::TIntrusivePtr<IStructuredJob>& mapper,
    const ::TIntrusivePtr<IStructuredJob>& reduceCombiner,
    const ::TIntrusivePtr<IStructuredJob>& reducer,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting map-reduce operation (PreparationId: %v)",
        preparer->GetPreparationId());
    TMapReduceOperationSpec spec = spec_;

    TMapReduceOperationIo operationIo;
    auto structuredInputs = CanonizeStructuredTableList(preparer->GetContext(), spec.GetStructuredInputs());
    auto structuredMapOutputs = CanonizeStructuredTableList(preparer->GetContext(), spec.GetStructuredMapOutputs());
    auto structuredOutputs = CanonizeStructuredTableList(preparer->GetContext(), spec.GetStructuredOutputs());

    const bool inferOutputSchema = options.InferOutputSchema_.GetOrElse(preparer->GetContext().Config->InferTableSchema);

    TVector<TTableSchema> currentInferenceResult;

    auto fixSpec = [&] (const TFormat& format) {
        if (format.IsYamredDsv()) {
            spec.SortBy_.Parts_.clear();
            spec.ReduceBy_.Parts_.clear();

            const TYamredDsvAttributes attributes = format.GetYamredDsvAttributes();
            for (auto& column : attributes.KeyColumnNames) {
                spec.SortBy_.Parts_.push_back(column);
                spec.ReduceBy_.Parts_.push_back(column);
            }
            for (const auto& column : attributes.SubkeyColumnNames) {
                spec.SortBy_.Parts_.push_back(column);
            }
        }
    };

    VerifyHasElements(structuredInputs, "inputs");

    TFormatBuilder formatBuilder(
        preparer->GetClientRetryPolicy(),
        preparer->GetContext(),
        preparer->GetTransactionId(),
        options);

    if (mapper) {
        auto mapperOutputDescription =
            spec.GetIntermediateMapOutputDescription()
            .GetOrElse(TUnspecifiedTableStructure());
        TStructuredJobTableList mapperOutput = {
            TStructuredJobTable::Intermediate(mapperOutputDescription),
        };

        for (const auto& table : structuredMapOutputs) {
            mapperOutput.push_back(TStructuredJobTable{table.Description, table.RichYPath});
        }

        auto hints = spec.MapperFormatHints_;

        auto mapperInferenceResult = PrepareOperation<TStructuredJobTableList>(
            *mapper,
            TOperationPreparationContext(
                structuredInputs,
                mapperOutput,
                preparer->GetContext(),
                preparer->GetClientRetryPolicy(),
                preparer->GetTransactionId()),
            &structuredInputs,
            /* outputs */ nullptr,
            hints);

        auto nodeReaderFormat = NodeReaderFormatFromHintAndGlobalConfig(spec.MapperFormatHints_);

        auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
            *mapper,
            EIODirection::Input,
            structuredInputs,
            hints.InputFormatHints_,
            nodeReaderFormat,
            /* allowFormatFromTableAttribute */ true);

        auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
            *mapper,
            EIODirection::Output,
            mapperOutput,
            hints.OutputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute */ false);

        operationIo.MapperJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
        operationIo.MapperInputFormat = inputFormat;
        operationIo.MapperOutputFormat = outputFormat;

        Y_ABORT_UNLESS(mapperInferenceResult.size() >= 1);
        currentInferenceResult = TVector<TTableSchema>{mapperInferenceResult[0]};
        // The first output as it corresponds to the intermediate data.
        TVector<TTableSchema> additionalOutputsInferenceResult(mapperInferenceResult.begin() + 1, mapperInferenceResult.end());

        operationIo.MapOutputs = GetPathList(
            structuredMapOutputs,
            additionalOutputsInferenceResult,
            inferOutputSchema);
    }

    if (reduceCombiner) {
        const bool isFirstStep = !mapper;
        TStructuredJobTableList inputs;
        if (isFirstStep) {
            inputs = structuredInputs;
        } else {
            auto reduceCombinerIntermediateInput =
                spec.GetIntermediateReduceCombinerInputDescription()
                .GetOrElse(TUnspecifiedTableStructure());
            inputs = {
                TStructuredJobTable::Intermediate(reduceCombinerIntermediateInput),
            };
        }

        auto reduceCombinerOutputDescription = spec.GetIntermediateReduceCombinerOutputDescription()
            .GetOrElse(TUnspecifiedTableStructure());

        TStructuredJobTableList outputs = {
            TStructuredJobTable::Intermediate(reduceCombinerOutputDescription),
        };

        auto hints = spec.ReduceCombinerFormatHints_;

        if (isFirstStep) {
            currentInferenceResult = PrepareOperation<TStructuredJobTableList>(
                *reduceCombiner,
                TOperationPreparationContext(
                    inputs,
                    outputs,
                    preparer->GetContext(),
                    preparer->GetClientRetryPolicy(),
                    preparer->GetTransactionId()),
                &inputs,
                /* outputs */ nullptr,
                hints);
        } else {
            currentInferenceResult = PrepareOperation<TStructuredJobTableList>(
                *reduceCombiner,
                TSpeculativeOperationPreparationContext(
                    currentInferenceResult,
                    inputs,
                    outputs),
                /* inputs */ nullptr,
                /* outputs */ nullptr,
                hints);
        }

        auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
            *reduceCombiner,
            EIODirection::Input,
            inputs,
            hints.InputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute = */ isFirstStep);

        auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
            *reduceCombiner,
            EIODirection::Output,
            outputs,
            hints.OutputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute = */ false);

        operationIo.ReduceCombinerJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
        operationIo.ReduceCombinerInputFormat = inputFormat;
        operationIo.ReduceCombinerOutputFormat = outputFormat;

        if (isFirstStep) {
            fixSpec(*operationIo.ReduceCombinerInputFormat);
        }
    }

    const bool isFirstStep = (!mapper && !reduceCombiner);
    TStructuredJobTableList reducerInputs;
    if (isFirstStep) {
        reducerInputs = structuredInputs;
    } else {
        auto reducerInputDescription =
            spec.GetIntermediateReducerInputDescription()
            .GetOrElse(TUnspecifiedTableStructure());
        reducerInputs = {
            TStructuredJobTable::Intermediate(reducerInputDescription),
        };
    }

    auto hints = spec.ReducerFormatHints_;

    TVector<TTableSchema> reducerInferenceResult;
    if (isFirstStep) {
        reducerInferenceResult = PrepareOperation(
            *reducer,
            TOperationPreparationContext(
                structuredInputs,
                structuredOutputs,
                preparer->GetContext(),
                preparer->GetClientRetryPolicy(),
                preparer->GetTransactionId()),
            &structuredInputs,
            &structuredOutputs,
            hints);
    } else {
        reducerInferenceResult = PrepareOperation<TStructuredJobTableList>(
            *reducer,
            TSpeculativeOperationPreparationContext(
                currentInferenceResult,
                reducerInputs,
                structuredOutputs),
            /* inputs */ nullptr,
            &structuredOutputs,
            hints);
    }

    auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
        *reducer,
        EIODirection::Input,
        reducerInputs,
        hints.InputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ isFirstStep);

    auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
        *reducer,
        EIODirection::Output,
        ToStructuredJobTableList(spec.GetStructuredOutputs()),
        hints.OutputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ false);
    operationIo.ReducerJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
    operationIo.ReducerInputFormat = inputFormat;
    operationIo.ReducerOutputFormat = outputFormat;

    if (isFirstStep) {
        fixSpec(operationIo.ReducerInputFormat);
    }

    operationIo.Inputs = GetPathList(
        ApplyProtobufColumnFilters(
            structuredInputs,
            *preparer,
            GetColumnsUsedInOperation(spec),
            options),
        /* jobSchemaInferenceResult */ Nothing(),
        /* inferSchema */ false);

    operationIo.Outputs = GetPathList(
        structuredOutputs,
        reducerInferenceResult,
        inferOutputSchema);

    VerifyHasElements(operationIo.Outputs, "outputs");

    return DoExecuteMapReduce(
        operation,
        preparer,
        operationIo,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options);
}

void ExecuteRawMapReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawMapReduceOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& mapper,
    const ::TIntrusivePtr<IRawJob>& reduceCombiner,
    const ::TIntrusivePtr<IRawJob>& reducer,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting raw map-reduce operation (PreparationId: %v)",
        preparer->GetPreparationId());
    TMapReduceOperationIo operationIo;
    operationIo.Inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer->GetContext(), spec.GetInputs());
    operationIo.MapOutputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer->GetContext(), spec.GetMapOutputs());
    operationIo.Outputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer->GetContext(), spec.GetOutputs());

    VerifyHasElements(operationIo.Inputs, "inputs");
    VerifyHasElements(operationIo.Outputs, "outputs");

    auto getFormatOrDefault = [&] (const TMaybe<TFormat>& maybeFormat, const TMaybe<TFormat> stageDefaultFormat, const char* formatName) {
        if (maybeFormat) {
            return *maybeFormat;
        } else if (stageDefaultFormat) {
            return *stageDefaultFormat;
        } else {
            ythrow TApiUsageError() << "Cannot derive " << formatName;
        }
    };

    if (mapper) {
        operationIo.MapperInputFormat = getFormatOrDefault(spec.MapperInputFormat_, spec.MapperFormat_, "mapper input format");
        operationIo.MapperOutputFormat = getFormatOrDefault(spec.MapperOutputFormat_, spec.MapperFormat_, "mapper output format");
    }

    if (reduceCombiner) {
        operationIo.ReduceCombinerInputFormat = getFormatOrDefault(spec.ReduceCombinerInputFormat_, spec.ReduceCombinerFormat_, "reduce combiner input format");
        operationIo.ReduceCombinerOutputFormat = getFormatOrDefault(spec.ReduceCombinerOutputFormat_, spec.ReduceCombinerFormat_, "reduce combiner output format");
    }

    operationIo.ReducerInputFormat = getFormatOrDefault(spec.ReducerInputFormat_, spec.ReducerFormat_, "reducer input format");
    operationIo.ReducerOutputFormat = getFormatOrDefault(spec.ReducerOutputFormat_, spec.ReducerFormat_, "reducer output format");

    return DoExecuteMapReduce(
        operation,
        preparer,
        operationIo,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options);
}

void ExecuteSort(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting sort operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer->GetContext(), spec.Inputs_);
    auto output = CanonizeYPath(nullptr, preparer->GetContext(), spec.Output_);

    if (options.CreateOutputTables_) {
        CheckInputTablesExist(*preparer, inputs);
        CreateOutputTable(*preparer, output);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("sort_by").Value(spec.SortBy_)
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(ToString(*spec.SchemaInferenceMode_));
        })
        .Do(std::bind(BuildCommonOperationPart<TSortOperationSpec>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildPartitionCountOperationPart(spec, &specNode["spec"]);
    BuildPartitionJobCountOperationPart(spec, &specNode["spec"]);
    BuildIntermediateDataPart(spec, &specNode["spec"]);

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        inputs,
        output
    ] () {
        auto operationId = preparer->StartOperation(operation, "sort", spec);

        LogYPaths(operationId, inputs, "input");
        LogYPath(operationId, output, "output");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteMerge(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMergeOperationSpec& spec,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting merge operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer->GetContext(), spec.Inputs_);
    auto output = CanonizeYPath(nullptr, preparer->GetContext(), spec.Output_);

    if (options.CreateOutputTables_) {
        CheckInputTablesExist(*preparer, inputs);
        CreateOutputTable(*preparer, output);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("mode").Value(ToString(spec.Mode_))
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Item("force_transform").Value(spec.ForceTransform_)
        .Item("merge_by").Value(spec.MergeBy_)
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(ToString(*spec.SchemaInferenceMode_));
        })
        .Do(std::bind(BuildCommonOperationPart<TMergeOperationSpec>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        inputs,
        output
    ] () {
        auto operationId = preparer->StartOperation(operation, "merge", spec);

        LogYPaths(operationId, inputs, "input");
        LogYPath(operationId, output, "output");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteErase(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TEraseOperationSpec& spec,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting erase operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto tablePath = CanonizeYPath(nullptr, preparer->GetContext(), spec.TablePath_);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("table_path").Value(tablePath)
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(ToString(*spec.SchemaInferenceMode_));
        })
        .Do(std::bind(BuildCommonOperationPart<TEraseOperationSpec>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options),
        preparer,
        tablePath
    ] () {
        auto operationId = preparer->StartOperation(operation, "erase", spec);

        LogYPath(operationId, tablePath, "table_path");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteRemoteCopy(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRemoteCopyOperationSpec& spec,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting remote copy operation (PreparationId: %v)",
        preparer->GetPreparationId());
    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer->GetContext(), spec.Inputs_);
    auto output = CanonizeYPath(nullptr, preparer->GetContext(), spec.Output_);

    if (options.CreateOutputTables_) {
        CreateOutputTable(*preparer, output);
    }

    Y_ENSURE_EX(!spec.ClusterName_.empty(), TApiUsageError() << "ClusterName parameter is required");

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("cluster_name").Value(spec.ClusterName_)
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .DoIf(spec.NetworkName_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("network_name").Value(*spec.NetworkName_);
        })
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(ToString(*spec.SchemaInferenceMode_));
        })
        .Item("copy_attributes").Value(spec.CopyAttributes_)
        .DoIf(!spec.AttributeKeys_.empty(), [&] (TFluentMap fluent) {
            Y_ENSURE_EX(spec.CopyAttributes_, TApiUsageError() <<
                "Specifying nonempty AttributeKeys in RemoteCopy "
                "doesn't make sense without CopyAttributes == true");
            fluent.Item("attribute_keys").List(spec.AttributeKeys_);
        })
        .Do(std::bind(BuildCommonOperationPart<TRemoteCopyOperationSpec>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto startOperation = [
        operation=operation.Get(),
        spec=MergeSpec(specNode, preparer->GetContext().Config->Spec, options),
        preparer,
        inputs,
        output
    ] () {
        auto operationId = preparer->StartOperation(operation, "remote_copy", spec);

        LogYPaths(operationId, inputs, "input");
        LogYPath(operationId, output, "output");

        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

void ExecuteVanilla(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options)
{
    YT_LOG_DEBUG("Starting vanilla operation (PreparationId: %v)",
        preparer->GetPreparationId());

    auto addTask = [&](TFluentMap fluent, const TVanillaTask& task) {
        Y_ABORT_UNLESS(task.Job_.Get());
        if (std::holds_alternative<TVoidStructuredRowStream>(task.Job_->GetOutputRowStreamDescription())) {
            Y_ENSURE_EX(task.Outputs_.empty(),
                TApiUsageError() << "Vanilla task with void IVanillaJob doesn't expect output tables");
            TJobPreparer jobPreparer(
                *preparer,
                task.Spec_,
                *task.Job_,
                /* outputTableCount */ 0,
                /* smallFileList */ {},
                options);
            fluent
                .Item(task.Name_).BeginMap()
                    .Item("job_count").Value(task.JobCount_)
                    .DoIf(task.NetworkProject_.Defined(), [&](TFluentMap fluent) {
                        fluent.Item("network_project").Value(*task.NetworkProject_);
                    })
                    .Do([&] (TFluentMap fluent) {
                        BuildUserJobFluently(
                            std::cref(jobPreparer),
                            /* inputFormat */ Nothing(),
                            /* outputFormat */ Nothing(),
                            fluent);
                    })
                .EndMap();
        } else {
            auto operationIo = CreateSimpleOperationIo(
                *task.Job_,
                *preparer,
                task,
                options,
                false);
            Y_ENSURE_EX(operationIo.Outputs.size() > 0,
                TApiUsageError() << "Vanilla task with IVanillaJob that has table writer expects output tables");
            if (options.CreateOutputTables_) {
                CreateOutputTables(*preparer, operationIo.Outputs);
            }
            TJobPreparer jobPreparer(
                *preparer,
                task.Spec_,
                *task.Job_,
                operationIo.Outputs.size(),
                operationIo.JobFiles,
                options);
            fluent
                .Item(task.Name_).BeginMap()
                    .Item("job_count").Value(task.JobCount_)
                    .DoIf(task.NetworkProject_.Defined(), [&](TFluentMap fluent) {
                        fluent.Item("network_project").Value(*task.NetworkProject_);
                    })
                    .Do([&] (TFluentMap fluent) {
                        BuildUserJobFluently(
                            std::cref(jobPreparer),
                            /* inputFormat */ Nothing(),
                            operationIo.OutputFormat,
                            fluent);
                    })
                    .Item("output_table_paths").List(operationIo.Outputs)
                    .Item("job_io").BeginMap()
                        .DoIf(!preparer->GetContext().Config->TableWriter.Empty(), [&](TFluentMap fluent) {
                            fluent.Item("table_writer").Value(preparer->GetContext().Config->TableWriter);
                        })
                        .Item("control_attributes").BeginMap()
                            .Item("enable_row_index").Value(TNode(true))
                            .Item("enable_range_index").Value(TNode(true))
                        .EndMap()
                    .EndMap()
                .EndMap();
        }
    };

    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, *preparer);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("tasks").DoMapFor(spec.Tasks_, addTask)
        .Do(std::bind(BuildCommonOperationPart<TVanillaOperationSpec>, preparer->GetContext().Config, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);

    auto startOperation = [operation=operation.Get(), spec=MergeSpec(std::move(specNode), preparer->GetContext().Config->Spec, options), preparer] () {
        auto operationId = preparer->StartOperation(operation, "vanilla", spec, /* useStartOperationRequest */ true);
        return operationId;
    };

    operation->SetDelayedStartFunction(std::move(startOperation));
}

////////////////////////////////////////////////////////////////////////////////

class TOperation::TOperationImpl
    : public TThrRefBase
{
public:
    TOperationImpl(
        IClientRetryPolicyPtr clientRetryPolicy,
        TClientContext context,
        const TMaybe<TOperationId>& operationId = {})
        : ClientRetryPolicy_(clientRetryPolicy)
        , Context_(std::move(context))
        , Id_(operationId)
        , PreparedPromise_(::NThreading::NewPromise<void>())
        , StartedPromise_(::NThreading::NewPromise<void>())
    {
        if (Id_) {
            PreparedPromise_.SetValue();
            StartedPromise_.SetValue();
        } else {
            PreparedPromise_.GetFuture().Subscribe([this_=::TIntrusivePtr(this)] (const ::NThreading::TFuture<void>& preparedResult) {
                try {
                    preparedResult.GetValue();
                } catch (...) {
                    this_->StartedPromise_.SetException(std::current_exception());
                    return;
                }
            });
        }
    }

    const TOperationId& GetId() const;
    TString GetWebInterfaceUrl() const;

    void OnPrepared();
    void SetDelayedStartFunction(std::function<TOperationId()> start);
    void Start();
    bool IsStarted() const;
    void OnPreparationException(std::exception_ptr e);

    TString GetStatus();
    void OnStatusUpdated(const TString& newStatus);

    ::NThreading::TFuture<void> GetPreparedFuture();
    ::NThreading::TFuture<void> GetStartedFuture();
    ::NThreading::TFuture<void> Watch(TClientPtr client);

    EOperationBriefState GetBriefState();
    TMaybe<TYtError> GetError();
    TJobStatistics GetJobStatistics();
    TMaybe<TOperationBriefProgress> GetBriefProgress();
    void AbortOperation();
    void CompleteOperation();
    void SuspendOperation(const TSuspendOperationOptions& options);
    void ResumeOperation(const TResumeOperationOptions& options);
    TOperationAttributes GetAttributes(const TGetOperationOptions& options);
    void UpdateParameters(const TUpdateOperationParametersOptions& options);
    TJobAttributes GetJob(const TJobId& jobId, const TGetJobOptions& options);
    TListJobsResult ListJobs(const TListJobsOptions& options);

    void AsyncFinishOperation(TOperationAttributes operationAttributes);
    void FinishWithException(std::exception_ptr exception);
    void UpdateBriefProgress(TMaybe<TOperationBriefProgress> briefProgress);
    void AnalyzeUnrecognizedSpec(TNode unrecognizedSpec);

    const TClientContext& GetContext() const;

private:
    void OnStarted(const TOperationId& operationId);

    void UpdateAttributesAndCall(bool needJobStatistics, std::function<void(const TOperationAttributes&)> func);

    void SyncFinishOperationImpl(const TOperationAttributes&);
    static void* SyncFinishOperationProc(void* );

    void ValidateOperationStarted() const;

private:
    IClientRetryPolicyPtr ClientRetryPolicy_;
    const TClientContext Context_;
    TMaybe<TOperationId> Id_;
    TMutex Lock_;

    ::NThreading::TPromise<void> PreparedPromise_;
    ::NThreading::TPromise<void> StartedPromise_;
    TMaybe<::NThreading::TPromise<void>> CompletePromise_;

    std::function<TOperationId()> DelayedStartFunction_;
    TString Status_;
    TOperationAttributes Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationPollerItem
    : public IYtPollerItem
{
public:
    TOperationPollerItem(::TIntrusivePtr<TOperation::TOperationImpl> operationImpl)
        : OperationImpl_(std::move(operationImpl))
    { }

    void PrepareRequest(TRawBatchRequest* batchRequest) override
    {
        auto filter = TOperationAttributeFilter()
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::BriefProgress)
            .Add(EOperationAttribute::Result);

        if (!UnrecognizedSpecAnalyzed_) {
            filter.Add(EOperationAttribute::UnrecognizedSpec);
        }

        OperationState_ = batchRequest->GetOperation(
            OperationImpl_->GetId(),
            TGetOperationOptions().AttributeFilter(filter));
    }

    EStatus OnRequestExecuted() override
    {
        try {
            const auto& attributes = OperationState_.GetValue();
            if (!UnrecognizedSpecAnalyzed_ && !attributes.UnrecognizedSpec.Empty()) {
                OperationImpl_->AnalyzeUnrecognizedSpec(*attributes.UnrecognizedSpec);
                UnrecognizedSpecAnalyzed_ = true;
            }
            Y_ABORT_UNLESS(attributes.BriefState,
                "get_operation for operation %s has not returned \"state\" field",
                GetGuidAsString(OperationImpl_->GetId()).Data());
            if (*attributes.BriefState != EOperationBriefState::InProgress) {
                OperationImpl_->AsyncFinishOperation(attributes);
                return PollBreak;
            } else {
                OperationImpl_->UpdateBriefProgress(attributes.BriefProgress);
            }
        } catch (const TErrorResponse& e) {
            if (!IsRetriable(e)) {
                OperationImpl_->FinishWithException(std::current_exception());
                return PollBreak;
            }
        } catch (const std::exception& e) {
            OperationImpl_->FinishWithException(std::current_exception());
            return PollBreak;
        }
        return PollContinue;
    }

    void OnItemDiscarded() override {
        OperationImpl_->FinishWithException(std::make_exception_ptr(yexception() << "Operation cancelled"));
    }

private:
    ::TIntrusivePtr<TOperation::TOperationImpl> OperationImpl_;
    ::NThreading::TFuture<TOperationAttributes> OperationState_;
    bool UnrecognizedSpecAnalyzed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

const TOperationId& TOperation::TOperationImpl::GetId() const
{
    ValidateOperationStarted();
    return *Id_;
}

TString TOperation::TOperationImpl::GetWebInterfaceUrl() const
{
    ValidateOperationStarted();
    return GetOperationWebInterfaceUrl(Context_.ServerName, *Id_);
}

void TOperation::TOperationImpl::OnPrepared()
{
    Y_ABORT_UNLESS(!PreparedPromise_.HasException() && !PreparedPromise_.HasValue());
    PreparedPromise_.SetValue();
}

void TOperation::TOperationImpl::SetDelayedStartFunction(std::function<TOperationId()> start)
{
    DelayedStartFunction_ = std::move(start);
}

void TOperation::TOperationImpl::Start()
{
    {
        auto guard = Guard(Lock_);
        if (Id_) {
            ythrow TApiUsageError() << "Start() should not be called on running operations";
        }
    }
    GetPreparedFuture().GetValueSync();

    std::function<TOperationId()> startStuff;
    {
        auto guard = Guard(Lock_);
        startStuff.swap(DelayedStartFunction_);
    }
    if (!startStuff) {
        ythrow TApiUsageError() << "Seems that Start() was called multiple times. If not, contact yt@";
    }

    TOperationId operationId;
    try {
        operationId = startStuff();
    } catch (...) {
        auto exception = std::current_exception();
        StartedPromise_.SetException(exception);
        std::rethrow_exception(exception);
    }
    OnStarted(operationId);
}

bool TOperation::TOperationImpl::IsStarted() const {
    auto guard = Guard(Lock_);
    return bool(Id_);
}

void TOperation::TOperationImpl::OnPreparationException(std::exception_ptr e)
{
    Y_ABORT_UNLESS(!PreparedPromise_.HasValue() && !PreparedPromise_.HasException());
    PreparedPromise_.SetException(e);
}

TString TOperation::TOperationImpl::GetStatus()
{
    {
        auto guard = Guard(Lock_);
        if (!Id_) {
            return Status_;
        }
    }
    TMaybe<TString> state;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        state = attributes.State;
    });

    return "On YT cluster: " + state.GetOrElse("undefined state");
}

void TOperation::TOperationImpl::OnStatusUpdated(const TString& newStatus)
{
    auto guard = Guard(Lock_);
    Status_ = newStatus;
}

::NThreading::TFuture<void> TOperation::TOperationImpl::GetPreparedFuture()
{
    return PreparedPromise_.GetFuture();
}

::NThreading::TFuture<void> TOperation::TOperationImpl::GetStartedFuture()
{
    return StartedPromise_.GetFuture();
}

::NThreading::TFuture<void> TOperation::TOperationImpl::Watch(TClientPtr client)
{
    {
        auto guard = Guard(Lock_);
        if (CompletePromise_) {
            return *CompletePromise_;
        }
        CompletePromise_ = ::NThreading::NewPromise<void>();
    }
    GetStartedFuture().Subscribe([
        this_=::TIntrusivePtr(this),
        client=std::move(client)
    ] (const ::NThreading::TFuture<void>& startedResult) {
        try {
            startedResult.GetValue();
        } catch (...) {
            this_->CompletePromise_->SetException(std::current_exception());
            return;
        }
        client->GetYtPoller().Watch(::MakeIntrusive<TOperationPollerItem>(this_));
        auto operationId = this_->GetId();
        auto registry = TAbortableRegistry::Get();
        registry->Add(
            operationId,
            ::MakeIntrusive<TOperationAbortable>(this_->ClientRetryPolicy_, this_->Context_, operationId));
        // We have to own an IntrusivePtr to registry to prevent use-after-free
        auto removeOperation = [registry, operationId] (const ::NThreading::TFuture<void>&) {
            registry->Remove(operationId);
        };
        this_->CompletePromise_->GetFuture().Subscribe(removeOperation);
    });

    return *CompletePromise_;
}

EOperationBriefState TOperation::TOperationImpl::GetBriefState()
{
    ValidateOperationStarted();
    EOperationBriefState result = EOperationBriefState::InProgress;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        Y_ABORT_UNLESS(attributes.BriefState,
            "get_operation for operation %s has not returned \"state\" field",
            GetGuidAsString(*Id_).Data());
        result = *attributes.BriefState;
    });
    return result;
}

TMaybe<TYtError> TOperation::TOperationImpl::GetError()
{
    ValidateOperationStarted();
    TMaybe<TYtError> result;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        Y_ABORT_UNLESS(attributes.Result);
        result = attributes.Result->Error;
    });
    return result;
}

TJobStatistics TOperation::TOperationImpl::GetJobStatistics()
{
    ValidateOperationStarted();
    TJobStatistics result;
    UpdateAttributesAndCall(true, [&] (const TOperationAttributes& attributes) {
        if (attributes.Progress) {
            result = attributes.Progress->JobStatistics;
        }
    });
    return result;
}

TMaybe<TOperationBriefProgress> TOperation::TOperationImpl::GetBriefProgress()
{
    ValidateOperationStarted();
    {
        auto g = Guard(Lock_);
        if (CompletePromise_.Defined()) {
            // Poller do this job for us
            return Attributes_.BriefProgress;
        }
    }
    TMaybe<TOperationBriefProgress> result;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        result = attributes.BriefProgress;
    });
    return result;
}

void TOperation::TOperationImpl::UpdateBriefProgress(TMaybe<TOperationBriefProgress> briefProgress)
{
    auto g = Guard(Lock_);
    Attributes_.BriefProgress = std::move(briefProgress);
}

void TOperation::TOperationImpl::AnalyzeUnrecognizedSpec(TNode unrecognizedSpec)
{
    static const TVector<TVector<TString>> knownUnrecognizedSpecFieldPaths = {
        {"mapper", "class_name"},
        {"reducer", "class_name"},
        {"reduce_combiner", "class_name"},
    };

    auto removeByPath = [] (TNode& node, auto pathBegin, auto pathEnd, auto& removeByPath) {
        if (pathBegin == pathEnd) {
            return;
        }
        if (!node.IsMap()) {
            return;
        }
        auto* child = node.AsMap().FindPtr(*pathBegin);
        if (!child) {
            return;
        }
        removeByPath(*child, std::next(pathBegin), pathEnd, removeByPath);
        if (std::next(pathBegin) == pathEnd || (child->IsMap() && child->Empty())) {
            node.AsMap().erase(*pathBegin);
        }
    };

    Y_ABORT_UNLESS(unrecognizedSpec.IsMap());
    for (const auto& knownFieldPath : knownUnrecognizedSpecFieldPaths) {
        Y_ABORT_UNLESS(!knownFieldPath.empty());
        removeByPath(unrecognizedSpec, knownFieldPath.cbegin(), knownFieldPath.cend(), removeByPath);
    }

    if (!unrecognizedSpec.Empty()) {
        YT_LOG_INFO(
            "WARNING! Unrecognized spec for operation %s is not empty "
            "(fields added by the YT API library are excluded): %s",
            GetGuidAsString(*Id_).Data(),
            NodeToYsonString(unrecognizedSpec).Data());
    }
}

void TOperation::TOperationImpl::OnStarted(const TOperationId& operationId)
{
    auto guard = Guard(Lock_);
    Y_ABORT_UNLESS(!Id_,
        "OnStarted() called with operationId = %s for operation with id %s",
        GetGuidAsString(operationId).Data(),
        GetGuidAsString(*Id_).Data());
    Id_ = operationId;

    Y_ABORT_UNLESS(!StartedPromise_.HasValue() && !StartedPromise_.HasException());
    StartedPromise_.SetValue();
}

void TOperation::TOperationImpl::UpdateAttributesAndCall(bool needJobStatistics, std::function<void(const TOperationAttributes&)> func)
{
    {
        auto g = Guard(Lock_);
        if (Attributes_.BriefState
            && *Attributes_.BriefState != EOperationBriefState::InProgress
            && (!needJobStatistics || Attributes_.Progress))
        {
            func(Attributes_);
            return;
        }
    }

    TOperationAttributes attributes = NDetail::GetOperation(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        Context_,
        *Id_,
        TGetOperationOptions().AttributeFilter(TOperationAttributeFilter()
            .Add(EOperationAttribute::Result)
            .Add(EOperationAttribute::Progress)
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::BriefProgress)));

    func(attributes);

    Y_ENSURE(attributes.BriefState);
    if (*attributes.BriefState != EOperationBriefState::InProgress) {
        auto g = Guard(Lock_);
        Attributes_ = std::move(attributes);
    }
}

void TOperation::TOperationImpl::FinishWithException(std::exception_ptr e)
{
    CompletePromise_->SetException(std::move(e));
}

void TOperation::TOperationImpl::AbortOperation()
{
    ValidateOperationStarted();
    NYT::NDetail::AbortOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_);
}

void TOperation::TOperationImpl::CompleteOperation()
{
    ValidateOperationStarted();
    NYT::NDetail::CompleteOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_);
}

void TOperation::TOperationImpl::SuspendOperation(const TSuspendOperationOptions& options)
{
    ValidateOperationStarted();
    NYT::NDetail::SuspendOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_, options);
}

void TOperation::TOperationImpl::ResumeOperation(const TResumeOperationOptions& options)
{
    ValidateOperationStarted();
    NYT::NDetail::ResumeOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_, options);
}

TOperationAttributes TOperation::TOperationImpl::GetAttributes(const TGetOperationOptions& options)
{
    ValidateOperationStarted();
    return NYT::NDetail::GetOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_, options);
}

void TOperation::TOperationImpl::UpdateParameters(const TUpdateOperationParametersOptions& options)
{
    ValidateOperationStarted();
    return NYT::NDetail::UpdateOperationParameters(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_, options);
}

TJobAttributes TOperation::TOperationImpl::GetJob(const TJobId& jobId, const TGetJobOptions& options)
{
    ValidateOperationStarted();
    return NYT::NDetail::GetJob(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_, jobId, options);
}

TListJobsResult TOperation::TOperationImpl::ListJobs(const TListJobsOptions& options)
{
    ValidateOperationStarted();
    return NYT::NDetail::ListJobs(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, *Id_, options);
}

struct TAsyncFinishOperationsArgs
{
    ::TIntrusivePtr<TOperation::TOperationImpl> OperationImpl;
    TOperationAttributes OperationAttributes;
};

void TOperation::TOperationImpl::AsyncFinishOperation(TOperationAttributes operationAttributes)
{
    auto args = new TAsyncFinishOperationsArgs;
    args->OperationImpl = this;
    args->OperationAttributes = std::move(operationAttributes);

    TThread thread(TThread::TParams(&TOperation::TOperationImpl::SyncFinishOperationProc, args).SetName("finish operation"));
    thread.Start();
    thread.Detach();
}

void* TOperation::TOperationImpl::SyncFinishOperationProc(void* pArgs)
{
    THolder<TAsyncFinishOperationsArgs> args(static_cast<TAsyncFinishOperationsArgs*>(pArgs));
    args->OperationImpl->SyncFinishOperationImpl(args->OperationAttributes);
    return nullptr;
}

void TOperation::TOperationImpl::SyncFinishOperationImpl(const TOperationAttributes& attributes)
{
    {
        auto guard = Guard(Lock_);
        Y_ABORT_UNLESS(Id_);
    }
    Y_ABORT_UNLESS(attributes.BriefState,
        "get_operation for operation %s has not returned \"state\" field",
        GetGuidAsString(*Id_).Data());
    Y_ABORT_UNLESS(*attributes.BriefState != EOperationBriefState::InProgress);

    {
        try {
            // `attributes' that came from poller don't have JobStatistics
            // so we call `GetJobStatistics' in order to get it from server
            // and cache inside object.
            GetJobStatistics();
        } catch (const TErrorResponse& ) {
            // But if for any reason we failed to get attributes
            // we complete operation using what we have.
            auto g = Guard(Lock_);
            Attributes_ = attributes;
        }
    }

    if (*attributes.BriefState == EOperationBriefState::Completed) {
        CompletePromise_->SetValue();
    } else if (*attributes.BriefState == EOperationBriefState::Aborted || *attributes.BriefState == EOperationBriefState::Failed) {
        Y_ABORT_UNLESS(attributes.Result && attributes.Result->Error);
        const auto& error = *attributes.Result->Error;
        YT_LOG_ERROR("Operation %v is `%v' with error: %v",
            *Id_,
            ToString(*attributes.BriefState),
            error.FullDescription());

        TString additionalExceptionText;
        TVector<TFailedJobInfo> failedJobStderrInfo;
        if (*attributes.BriefState == EOperationBriefState::Failed) {
            try {
                failedJobStderrInfo = NYT::NDetail::GetFailedJobInfo(ClientRetryPolicy_, Context_, *Id_, TGetFailedJobInfoOptions());
            } catch (const std::exception& e) {
                additionalExceptionText = "Cannot get job stderrs: ";
                additionalExceptionText += e.what();
            }
        }
        CompletePromise_->SetException(
            std::make_exception_ptr(
                TOperationFailedError(
                    *attributes.BriefState == EOperationBriefState::Failed
                        ? TOperationFailedError::Failed
                        : TOperationFailedError::Aborted,
                    *Id_,
                    error,
                    failedJobStderrInfo) << additionalExceptionText));
    }
}

void TOperation::TOperationImpl::ValidateOperationStarted() const
{
    auto guard = Guard(Lock_);
    if (!Id_) {
        ythrow TApiUsageError() << "Operation is not started";
    }
}

const TClientContext& TOperation::TOperationImpl::GetContext() const
{
    return Context_;
}

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(TClientPtr client)
    : Client_(std::move(client))
    , Impl_(::MakeIntrusive<TOperationImpl>(Client_->GetRetryPolicy(), Client_->GetContext()))
{
}

TOperation::TOperation(TOperationId id, TClientPtr client)
    : Client_(std::move(client))
    , Impl_(::MakeIntrusive<TOperationImpl>(Client_->GetRetryPolicy(), Client_->GetContext(), id))
{
}

const TOperationId& TOperation::GetId() const
{
    return Impl_->GetId();
}

TString TOperation::GetWebInterfaceUrl() const
{
    return Impl_->GetWebInterfaceUrl();
}

void TOperation::OnPrepared()
{
    Impl_->OnPrepared();
}

void TOperation::SetDelayedStartFunction(std::function<TOperationId()> start)
{
    Impl_->SetDelayedStartFunction(std::move(start));
}

void TOperation::Start()
{
    Impl_->Start();
}

bool TOperation::IsStarted() const
{
    return Impl_->IsStarted();
}

void TOperation::OnPreparationException(std::exception_ptr e)
{
    Impl_->OnPreparationException(std::move(e));
}

TString TOperation::GetStatus() const
{
    return Impl_->GetStatus();
}

void TOperation::OnStatusUpdated(const TString& newStatus)
{
    Impl_->OnStatusUpdated(newStatus);
}

::NThreading::TFuture<void> TOperation::GetPreparedFuture()
{
    return Impl_->GetPreparedFuture();
}

::NThreading::TFuture<void> TOperation::GetStartedFuture()
{
    return Impl_->GetStartedFuture();
}

::NThreading::TFuture<void> TOperation::Watch()
{
    return Impl_->Watch(Client_);
}

TVector<TFailedJobInfo> TOperation::GetFailedJobInfo(const TGetFailedJobInfoOptions& options)
{
    return NYT::NDetail::GetFailedJobInfo(Client_->GetRetryPolicy(), Client_->GetContext(), GetId(), options);
}

EOperationBriefState TOperation::GetBriefState()
{
    return Impl_->GetBriefState();
}

TMaybe<TYtError> TOperation::GetError()
{
    return Impl_->GetError();
}

TJobStatistics TOperation::GetJobStatistics()
{
    return Impl_->GetJobStatistics();
}

TMaybe<TOperationBriefProgress> TOperation::GetBriefProgress()
{
    return Impl_->GetBriefProgress();
}

void TOperation::AbortOperation()
{
    Impl_->AbortOperation();
}

void TOperation::CompleteOperation()
{
    Impl_->CompleteOperation();
}

void TOperation::SuspendOperation(const TSuspendOperationOptions& options)
{
    Impl_->SuspendOperation(options);
}

void TOperation::ResumeOperation(const TResumeOperationOptions& options)
{
    Impl_->ResumeOperation(options);
}

TOperationAttributes TOperation::GetAttributes(const TGetOperationOptions& options)
{
    return Impl_->GetAttributes(options);
}

void TOperation::UpdateParameters(const TUpdateOperationParametersOptions& options)
{
    Impl_->UpdateParameters(options);
}

TJobAttributes TOperation::GetJob(const TJobId& jobId, const TGetJobOptions& options)
{
    return Impl_->GetJob(jobId, options);
}

TListJobsResult TOperation::ListJobs(const TListJobsOptions& options)
{
    return Impl_->ListJobs(options);
}

////////////////////////////////////////////////////////////////////////////////

struct TAsyncPrepareAndStartOperationArgs
{
    std::function<void()> PrepareAndStart;
};

void* SyncPrepareAndStartOperation(void* pArgs)
{
    THolder<TAsyncPrepareAndStartOperationArgs> args(static_cast<TAsyncPrepareAndStartOperationArgs*>(pArgs));
    args->PrepareAndStart();
    return nullptr;
}

::TIntrusivePtr<TOperation> ProcessOperation(
    NYT::NDetail::TClientPtr client,
    std::function<void()> prepare,
    ::TIntrusivePtr<TOperation> operation,
    const TOperationOptions& options)
{
    auto prepareAndStart = [prepare = std::move(prepare), operation, mode = options.StartOperationMode_] () {
        try {
            prepare();
            operation->OnPrepared();
        } catch (...) {
            operation->OnPreparationException(std::current_exception());
        }
        if (mode >= TOperationOptions::EStartOperationMode::AsyncStart) {
            try {
                operation->Start();
            } catch (...) { }
        }
    };
    if (options.StartOperationMode_ >= TOperationOptions::EStartOperationMode::SyncStart) {
        prepareAndStart();
        WaitIfRequired(operation, client, options);
    } else {
        auto args = new TAsyncPrepareAndStartOperationArgs;
        args->PrepareAndStart = std::move(prepareAndStart);

        TThread thread(TThread::TParams(SyncPrepareAndStartOperation, args).SetName("prepare and start operation"));
        thread.Start();
        thread.Detach();
    }
    return operation;
}

void WaitIfRequired(const TOperationPtr& operation, const TClientPtr& client, const TOperationOptions& options)
{
    auto retryPolicy = client->GetRetryPolicy();
    auto context = client->GetContext();
    if (options.StartOperationMode_ >= TOperationOptions::EStartOperationMode::SyncStart) {
        operation->GetStartedFuture().GetValueSync();
    }
    if (options.StartOperationMode_ == TOperationOptions::EStartOperationMode::SyncWait) {
        auto finishedFuture = operation->Watch();
        TWaitProxy::Get()->WaitFuture(finishedFuture);
        finishedFuture.GetValue();
        if (context.Config->WriteStderrSuccessfulJobs) {
            auto stderrs = GetJobsStderr(retryPolicy, context, operation->GetId());
            for (const auto& jobStderr : stderrs) {
                if (!jobStderr.empty()) {
                    Cerr << jobStderr << '\n';
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ResetUseClientProtobuf(const char* methodName)
{
    Cerr << "WARNING! OPTION `TConfig::UseClientProtobuf' IS RESET TO `true'; "
        << "IT CAN DETERIORATE YOUR CODE PERFORMANCE!!! DON'T USE DEPRECATED METHOD `"
        << "TOperationIOSpec::" << methodName << "' TO AVOID THIS RESET" << Endl;
    // Give users some time to contemplate about usage of deprecated functions.
    Cerr << "Sleeping for 5 seconds..." << Endl;
    Sleep(TDuration::Seconds(5));
    TConfig::Get()->UseClientProtobuf = true;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<INodeReaderImpl> CreateJobNodeReader(TRawTableReaderPtr rawTableReader)
{
    if (auto schema = NDetail::GetJobInputSkiffSchema()) {
        return new NDetail::TSkiffTableReader(rawTableReader, schema);
    } else {
        return new TNodeTableReader(rawTableReader);
    }
}

::TIntrusivePtr<IYaMRReaderImpl> CreateJobYaMRReader(TRawTableReaderPtr rawTableReader)
{
    return new TYaMRTableReader(rawTableReader);
}

::TIntrusivePtr<IProtoReaderImpl> CreateJobProtoReader(TRawTableReaderPtr rawTableReader)
{
    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableReader(
            rawTableReader,
            GetJobInputDescriptors());
    } else {
        return new TLenvalProtoTableReader(
            rawTableReader,
            GetJobInputDescriptors());
    }
}

::TIntrusivePtr<INodeWriterImpl> CreateJobNodeWriter(THolder<IProxyOutput> rawJobWriter)
{
    return new TNodeTableWriter(std::move(rawJobWriter));
}

::TIntrusivePtr<IYaMRWriterImpl> CreateJobYaMRWriter(THolder<IProxyOutput> rawJobWriter)
{
    return new TYaMRTableWriter(std::move(rawJobWriter));
}

::TIntrusivePtr<IProtoWriterImpl> CreateJobProtoWriter(THolder<IProxyOutput> rawJobWriter)
{
    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableWriter(
            std::move(rawJobWriter),
            GetJobOutputDescriptors());
    } else {
        return new TLenvalProtoTableWriter(
            std::move(rawJobWriter),
            GetJobOutputDescriptors());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
