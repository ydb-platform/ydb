#pragma once

#include "client_common.h"

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/client/scheduler/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TStartOperationOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TAbortOperationOptions
    : public TTimeoutOptions
{
    std::optional<TString> AbortMessage;
};

struct TSuspendOperationOptions
    : public TTimeoutOptions
{
    bool AbortRunningJobs = false;
};

struct TResumeOperationOptions
    : public TTimeoutOptions
{ };

struct TCompleteOperationOptions
    : public TTimeoutOptions
{ };

struct TUpdateOperationParametersOptions
    : public TTimeoutOptions
{ };

struct TDumpJobContextOptions
    : public TTimeoutOptions
{ };

//! Source to fetch job spec from. Useful in tests.
DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EJobSpecSource, ui16,
    //! Job spec is fetched from exec node.
    ((Node) (1))

    //! Job spec is fetched from job archive.
    ((Archive) (2))

    //! Job spec is fetched from any available source.
    ((Auto) (0xFFFF))
);

struct TGetJobInputOptions
    : public TTimeoutOptions
{
    //! Where job spec should be retrieved from.
    EJobSpecSource JobSpecSource = EJobSpecSource::Auto;
};

struct TGetJobInputPathsOptions
    : public TTimeoutOptions
{
    //! Where job spec should be retrieved from.
    EJobSpecSource JobSpecSource = EJobSpecSource::Auto;
};

struct TGetJobSpecOptions
    : public TTimeoutOptions
{
    //! Where job spec should be retrieved from.
    EJobSpecSource JobSpecSource = EJobSpecSource::Auto;

    bool OmitNodeDirectory = false;
    bool OmitInputTableSpecs = false;
    bool OmitOutputTableSpecs = false;
};

struct TGetJobStderrOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{ };

struct TGetJobFailContextOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{ };

struct TListOperationsAccessFilter
    : public NYTree::TYsonStruct
{
    TString Subject;
    NYTree::EPermissionSet Permissions;

    // This parameter cannot be set from YSON, it must be computed.
    THashSet<TString> SubjectTransitiveClosure;

    REGISTER_YSON_STRUCT(TListOperationsAccessFilter);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TListOperationsAccessFilter)

struct TListOperationsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<TInstant> FromTime;
    std::optional<TInstant> ToTime;
    std::optional<TInstant> CursorTime;
    EOperationSortDirection CursorDirection = EOperationSortDirection::Past;
    std::optional<TString> UserFilter;

    TListOperationsAccessFilterPtr AccessFilter;

    std::optional<NScheduler::EOperationState> StateFilter;
    std::optional<NScheduler::EOperationType> TypeFilter;
    std::optional<TString> SubstrFilter;
    std::optional<TString> PoolTree;
    std::optional<TString> Pool;
    std::optional<bool> WithFailedJobs;
    bool IncludeArchive = false;
    bool IncludeCounters = true;
    ui64 Limit = 100;

    std::optional<THashSet<TString>> Attributes;

    // TODO(ignat): Remove this mode when UI migrate to list_operations without enabled UI mode.
    // See st/YTFRONT-1360.
    bool EnableUIMode = false;

    TDuration ArchiveFetchingTimeout = TDuration::Seconds(3);

    TListOperationsOptions()
    {
        ReadFrom = EMasterChannelKind::Cache;
    }
};

struct TPollJobShellResponse
{
    NYson::TYsonString Result;
    // YT-14507: Logging context is required for SOC audit.
    NYson::TYsonString LoggingContext;
};

DEFINE_ENUM(EJobSortField,
    ((None)       (0))
    ((Type)       (1))
    ((State)      (2))
    ((StartTime)  (3))
    ((FinishTime) (4))
    ((Address)    (5))
    ((Duration)   (6))
    ((Progress)   (7))
    ((Id)         (8))
);

DEFINE_ENUM(EJobSortDirection,
    ((Ascending)  (0))
    ((Descending) (1))
);

DEFINE_ENUM(EDataSource,
    ((Archive) (0))
    ((Runtime) (1))
    ((Auto)    (2))
    // Should be used only in tests.
    ((Manual)  (3))
);

struct TListJobsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    NJobTrackerClient::TJobId JobCompetitionId;
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> State;
    std::optional<TString> Address;
    std::optional<bool> WithStderr;
    std::optional<bool> WithFailContext;
    std::optional<bool> WithSpec;
    std::optional<bool> WithCompetitors;
    std::optional<bool> WithMonitoringDescriptor;
    std::optional<TString> TaskName;

    TDuration RunningJobsLookbehindPeriod = TDuration::Max();

    EJobSortField SortField = EJobSortField::None;
    EJobSortDirection SortOrder = EJobSortDirection::Ascending;

    i64 Limit = 1000;
    i64 Offset = 0;

    // All options below are deprecated.
    bool IncludeCypress = false;
    bool IncludeControllerAgent = false;
    bool IncludeArchive = false;
    EDataSource DataSource = EDataSource::Auto;
};

struct TAbandonJobOptions
    : public TTimeoutOptions
{ };

struct TPollJobShellOptions
    : public TTimeoutOptions
{ };

struct TAbortJobOptions
    : public TTimeoutOptions
{
    std::optional<TDuration> InterruptTimeout;
};

struct TDumpJobProxyLogOptions
    : public TTimeoutOptions
{ };

struct TGetOperationOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<THashSet<TString>> Attributes;
    TDuration ArchiveTimeout = TDuration::Seconds(5);
    TDuration MaximumCypressProgressAge = TDuration::Minutes(2);
    bool IncludeRuntime = false;
};

struct TGetJobOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<THashSet<TString>> Attributes;
};

struct TOperation
{
    std::optional<NScheduler::TOperationId> Id;

    std::optional<NScheduler::EOperationType> Type;
    std::optional<NScheduler::EOperationState> State;

    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;

    std::optional<TString> AuthenticatedUser;

    NYson::TYsonString BriefSpec;
    NYson::TYsonString Spec;
    NYson::TYsonString ProvidedSpec;
    NYson::TYsonString ExperimentAssignments;
    NYson::TYsonString ExperimentAssignmentNames;
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;

    NYson::TYsonString BriefProgress;
    NYson::TYsonString Progress;

    NYson::TYsonString RuntimeParameters;

    std::optional<bool> Suspended;

    NYson::TYsonString Events;
    NYson::TYsonString Result;

    NYson::TYsonString SlotIndexPerPoolTree;
    NYson::TYsonString SchedulingAttributesPerPoolTree;
    NYson::TYsonString Alerts;
    NYson::TYsonString AlertEvents;

    NYson::TYsonString TaskNames;

    NYson::TYsonString ControllerFeatures;

    NYTree::IAttributeDictionaryPtr OtherAttributes;
};

void Serialize(
    const TOperation& operation,
    NYson::IYsonConsumer* consumer,
    bool needType = true,
    bool needOperationType = false,
    bool idWithAttributes = false);

void Deserialize(TOperation& operation, NYTree::IAttributeDictionaryPtr attriubutes, bool clone = true);

struct TListOperationsResult
{
    std::vector<TOperation> Operations;
    std::optional<THashMap<TString, i64>> PoolTreeCounts;
    std::optional<THashMap<TString, i64>> PoolCounts;
    std::optional<THashMap<TString, i64>> UserCounts;
    std::optional<TEnumIndexedArray<NScheduler::EOperationState, i64>> StateCounts;
    std::optional<TEnumIndexedArray<NScheduler::EOperationType, i64>> TypeCounts;
    std::optional<i64> FailedJobsCount;
    bool Incomplete = false;
};

struct TJob
{
    NJobTrackerClient::TJobId Id;
    NJobTrackerClient::TOperationId OperationId;
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> ControllerState;
    std::optional<NJobTrackerClient::EJobState> ArchiveState;
    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;
    std::optional<TString> Address;
    std::optional<double> Progress;
    std::optional<ui64> StderrSize;
    std::optional<ui64> FailContextSize;
    std::optional<bool> HasSpec;
    std::optional<bool> HasCompetitors;
    std::optional<bool> HasProbingCompetitors;
    NJobTrackerClient::TJobId JobCompetitionId;
    NJobTrackerClient::TJobId ProbingJobCompetitionId;
    NYson::TYsonString Error;
    NYson::TYsonString InterruptionInfo;
    NYson::TYsonString BriefStatistics;
    NYson::TYsonString Statistics;
    NYson::TYsonString InputPaths;
    NYson::TYsonString CoreInfos;
    NYson::TYsonString Events;
    NYson::TYsonString ExecAttributes;
    std::optional<TString> TaskName;
    std::optional<TString> PoolTree;
    std::optional<TString> Pool;
    std::optional<TString> MonitoringDescriptor;
    std::optional<ui64> JobCookie;
    NYson::TYsonString ArchiveFeatures;

    std::optional<bool> IsStale;

    std::optional<NJobTrackerClient::EJobState> GetState() const;
};

void Serialize(const TJob& job, NYson::IYsonConsumer* consumer, TStringBuf idKey);

struct TListJobsStatistics
{
    TEnumIndexedArray<NJobTrackerClient::EJobState, i64> StateCounts;
    TEnumIndexedArray<NJobTrackerClient::EJobType, i64> TypeCounts;
};

struct TListJobsResult
{
    std::vector<TJob> Jobs;
    std::optional<int> CypressJobCount;
    std::optional<int> ControllerAgentJobCount;
    std::optional<int> ArchiveJobCount;

    TListJobsStatistics Statistics;

    std::vector<TError> Errors;
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationClient
{
    virtual ~IOperationClient() = default;

    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options = {}) = 0;

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options = {}) = 0;

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options = {}) = 0;

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options = {}) = 0;

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options = {}) = 0;

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options = {}) = 0;

    virtual TFuture<TOperation> GetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options = {}) = 0;

    virtual TFuture<void> DumpJobContext(
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options = {}) = 0;

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJobInputPaths(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJobSpec(
        NJobTrackerClient::TJobId jobId,
        const TGetJobSpecOptions& options = {}) = 0;

    virtual TFuture<TSharedRef> GetJobStderr(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options = {}) = 0;

    virtual TFuture<TSharedRef> GetJobFailContext(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options = {}) = 0;

    virtual TFuture<TListOperationsResult> ListOperations(
        const TListOperationsOptions& options = {}) = 0;

    virtual TFuture<TListJobsResult> ListJobs(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJob(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options = {}) = 0;

    virtual TFuture<void> AbandonJob(
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options = {}) = 0;

    virtual TFuture<TPollJobShellResponse> PollJobShell(
        NJobTrackerClient::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options = {}) = 0;

    virtual TFuture<void> AbortJob(
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options = {}) = 0;

    virtual TFuture<void> DumpJobProxyLog(
        NJobTrackerClient::TJobId jobId,
        NJobTrackerClient::TOperationId operationId,
        const NYPath::TYPath& path,
        const TDumpJobProxyLogOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

