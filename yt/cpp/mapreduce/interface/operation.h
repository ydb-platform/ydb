#pragma once

///
/// @file yt/cpp/mapreduce/interface/operation.h
///
/// Header containing interface to run operations in YT
/// and retrieve information about them.
/// @see [the doc](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map_reduce_overview.html).

#include "client_method_options.h"
#include "errors.h"
#include "io.h"
#include "job_statistics.h"
#include "job_counters.h"

#include <library/cpp/threading/future/future.h>
#include <library/cpp/type_info/type_info.h>

#include <util/datetime/base.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/system/file.h>
#include <util/system/types.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// Tag class marking that the row type for table is not specified.
struct TUnspecifiedTableStructure
{ };

/// Tag class marking that table rows have protobuf type.
struct TProtobufTableStructure
{
    /// @brief Descriptor of the protobuf type of table rows.
    ///
    /// @note If table is tagged with @ref ::google::protobuf::Message instead of real proto class
    /// this descriptor might be null.
    const ::google::protobuf::Descriptor* Descriptor = nullptr;
};


/// Tag class to specify table row type.
using TTableStructure = std::variant<
    TUnspecifiedTableStructure,
    TProtobufTableStructure
>;

bool operator==(const TUnspecifiedTableStructure&, const TUnspecifiedTableStructure&);
bool operator==(const TProtobufTableStructure& lhs, const TProtobufTableStructure& rhs);

/// Table path marked with @ref NYT::TTableStructure tag.
struct TStructuredTablePath
{
    TStructuredTablePath(TRichYPath richYPath = TRichYPath(), TTableStructure description = TUnspecifiedTableStructure())
        : RichYPath(std::move(richYPath))
        , Description(std::move(description))
    { }

    TStructuredTablePath(TRichYPath richYPath, const ::google::protobuf::Descriptor* descriptor)
        : RichYPath(std::move(richYPath))
        , Description(TProtobufTableStructure({descriptor}))
    { }

    TStructuredTablePath(TYPath path)
        : RichYPath(std::move(path))
        , Description(TUnspecifiedTableStructure())
    { }

    TStructuredTablePath(const char* path)
        : RichYPath(path)
        , Description(TUnspecifiedTableStructure())
    { }

    TRichYPath RichYPath;
    TTableStructure Description;
};

/// Create marked table path from row type.
template <typename TRow>
TStructuredTablePath Structured(TRichYPath richYPath);

/// Create tag class from row type.
template <typename TRow>
TTableStructure StructuredTableDescription();

////////////////////////////////////////////////////////////////////////////////

/// Tag class marking that row stream is empty.
struct TVoidStructuredRowStream
{ };

/// Tag class marking that row stream consists of `NYT::TNode`.
struct TTNodeStructuredRowStream
{ };

/// Tag class marking that row stream consists of @ref NYT::TYaMRRow.
struct TTYaMRRowStructuredRowStream
{ };

/// Tag class marking that row stream consists of protobuf rows of given type.
struct TProtobufStructuredRowStream
{
    /// @brief Descriptor of the protobuf type of table rows.
    ///
    /// @note If `Descriptor` is nullptr, then row stream consists of multiple message types.
    const ::google::protobuf::Descriptor* Descriptor = nullptr;
};

/// Tag class to specify type of rows in an operation row stream
using TStructuredRowStreamDescription = std::variant<
    TVoidStructuredRowStream,
    TTNodeStructuredRowStream,
    TTYaMRRowStructuredRowStream,
    TProtobufStructuredRowStream
>;

////////////////////////////////////////////////////////////////////////////////

/// Tag class marking that current binary should be used in operation.
struct TJobBinaryDefault
{ };

/// Tag class marking that binary from specified local path should be used in operation.
struct TJobBinaryLocalPath
{
    TString Path;
    TMaybe<TString> MD5CheckSum;
};

/// Tag class marking that binary from specified Cypress path should be used in operation.
struct TJobBinaryCypressPath
{
    TYPath Path;
    TMaybe<TTransactionId> TransactionId;
};

////////////////////////////////////////////////////////////////////////////////


/// @cond Doxygen_Suppress
namespace NDetail {
    extern i64 OutputTableCount;
} // namespace NDetail
/// @endcond

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Auto merge mode.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/automerge
enum class EAutoMergeMode
{
    /// Auto merge is disabled.
    Disabled   /* "disabled" */,

    /// Mode that tries to achieve good chunk sizes and doesn't limit usage of chunk quota for intermediate chunks.
    Relaxed    /* "relaxed" */,

    /// Mode that tries to optimize usage of chunk quota for intermediate chunks, operation might run slower.
    Economy    /* "economy" */,

    ///
    /// @brief Manual configuration of automerge parameters.
    ///
    /// @ref TAutoMergeSpec
    Manual     /* "manual" */,
};

///
/// @brief Options for auto merge operation stage.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/automerge
class TAutoMergeSpec
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TAutoMergeSpec;
    /// @endcond

    /// Mode of the auto merge.
    FLUENT_FIELD_OPTION(EAutoMergeMode, Mode);

    /// @brief Upper limit for number of intermediate chunks.
    ///
    /// Works only for Manual mode.
    FLUENT_FIELD_OPTION(i64, MaxIntermediateChunkCount);

    /// @brief Number of chunks limit to merge in one job.
    ///
    /// Works only for Manual mode.
    FLUENT_FIELD_OPTION(i64, ChunkCountPerMergeJob);

    /// @brief Automerge will not merge chunks that are larger than `DesiredChunkSize * (ChunkSizeThreshold / 100.)`
    ///
    /// Works only for Manual mode.
    FLUENT_FIELD_OPTION(i64, ChunkSizeThreshold);
};

/// Base for operations with auto merge options.
template <class TDerived>
class TWithAutoMergeSpec
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Options for auto merge operation stage.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/automerge
    FLUENT_FIELD_OPTION(TAutoMergeSpec, AutoMerge);
};

///
/// @brief Resources controlled by scheduler and used by running operations.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/scheduler/scheduler-and-pools#resources
class TSchedulerResources
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TSchedulerResources;
    /// @endcond

    /// Each job consumes exactly one user slot.
    FLUENT_FIELD_OPTION_ENCAPSULATED(i64, UserSlots);

    /// Number of (virtual) cpu cores consumed by all jobs.
    FLUENT_FIELD_OPTION_ENCAPSULATED(i64, Cpu);

    /// Amount of memory in bytes.
    FLUENT_FIELD_OPTION_ENCAPSULATED(i64, Memory);
};

/// Base for input format hints of a user job.
template <class TDerived>
class TUserJobInputFormatHintsBase
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Fine tune input format of the job.
    FLUENT_FIELD_OPTION(TFormatHints, InputFormatHints);
};

/// Base for output format hints of a user job.
template <class TDerived>
class TUserJobOutputFormatHintsBase
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Fine tune output format of the job.
    FLUENT_FIELD_OPTION(TFormatHints, OutputFormatHints);
};

/// Base for format hints of a user job.
template <class TDerived>
class TUserJobFormatHintsBase
    : public TUserJobInputFormatHintsBase<TDerived>
    , public TUserJobOutputFormatHintsBase<TDerived>
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond
};

/// User job format hints.
class TUserJobFormatHints
    : public TUserJobFormatHintsBase<TUserJobFormatHints>
{ };

/// Spec of input and output tables of a raw operation.
template <class TDerived>
class TRawOperationIoTableSpec
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// Add input table path to input path list.
    TDerived& AddInput(const TRichYPath& path);

    /// Set input table path no. `tableIndex`.
    TDerived& SetInput(size_t tableIndex, const TRichYPath& path);

    /// Add output table path to output path list.
    TDerived& AddOutput(const TRichYPath& path);

    /// Set output table path no. `tableIndex`.
    TDerived& SetOutput(size_t tableIndex, const TRichYPath& path);

    /// Get all input table paths.
    const TVector<TRichYPath>& GetInputs() const;

    /// Get all output table paths.
    const TVector<TRichYPath>& GetOutputs() const;

private:
    TVector<TRichYPath> Inputs_;
    TVector<TRichYPath> Outputs_;
};

/// Base spec for IO in "simple" raw operations (Map, Reduce etc.).
template <class TDerived>
struct TSimpleRawOperationIoSpec
    : public TRawOperationIoTableSpec<TDerived>
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Describes format for both input and output.
    ///
    /// @note `Format' is overridden by `InputFormat' and `OutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, Format);

    /// Describes input format.
    FLUENT_FIELD_OPTION(TFormat, InputFormat);

    /// Describes output format.
    FLUENT_FIELD_OPTION(TFormat, OutputFormat);
};

/// Spec for IO in MapReduce operation.
template <class TDerived>
class TRawMapReduceOperationIoSpec
    : public TRawOperationIoTableSpec<TDerived>
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Describes format for both input and output of mapper.
    ///
    /// @note `MapperFormat' is overridden by `MapperInputFormat' and `MapperOutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, MapperFormat);

    /// Describes mapper input format.
    FLUENT_FIELD_OPTION(TFormat, MapperInputFormat);

    /// Describes mapper output format.
    FLUENT_FIELD_OPTION(TFormat, MapperOutputFormat);

    /// @brief Describes format for both input and output of reduce combiner.
    ///
    /// @note `ReduceCombinerFormat' is overridden by `ReduceCombinerInputFormat' and `ReduceCombinerOutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, ReduceCombinerFormat);

    /// Describes reduce combiner input format.
    FLUENT_FIELD_OPTION(TFormat, ReduceCombinerInputFormat);

    /// Describes reduce combiner output format.
    FLUENT_FIELD_OPTION(TFormat, ReduceCombinerOutputFormat);

    /// @brief Describes format for both input and output of reducer.
    ///
    /// @note `ReducerFormat' is overridden by `ReducerInputFormat' and `ReducerOutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, ReducerFormat);

    /// Describes reducer input format.
    FLUENT_FIELD_OPTION(TFormat, ReducerInputFormat);

    /// Describes reducer output format.
    FLUENT_FIELD_OPTION(TFormat, ReducerOutputFormat);

    /// Add direct map output table path.
    TDerived& AddMapOutput(const TRichYPath& path);

    /// Set direct map output table path no. `tableIndex`.
    TDerived& SetMapOutput(size_t tableIndex, const TRichYPath& path);

    /// Get all direct map output table paths
    const TVector<TRichYPath>& GetMapOutputs() const;

private:
    TVector<TRichYPath> MapOutputs_;
};

///
/// @brief Base spec of operations with input tables.
class TOperationInputSpecBase
{
public:
    template <class T, class = void>
    struct TFormatAdder;

    ///
    /// @brief Add input table path to input path list and specify type of rows.
    template <class T>
    void AddInput(const TRichYPath& path);

    ///
    /// @brief Add input table path as structured paths.
    void AddStructuredInput(TStructuredTablePath path);

    ///
    /// @brief Set input table path and type.
    template <class T>
    void SetInput(size_t tableIndex, const TRichYPath& path);

    ///
    /// @brief All input paths.
    TVector<TRichYPath> Inputs_;

    ///
    /// @brief Get all input structured paths.
    const TVector<TStructuredTablePath>& GetStructuredInputs() const;

private:
    TVector<TStructuredTablePath> StructuredInputs_;
    friend struct TOperationIOSpecBase;
    template <class T>
    friend struct TOperationIOSpec;
};

///
/// @brief Base spec of operations with output tables.
class TOperationOutputSpecBase
{
public:
    template <class T, class = void>
    struct TFormatAdder;

    ///
    /// @brief Add output table path to output path list and specify type of rows.
    template <class T>
    void AddOutput(const TRichYPath& path);

    ///
    /// @brief Add output table path as structured paths.
    void AddStructuredOutput(TStructuredTablePath path);

    ///
    /// @brief Set output table path and type.
    template <class T>
    void SetOutput(size_t tableIndex, const TRichYPath& path);

    ///
    /// @brief All output paths.
    TVector<TRichYPath> Outputs_;

    ///
    /// @brief Get all output structured paths.
    const TVector<TStructuredTablePath>& GetStructuredOutputs() const;

private:
    TVector<TStructuredTablePath> StructuredOutputs_;
    friend struct TOperationIOSpecBase;
    template <class T>
    friend struct TOperationIOSpec;
};

///
/// @brief Base spec for operations with inputs and outputs.
struct TOperationIOSpecBase
    : public TOperationInputSpecBase
    , public TOperationOutputSpecBase
{ };

///
/// @brief Base spec for operations with inputs and outputs.
template <class TDerived>
struct TOperationIOSpec
    : public TOperationIOSpecBase
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    template <class T>
    TDerived& AddInput(const TRichYPath& path);

    TDerived& AddStructuredInput(TStructuredTablePath path);

    template <class T>
    TDerived& SetInput(size_t tableIndex, const TRichYPath& path);

    template <class T>
    TDerived& AddOutput(const TRichYPath& path);

    TDerived& AddStructuredOutput(TStructuredTablePath path);

    template <class T>
    TDerived& SetOutput(size_t tableIndex, const TRichYPath& path);


    // DON'T USE THESE METHODS! They are left solely for backward compatibility.
    // These methods are the only way to do equivalent of (Add/Set)(Input/Output)<Message>
    // but please consider using (Add/Set)(Input/Output)<TConcreteMessage>
    // (where TConcreteMessage is some descendant of Message)
    // because they are faster and better (see https://st.yandex-team.ru/YT-6967)
    TDerived& AddProtobufInput_VerySlow_Deprecated(const TRichYPath& path);
    TDerived& AddProtobufOutput_VerySlow_Deprecated(const TRichYPath& path);
};

///
/// @brief Base spec for all operations.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations_options
template <class TDerived>
struct TOperationSpecBase
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Limit on operation execution time.
    ///
    /// If operation doesn't finish in time it will be aborted.
    FLUENT_FIELD_OPTION(TDuration, TimeLimit);

    /// @brief Title to be shown in web interface.
    FLUENT_FIELD_OPTION(TString, Title);

    /// @brief Pool to be used for this operation.
    FLUENT_FIELD_OPTION(TString, Pool);

    /// @brief Weight of operation.
    ///
    /// Coefficient defining how much resources operation gets relative to its siblings in the same pool.
    FLUENT_FIELD_OPTION(double, Weight);

    /// @brief Pool tree list that operation will use.
    FLUENT_OPTIONAL_VECTOR_FIELD_ENCAPSULATED(TString, PoolTree);

    /// How much resources can be consumed by operation.
    FLUENT_FIELD_OPTION_ENCAPSULATED(TSchedulerResources, ResourceLimits);

    /// How many jobs can fail before operation is failed.
    FLUENT_FIELD_OPTION(ui64, MaxFailedJobCount);
};

///
/// @brief Base spec for all operations with user jobs.
template <class TDerived>
struct TUserOperationSpecBase
    : TOperationSpecBase<TDerived>
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// On any unsuccessful job completion (i.e. abortion or failure) force the whole operation to fail.
    FLUENT_FIELD_OPTION(bool, FailOnJobRestart);

    ///
    /// @brief Table to save whole stderr of operation.
    ///
    /// @see https://clubs.at.yandex-team.ru/yt/1045
    FLUENT_FIELD_OPTION(TYPath, StderrTablePath);

    ///
    /// @brief Table to save coredumps of operation.
    ///
    /// @see https://clubs.at.yandex-team.ru/yt/1045
    FLUENT_FIELD_OPTION(TYPath, CoreTablePath);

    ///
    /// @brief How long should the scheduler wait for the job to be started on a node.
    ///
    /// When you run huge jobs that require preemption of all the other jobs on
    /// a node, the default timeout might be insufficient and your job may be
    /// aborted with 'waiting_timeout' reason. This is especially problematic
    /// when you are setting 'FailOnJobRestart' option.
    ///
    /// @note The value must be between 10 seconds and 10 minutes.
    FLUENT_FIELD_OPTION(TDuration, WaitingJobTimeout);
};

///
/// @brief Class to provide information on intermediate mapreduce stream protobuf types.
///
/// When using protobuf format it is important to know exact types of proto messages
/// that are used in input/output.
///
/// Sometimes such messages cannot be derived from job class
/// i.e. when job class uses `NYT::TTableReader<::google::protobuf::Message>`
/// or `NYT::TTableWriter<::google::protobuf::Message>`.
///
/// When using such jobs user can provide exact message type using this class.
///
/// @note Only input/output that relate to intermediate tables can be hinted.
/// Input to map and output of reduce is derived from `AddInput`/`AddOutput`.
template <class TDerived>
struct TIntermediateTablesHintSpec
{
    /// Specify intermediate map output type.
    template <class T>
    TDerived& HintMapOutput();

    /// Specify reduce combiner input.
    template <class T>
    TDerived& HintReduceCombinerInput();

    /// Specify reduce combiner output.
    template <class T>
    TDerived& HintReduceCombinerOutput();

    /// Specify reducer input.
    template <class T>
    TDerived& HintReduceInput();

    ///
    /// @brief Add output of map stage.
    ///
    /// Mapper output table #0 is always intermediate table that is going to be reduced later.
    /// Rows that mapper write to tables #1, #2, ... are saved in MapOutput tables.
    template <class T>
    TDerived& AddMapOutput(const TRichYPath& path);

    TVector<TRichYPath> MapOutputs_;

    const TVector<TStructuredTablePath>& GetStructuredMapOutputs() const;
    const TMaybe<TTableStructure>& GetIntermediateMapOutputDescription() const;
    const TMaybe<TTableStructure>& GetIntermediateReduceCombinerInputDescription() const;
    const TMaybe<TTableStructure>& GetIntermediateReduceCombinerOutputDescription() const;
    const TMaybe<TTableStructure>& GetIntermediateReducerInputDescription() const;

private:
    TVector<TStructuredTablePath> StructuredMapOutputs_;
    TMaybe<TTableStructure> IntermediateMapOutputDescription_;
    TMaybe<TTableStructure> IntermediateReduceCombinerInputDescription_;
    TMaybe<TTableStructure> IntermediateReduceCombinerOutputDescription_;
    TMaybe<TTableStructure> IntermediateReducerInputDescription_;
};

////////////////////////////////////////////////////////////////////////////////

struct TAddLocalFileOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TAddLocalFileOptions;
    /// @endcond

    ///
    /// @brief Path by which job will see the uploaded file.
    ///
    /// Defaults to basename of the local path.
    FLUENT_FIELD_OPTION(TString, PathInJob);

    ///
    /// @brief MD5 checksum of uploaded file.
    ///
    /// If not specified it is computed by this library.
    /// If this argument is provided, the user can some cpu and disk IO.
    FLUENT_FIELD_OPTION(TString, MD5CheckSum);

    ///
    /// @brief Do not put file into node cache
    ///
    /// @see NYT::TRichYPath::BypassArtifactCache
    FLUENT_FIELD_OPTION(bool, BypassArtifactCache);
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Binary to run job profiler on.
enum class EProfilingBinary
{
    /// Profile job proxy.
    JobProxy       /* "job_proxy" */,

    /// Profile user job.
    UserJob        /* "user_job" */,
};

/// @brief Type of job profiler.
enum class EProfilerType
{
    /// Profile CPU usage.
    Cpu            /* "cpu" */,

    /// Profile memory usage.
    Memory         /* "memory" */,

    /// Profiler peak memory usage.
    PeakMemory     /* "peak_memory" */,
};

/// @brief Specifies a job profiler.
struct TJobProfilerSpec
{
    /// @cond Doxygen_Suppress
    using TSelf = TJobProfilerSpec;
    /// @endcond

    /// @brief Binary to profile.
    FLUENT_FIELD_OPTION(EProfilingBinary, ProfilingBinary);

    /// @brief Type of the profiler.
    FLUENT_FIELD_OPTION(EProfilerType, ProfilerType);

    /// @brief Probability of the job being selected for profiling.
    FLUENT_FIELD_OPTION(double, ProfilingProbability);

    /// @brief For sampling profilers, sets the number of samples per second.
    FLUENT_FIELD_OPTION(int, SamplingFrequency);
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Specification of a disk that will be available in job.
///
/// Disk request should be used in case job requires specific requirements for disk (i.e. it requires NVME or SSD).
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#disk_request
struct TDiskRequest
{
    /// @cond Doxygen_Suppress
    using TSelf = TDiskRequest;
    /// @endcond

    /// Required disk space in bytes.
    FLUENT_FIELD_OPTION(i64, DiskSpace);

    /// Limit for inodes.
    FLUENT_FIELD_OPTION(i64, InodeCount);

    /// Account which quota is going to be used.
    /// Account must have available quota for the specified medium.
    FLUENT_FIELD_OPTION(TString, Account);

    /// Name of the medium corresponding to required disk type.
    FLUENT_FIELD_OPTION(TString, MediumName);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Spec of user job.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#user_script_options
struct TUserJobSpec
{
    /// @cond Doxygen_Suppress
    using TSelf = TUserJobSpec;
    /// @endcond

    ///
    /// @brief Specify a local file to upload to Cypress and prepare for use in job.
    TSelf& AddLocalFile(const TLocalFilePath& path, const TAddLocalFileOptions& options = TAddLocalFileOptions());

    ///
    /// @brief Get the list of all added local files.
    TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> GetLocalFiles() const;

    /// @brief Paths to files in Cypress to use in job.
    FLUENT_VECTOR_FIELD(TRichYPath, File);

    /// @brief Porto layers to use in the job. Layers are listed from top to bottom.
    FLUENT_VECTOR_FIELD(TYPath, Layer);

    ///
    /// @brief MemoryLimit specifies how much memory job process can use.
    ///
    /// @note
    /// If job uses tmpfs (check @ref NYT::TOperationOptions::MountSandboxInTmpfs)
    /// YT computes its memory usage as total of:
    ///   - memory usage of job process itself (including mapped files);
    ///   - total size of tmpfs used by this job.
    ///
    /// @note
    /// When @ref NYT::TOperationOptions::MountSandboxInTmpfs is enabled library will compute
    /// total size of all files used by this job and add this total size to MemoryLimit.
    /// Thus you shouldn't include size of your files (e.g. binary file) into MemoryLimit.
    ///
    /// @note
    /// Final memory memory_limit passed to YT is calculated as follows:
    ///
    /// @note
    /// ```
    /// memory_limit = MemoryLimit + <total-size-of-used-files> + ExtraTmpfsSize
    /// ```
    ///
    /// @see NYT::TUserJobSpec::ExtraTmpfsSize
    FLUENT_FIELD_OPTION(i64, MemoryLimit);

    ///
    /// @brief Size of data that is going to be written to tmpfs.
    ///
    /// This option should be used if job writes data to tmpfs.
    ///
    /// ExtraTmpfsSize should not include size of files specified with
    /// @ref NYT::TUserJobSpec::AddLocalFile or @ref NYT::TUserJobSpec::AddFile
    /// These files are copied to tmpfs automatically and their total size
    /// is computed automatically.
    ///
    /// @see NYT::TOperationOptions::MountSandboxInTmpfs
    /// @see NYT::TUserJobSpec::MemoryLimit
    FLUENT_FIELD_OPTION(i64, ExtraTmpfsSize);

    ///
    /// @brief Maximum number of CPU cores for a single job to use.
    FLUENT_FIELD_OPTION(double, CpuLimit);

    ///
    /// @brief Fraction of @ref NYT::TUserJobSpec::MemoryLimit that job gets at start.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#memory_reserve_factor
    FLUENT_FIELD_OPTION(double, MemoryReserveFactor);

    ///
    /// @brief Local path to executable to be used inside jobs.
    ////
    /// Provided executable must use C++ YT API library (this library)
    /// and implement job class that is going to be used.
    ///
    /// This option might be useful if we want to start operation from nonlinux machines
    /// (in that case we use `JobBinary` to provide path to the same program compiled for linux).
    /// Other example of using this option is uploading executable to cypress in advance
    /// and save the time required to upload current executable to cache.
    /// `md5` argument can be used to save cpu time and disk IO when binary MD5 checksum is known.
    /// When argument is not provided library will compute it itself.
    TUserJobSpec& JobBinaryLocalPath(TString path, TMaybe<TString> md5 = Nothing());

    ///
    /// @brief Cypress path to executable to be used inside jobs.
    TUserJobSpec& JobBinaryCypressPath(TString path, TMaybe<TTransactionId> transactionId = Nothing());

    ///
    /// @brief String that will be prepended to the command.
    ///
    /// This option overrides @ref NYT::TOperationOptions::JobCommandPrefix.
    FLUENT_FIELD(TString, JobCommandPrefix);

    ///
    /// @brief String that will be appended to the command.
    ///
    /// This option overrides @ref NYT::TOperationOptions::JobCommandSuffix.
    FLUENT_FIELD(TString, JobCommandSuffix);

    ///
    /// @brief Map of environment variables that will be set for jobs.
    FLUENT_MAP_FIELD(TString, TString, Environment);

    ///
    /// @brief Limit for all files inside job sandbox (in bytes).
    FLUENT_FIELD_OPTION(ui64, DiskSpaceLimit);

    ///
    /// @brief Number of ports reserved for the job (passed through environment in YT_PORT_0, YT_PORT_1, ...).
    FLUENT_FIELD_OPTION(ui16, PortCount);

    ///
    /// @brief Network project used to isolate job network.
    FLUENT_FIELD_OPTION(TString, NetworkProject);

    ///
    /// @brief Limit on job execution time.
    ///
    /// Jobs that exceed this limit will be considered failed.
    FLUENT_FIELD_OPTION(TDuration, JobTimeLimit);

    ///
    /// @brief Get job binary config.
    const TJobBinaryConfig& GetJobBinary() const;

    ///
    /// @brief List of profilers to run.
    FLUENT_VECTOR_FIELD(TJobProfilerSpec, JobProfiler);

    ///
    /// @brief Specification of a disk required for job.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#disk_request
    FLUENT_FIELD_OPTION(TDiskRequest, DiskRequest);

private:
    TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> LocalFiles_;
    TJobBinaryConfig JobBinary_;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Spec of Map operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map
template <typename TDerived>
struct TMapOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
    , public TWithAutoMergeSpec<TDerived>
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Spec of mapper job.
    FLUENT_FIELD(TUserJobSpec, MapperSpec);

    ///
    /// @brief Whether to guarantee the order of rows passed to mapper matches the order in the table.
    ///
    /// When `Ordered' is false (by default), there is no guaranties about order of reading rows.
    /// In this case mapper might work slightly faster because row delivered from fast node can be processed YT waits
    /// response from slow nodes.
    /// When `Ordered' is true, rows will come in order in which they are stored in input tables.
    FLUENT_FIELD_OPTION(bool, Ordered);

    ///
    /// @brief Recommended number of jobs to run.
    ///
    /// `JobCount' has higher priority than @ref NYT::TMapOperationSpecBase::DataSizePerJob.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui32, JobCount);

    ///
    /// @brief Recommended of data size for each job.
    ///
    /// `DataSizePerJob` has lower priority that @ref NYT::TMapOperationSpecBase::JobCount.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

///
/// @brief Spec of Map operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map
struct TMapOperationSpec
    : public TMapOperationSpecBase<TMapOperationSpec>
    , public TOperationIOSpec<TMapOperationSpec>
    , public TUserJobFormatHintsBase<TMapOperationSpec>
{ };

///
/// @brief Spec of raw Map operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map
struct TRawMapOperationSpec
    : public TMapOperationSpecBase<TRawMapOperationSpec>
    , public TSimpleRawOperationIoSpec<TRawMapOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Spec of Reduce operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce
template <typename TDerived>
struct TReduceOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
    , public TWithAutoMergeSpec<TDerived>
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Spec of reduce job.
    FLUENT_FIELD(TUserJobSpec, ReducerSpec);

    ///
    /// @brief Columns to sort rows by (must include `ReduceBy` as prefix).
    FLUENT_FIELD(TSortColumns, SortBy);

    ///
    /// @brief Columns to group rows by.
    FLUENT_FIELD(TSortColumns, ReduceBy);

    ///
    /// @brief Columns to join foreign tables by (must be prefix of `ReduceBy`).
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce#foreign_tables
    FLUENT_FIELD_OPTION(TSortColumns, JoinBy);

    ///
    /// @brief Guarantee to feed all rows with same `ReduceBy` columns to a single job (`true` by default).
    FLUENT_FIELD_OPTION(bool, EnableKeyGuarantee);

    ///
    /// @brief Recommended number of jobs to run.
    ///
    /// `JobCount' has higher priority than @ref NYT::TReduceOperationSpecBase::DataSizePerJob.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui32, JobCount);

    ///
    /// @brief Recommended of data size for each job.
    ///
    /// `DataSizePerJob` has lower priority that @ref NYT::TReduceOperationSpecBase::JobCount.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

///
/// @brief Spec of Reduce operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce
struct TReduceOperationSpec
    : public TReduceOperationSpecBase<TReduceOperationSpec>
    , public TOperationIOSpec<TReduceOperationSpec>
    , public TUserJobFormatHintsBase<TReduceOperationSpec>
{ };

///
/// @brief Spec of raw Reduce operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce
struct TRawReduceOperationSpec
    : public TReduceOperationSpecBase<TRawReduceOperationSpec>
    , public TSimpleRawOperationIoSpec<TRawReduceOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Spec of JoinReduce operation.
///
/// @deprecated Instead the user should run a reduce operation
/// with @ref NYT::TReduceOperationSpec::EnableKeyGuarantee set to `false`.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce#foreign_tables
template <typename TDerived>
struct TJoinReduceOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Spec of reduce job.
    FLUENT_FIELD(TUserJobSpec, ReducerSpec);

    ///
    /// @brief Columns to join foreign tables by (must be prefix of `ReduceBy`).
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce#foreign_tables
    FLUENT_FIELD(TSortColumns, JoinBy);

    ///
    /// @brief Recommended number of jobs to run.
    ///
    /// `JobCount' has higher priority than @ref NYT::TJoinReduceOperationSpecBase::DataSizePerJob.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui32, JobCount);

    ///
    /// @brief Recommended of data size for each job.
    ///
    /// `DataSizePerJob` has lower priority that @ref NYT::TJoinReduceOperationSpecBase::JobCount.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

///
/// @brief Spec of JoinReduce operation.
///
/// @deprecated Instead the user should run a reduce operation
/// with @ref NYT::TReduceOperationSpec::EnableKeyGuarantee set to `false`.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce#foreign_tables
struct TJoinReduceOperationSpec
    : public TJoinReduceOperationSpecBase<TJoinReduceOperationSpec>
    , public TOperationIOSpec<TJoinReduceOperationSpec>
    , public TUserJobFormatHintsBase<TJoinReduceOperationSpec>
{ };

///
/// @brief Spec of raw JoinReduce operation.
///
/// @deprecated Instead the user should run a reduce operation
/// with @ref NYT::TReduceOperationSpec::EnableKeyGuarantee set to `false`.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce#foreign_tables
struct TRawJoinReduceOperationSpec
    : public TJoinReduceOperationSpecBase<TRawJoinReduceOperationSpec>
    , public TSimpleRawOperationIoSpec<TRawJoinReduceOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Spec of MapReduce operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
template <typename TDerived>
struct TMapReduceOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Spec of map job.
    FLUENT_FIELD(TUserJobSpec, MapperSpec);

    ///
    /// @brief Spec of reduce job.
    FLUENT_FIELD(TUserJobSpec, ReducerSpec);

    ///
    /// @brief Spec of reduce combiner.
    FLUENT_FIELD(TUserJobSpec, ReduceCombinerSpec);

    ///
    /// @brief Columns to sort rows by (must include `ReduceBy` as prefix).
    FLUENT_FIELD(TSortColumns, SortBy);

    ///
    /// @brief Columns to group rows by.
    FLUENT_FIELD(TSortColumns, ReduceBy);

    ///
    /// @brief Recommended number of map jobs to run.
    ///
    /// `JobCount' has higher priority than @ref NYT::TMapReduceOperationSpecBase::DataSizePerMapJob.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui32, MapJobCount);

    ///
    /// @brief Recommended of data size for each map job.
    ///
    /// `DataSizePerJob` has lower priority that @ref NYT::TMapReduceOperationSpecBase::MapJobCount.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, DataSizePerMapJob);

    ///
    /// @brief Recommended number of intermediate data partitions.
    FLUENT_FIELD_OPTION(ui64, PartitionCount);

    ///
    /// @brief Recommended size of intermediate data partitions.
    FLUENT_FIELD_OPTION(ui64, PartitionDataSize);

    ///
    /// @brief Account to use for intermediate data.
    FLUENT_FIELD_OPTION(TString, IntermediateDataAccount);

    ///
    /// @brief Replication factor for intermediate data (1 by default).
    FLUENT_FIELD_OPTION(ui64,  IntermediateDataReplicationFactor);

    ///
    /// @brief Recommended size of data to be passed to a single reduce combiner.
    FLUENT_FIELD_OPTION(ui64, DataSizePerSortJob);

    ///
    /// @brief Whether to guarantee the order of rows passed to mapper matches the order in the table.
    ///
    /// @see @ref NYT::TMapOperationSpec::Ordered for more info.
    FLUENT_FIELD_OPTION(bool, Ordered);

    ///
    /// @brief Guarantee to run reduce combiner before reducer.
    FLUENT_FIELD_OPTION(bool, ForceReduceCombiners);
};

///
/// @brief Spec of MapReduce operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
struct TMapReduceOperationSpec
    : public TMapReduceOperationSpecBase<TMapReduceOperationSpec>
    , public TOperationIOSpec<TMapReduceOperationSpec>
    , public TIntermediateTablesHintSpec<TMapReduceOperationSpec>
{
    /// @cond Doxygen_Suppress
    using TSelf = TMapReduceOperationSpec;
    /// @endcond

    ///
    /// @brief Format hints for mapper.
    FLUENT_FIELD_DEFAULT(TUserJobFormatHints, MapperFormatHints, TUserJobFormatHints());

    ///
    /// @brief Format hints for reducer.
    FLUENT_FIELD_DEFAULT(TUserJobFormatHints, ReducerFormatHints, TUserJobFormatHints());

    ///
    /// @brief Format hints for reduce combiner.
    FLUENT_FIELD_DEFAULT(TUserJobFormatHints, ReduceCombinerFormatHints, TUserJobFormatHints());
};

///
/// @brief Spec of raw MapReduce operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
struct TRawMapReduceOperationSpec
    : public TMapReduceOperationSpecBase<TRawMapReduceOperationSpec>
    , public TRawMapReduceOperationIoSpec<TRawMapReduceOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Schema inference mode.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema.html#schema_inference
enum class ESchemaInferenceMode : int
{
    FromInput   /* "from_input" */,
    FromOutput  /* "from_output" */,
    Auto        /* "auto" */,
};

///
/// @brief Spec of Sort operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/sort
struct TSortOperationSpec
    : TOperationSpecBase<TSortOperationSpec>
{
    /// @cond Doxygen_Suppress
    using TSelf = TSortOperationSpec;
    /// @endcond

    ///
    /// @brief Paths to input tables.
    FLUENT_VECTOR_FIELD(TRichYPath, Input);

    ///
    /// @brief Path to output table.
    FLUENT_FIELD(TRichYPath, Output);

    ///
    /// @brief Columns to sort table by.
    FLUENT_FIELD(TSortColumns, SortBy);

    ///
    /// @brief Recommended number of intermediate data partitions.
    FLUENT_FIELD_OPTION(ui64, PartitionCount);

    ///
    /// @brief Recommended size of intermediate data partitions.
    FLUENT_FIELD_OPTION(ui64, PartitionDataSize);

    ///
    /// @brief Recommended number of partition jobs to run.
    ///
    /// `JobCount' has higher priority than @ref NYT::TSortOperationSpec::DataSizePerPartitionJob.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, PartitionJobCount);

    ///
    /// @brief Recommended of data size for each partition job.
    ///
    /// `DataSizePerJob` has lower priority that @ref NYT::TSortOperationSpec::PartitionJobCount.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, DataSizePerPartitionJob);

    ///
    /// @brief Inference mode for output table schema.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema.html#schema_inference
    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);

    ///
    /// @brief Account to use for intermediate data.
    FLUENT_FIELD_OPTION(TString, IntermediateDataAccount);

    ///
    /// @brief Replication factor for intermediate data (1 by default).
    FLUENT_FIELD_OPTION(ui64, IntermediateDataReplicationFactor);
};


///
/// @brief Merge mode.
enum EMergeMode : int
{
    MM_UNORDERED    /* "unordered" */,
    MM_ORDERED      /* "ordered" */,
    MM_SORTED       /* "sorted" */,
};

///
/// @brief Spec of Merge operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/merge
struct TMergeOperationSpec
    : TOperationSpecBase<TMergeOperationSpec>
{
    /// @cond Doxygen_Suppress
    using TSelf = TMergeOperationSpec;
    /// @endcond

    ///
    /// @brief Paths to input tables.
    FLUENT_VECTOR_FIELD(TRichYPath, Input);

    ///
    /// @brief Path to output table.
    FLUENT_FIELD(TRichYPath, Output);

    ///
    /// @brief Columns by which to merge (for @ref NYT::EMergeMode::MM_SORTED).
    FLUENT_FIELD(TSortColumns, MergeBy);

    ///
    /// @brief Merge mode.
    FLUENT_FIELD_DEFAULT(EMergeMode, Mode, MM_UNORDERED);

    ///
    /// @brief Combine output chunks to larger ones.
    FLUENT_FIELD_DEFAULT(bool, CombineChunks, false);

    ///
    /// @brief Guarantee that all input chunks will be read.
    FLUENT_FIELD_DEFAULT(bool, ForceTransform, false);

    ///
    /// @brief Recommended number of jobs to run.
    ///
    /// `JobCount' has higher priority than @ref NYT::TMergeOperationSpec::DataSizePerJob.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui32, JobCount);

    ///
    /// @brief Recommended of data size for each job.
    ///
    /// `DataSizePerJob` has lower priority that @ref NYT::TMergeOperationSpec::JobCount.
    /// This option only provide a recommendation and may be ignored if conflicting with YT internal limits.
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);

    ///
    /// @brief Inference mode for output table schema.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema.html#schema_inference
    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);
};

///
/// @brief Spec of Erase operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/erase
struct TEraseOperationSpec
    : TOperationSpecBase<TEraseOperationSpec>
{
    /// @cond Doxygen_Suppress
    using TSelf = TEraseOperationSpec;
    /// @endcond

    ///
    /// @brief Which table (or row range) to erase.
    FLUENT_FIELD(TRichYPath, TablePath);

    ///
    /// Combine output chunks to larger ones.
    FLUENT_FIELD_DEFAULT(bool, CombineChunks, false);

    ///
    /// @brief Inference mode for output table schema.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema.html#schema_inference
    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);
};

///
/// @brief Spec of RemoteCopy operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/remote_copy
struct TRemoteCopyOperationSpec
    : TOperationSpecBase<TRemoteCopyOperationSpec>
{
    /// @cond Doxygen_Suppress
    using TSelf = TRemoteCopyOperationSpec;
    /// @endcond

    ///
    /// @brief Source cluster name.
    FLUENT_FIELD(TString, ClusterName);

    ///
    /// @brief Network to use for copy (all remote cluster nodes must have it configured).
    FLUENT_FIELD_OPTION(TString, NetworkName);

    ///
    /// @brief Paths to input tables.
    FLUENT_VECTOR_FIELD(TRichYPath, Input);

    ///
    /// @brief Path to output table.
    FLUENT_FIELD(TRichYPath, Output);

    ///
    /// @brief Inference mode for output table schema.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema.html#schema_inference
    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);

    ///
    /// @brief Copy user attributes from input to output table (allowed only for single input table).
    FLUENT_FIELD_DEFAULT(bool, CopyAttributes, false);

    ///
    /// @brief Names of user attributes to copy from input to output table.
    ///
    /// @note To make this option make sense set @ref NYT::TRemoteCopyOperationSpec::CopyAttributes to `true`.
    FLUENT_VECTOR_FIELD(TString, AttributeKey);

private:

    ///
    /// @brief Config for remote cluster connection.
    FLUENT_FIELD_OPTION(TNode, ClusterConnection);
};

class IVanillaJobBase;

///
/// @brief Task of Vanilla operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla
struct TVanillaTask
    : public TOperationOutputSpecBase
    , public TUserJobOutputFormatHintsBase<TVanillaTask>
{
    /// @cond Doxygen_Suppress
    using TSelf = TVanillaTask;
    /// @endcond

    ///
    /// @brief Add output table path and specify the task output type (i.e. TMyProtoMessage).
    template <class T>
    TSelf& AddOutput(const TRichYPath& path);

    ///
    /// @brief Add output table path as structured path.
    TSelf& AddStructuredOutput(TStructuredTablePath path);

    ///
    /// @brief Set output table path and specify the task output type (i.e. TMyProtoMessage).
    template <class T>
    TSelf& SetOutput(size_t tableIndex, const TRichYPath& path);

    ///
    /// @brief Task name.
    FLUENT_FIELD(TString, Name);

    ///
    /// @brief Job to be executed in this task.
    FLUENT_FIELD(::TIntrusivePtr<IVanillaJobBase>, Job);

    ///
    /// @brief User job spec.
    FLUENT_FIELD(TUserJobSpec, Spec);

    ///
    /// @brief Number of jobs to run and wait for successful completion.
    ///
    /// @note If @ref NYT::TUserOperationSpecBase::FailOnJobRestart is `false`, a failed job will be restarted
    /// and will not count in this amount.
    FLUENT_FIELD(ui64, JobCount);

    ///
    /// @brief Network project name.
    FLUENT_FIELD(TMaybe<TString>, NetworkProject);

};

///
/// @brief Spec of Vanilla operation.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla
struct TVanillaOperationSpec
    : TUserOperationSpecBase<TVanillaOperationSpec>
{
    /// @cond Doxygen_Suppress
    using TSelf = TVanillaOperationSpec;
    /// @endcond

    ///
    /// @brief Description of tasks to run in this operation.
    FLUENT_VECTOR_FIELD(TVanillaTask, Task);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Options for @ref NYT::IOperationClient::Map and other operation start commands.
struct TOperationOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TOperationOptions;
    /// @endcond

    ///
    /// @brief Additional field to put to operation spec.
    FLUENT_FIELD_OPTION(TNode, Spec);

    ///
    /// @brief Start operation mode.
    enum class EStartOperationMode : int
    {
        ///
        /// @brief Prepare operation asynchronously. Call IOperation::Start() to start operation.
        AsyncPrepare,

        ///
        /// @brief Prepare and start operation asynchronously. Don't wait for operation completion.
        AsyncStart,

        ///
        /// @brief Prepare and start operation synchronously. Don't wait for operation completion.
        SyncStart,

        ///
        /// @brief Prepare, start and wait for operation completion synchronously.
        SyncWait,
    };

    ///
    /// @brief Start operation mode.
    FLUENT_FIELD_DEFAULT(EStartOperationMode, StartOperationMode, EStartOperationMode::SyncWait);

    ///
    /// @brief Wait for operation finish synchronously.
    ///
    /// @deprecated Use StartOperationMode() instead.
    TSelf& Wait(bool value) {
        StartOperationMode_ = value ? EStartOperationMode::SyncWait : EStartOperationMode::SyncStart;
        return static_cast<TSelf&>(*this);
    }

    ///
    ///
    /// @brief Use format from table attribute (for YAMR-like format).
    ///
    /// @deprecated
    FLUENT_FIELD_DEFAULT(bool, UseTableFormats, false);

    ///
    /// @brief Prefix for bash command running the jobs.
    ///
    /// Can be overridden for the specific job type in the @ref NYT::TUserJobSpec.
    FLUENT_FIELD(TString, JobCommandPrefix);

    ///
    /// @brief Suffix for bash command running the jobs.
    ///
    /// Can be overridden for the specific job type in the @ref NYT::TUserJobSpec.
    FLUENT_FIELD(TString, JobCommandSuffix);

    ///
    /// @brief Put all files required by the job into tmpfs.
    ///
    /// This option can be set globally using @ref NYT::TConfig::MountSandboxInTmpfs.
    /// @see https://ytsaurus.tech/docs/en/problems/woodpeckers
    FLUENT_FIELD_DEFAULT(bool, MountSandboxInTmpfs, false);

    ///
    /// @brief Path to directory to store temporary files.
    FLUENT_FIELD_OPTION(TString, FileStorage);

    ///
    /// @brief Expiration timeout for uploaded files.
    FLUENT_FIELD_OPTION(TDuration, FileExpirationTimeout);

    ///
    /// @brief Info to be passed securely to the job.
    FLUENT_FIELD_OPTION(TNode, SecureVault);

    ///
    /// @brief File cache mode.
    enum class EFileCacheMode : int
    {
        ///
        /// @brief Use YT API commands "get_file_from_cache" and "put_file_to_cache".
        ApiCommandBased,

        ///
        /// @brief Upload files to random paths inside @ref NYT::TOperationOptions::FileStorage without caching.
        CachelessRandomPathUpload,
    };

    ///
    /// @brief File cache mode.
    FLUENT_FIELD_DEFAULT(EFileCacheMode, FileCacheMode, EFileCacheMode::ApiCommandBased);

    ///
    /// @brief Id of transaction within which all Cypress file storage entries will be checked/created.
    ///
    /// By default, the root transaction is used.
    ///
    /// @note Set a specific transaction only if you
    ///  1. specify non-default file storage path in @ref NYT::TOperationOptions::FileStorage or in @ref NYT::TConfig::RemoteTempFilesDirectory.
    ///  2. use `CachelessRandomPathUpload` caching mode (@ref NYT::TOperationOptions::FileCacheMode).
    FLUENT_FIELD(TTransactionId, FileStorageTransactionId);

    ///
    /// @brief Ensure stderr and core tables exist before starting operation.
    ///
    /// If set to `false`, it is user's responsibility to ensure these tables exist.
    FLUENT_FIELD_DEFAULT(bool, CreateDebugOutputTables, true);

    ///
    /// @brief Ensure output tables exist before starting operation.
    ///
    /// If set to `false`, it is user's responsibility to ensure output tables exist.
    FLUENT_FIELD_DEFAULT(bool, CreateOutputTables, true);

    ///
    /// @brief Try to infer schema of inexistent table from the type of written rows.
    ///
    /// @note Default values for this option may differ depending on the row type.
    /// For protobuf it's currently `false` by default.
    FLUENT_FIELD_OPTION(bool, InferOutputSchema);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Get operation secure vault (specified in @ref NYT::TOperationOptions::SecureVault) inside a job.
const TNode& GetJobSecureVault();

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Context passed to @ref NYT::IRawJob::Do.
class TRawJobContext
{
public:
    explicit TRawJobContext(size_t outputTableCount);

    ///
    /// @brief Get file corresponding to input stream.
    const TFile& GetInputFile() const;

    ///
    /// @brief Get files corresponding to output streams.
    const TVector<TFile>& GetOutputFileList() const;

private:
    TFile InputFile_;
    TVector<TFile> OutputFileList_;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Interface for classes that can be Saved/Loaded (to be used with @ref Y_SAVELOAD_JOB).
class ISerializableForJob
{
public:
    virtual ~ISerializableForJob() = default;

    ///
    /// @brief Dump state to output stream to be restored in job.
    virtual void Save(IOutputStream& stream) const = 0;

    ///
    /// @brief Load state from a stream.
    virtual void Load(IInputStream& stream) = 0;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Provider of information about operation inputs/outputs during @ref NYT::IJob::PrepareOperation.
class IOperationPreparationContext
{
public:
    virtual ~IOperationPreparationContext() = default;

    /// @brief Get the number of input tables.
    virtual int GetInputCount() const = 0;

    /// @brief Get the number of output tables.
    virtual int GetOutputCount() const = 0;

    /// @brief Get the schema of input table no. `index`.
    virtual const TTableSchema& GetInputSchema(int index) const = 0;

    /// @brief Get all the input table schemas.
    virtual const TVector<TTableSchema>& GetInputSchemas() const = 0;

    /// @brief Path to the input table if available (`Nothing()` for intermediate tables).
    virtual TMaybe<TYPath> GetInputPath(int index) const = 0;

    /// @brief Path to the output table if available (`Nothing()` for intermediate tables).
    virtual TMaybe<TYPath> GetOutputPath(int index) const = 0;
};

///
/// @brief Fluent builder class for @ref NYT::IJob::PrepareOperation.
///
/// @note Method calls are supposed to be chained.
class TJobOperationPreparer
{
public:

    ///
    /// @brief Group of input tables that allows to specify properties on all of them at once.
    ///
    /// The instances are created with @ref NYT::TJobOperationPreparer::BeginInputGroup, not directly.
    class TInputGroup
    {
    public:
        TInputGroup(TJobOperationPreparer& preparer, TVector<int> indices);

        /// @brief Specify the type of input rows.
        template <typename TRow>
        TInputGroup& Description();

        /// @brief Specify renaming of input columns.
        TInputGroup& ColumnRenaming(const THashMap<TString, TString>& renaming);

        /// @brief Specify what input columns to send to job
        ///
        /// @note Filter is applied before renaming, so it must specify original column names.
        TInputGroup& ColumnFilter(const TVector<TString>& columns);

        /// @brief Finish describing the input group.
        TJobOperationPreparer& EndInputGroup();

    private:
        TJobOperationPreparer& Preparer_;
        TVector<int> Indices_;
    };

    ///
    /// @brief Group of output tables that allows to specify properties on all of them at once.
    ///
    /// The instances are created with @ref NYT::TJobOperationPreparer::BeginOutputGroup, not directly.
    class TOutputGroup
    {
    public:
        TOutputGroup(TJobOperationPreparer& preparer, TVector<int> indices);

        /// @brief Specify the type of output rows.
        ///
        /// @tparam TRow type of output rows from tables of this group.
        /// @param inferSchema Infer schema from `TRow` and specify it for these output tables.
        template <typename TRow>
        TOutputGroup& Description(bool inferSchema = true);

        /// @brief Specify schema for these tables.
        TOutputGroup& Schema(const TTableSchema& schema);

        /// @brief Specify that all the the tables in this group are unschematized.
        ///
        /// It is equivalent of `.Schema(TTableSchema().Strict(false)`.
        TOutputGroup& NoSchema();

        /// @brief Finish describing the output group.
        TJobOperationPreparer& EndOutputGroup();

    private:
        TJobOperationPreparer& Preparer_;
        TVector<int> Indices_;
    };

public:
    explicit TJobOperationPreparer(const IOperationPreparationContext& context);

    /// @brief Begin input group consisting of tables with indices `[begin, end)`.
    ///
    /// @param begin First index.
    /// @param end Index after the last one.
    TInputGroup BeginInputGroup(int begin, int end);

    /// @brief Begin input group consisting of tables with indices from `indices`.
    ///
    /// @tparam TCont Container with integers. Must support `std::begin` and `std::end` functions.
    /// @param indices Indices of tables to include in the group.
    template <typename TCont>
    TInputGroup BeginInputGroup(const TCont& indices);

    /// @brief Begin output group consisting of tables with indices `[begin, end)`.
    ///
    /// @param begin First index.
    /// @param end Index after the last one.
    TOutputGroup BeginOutputGroup(int begin, int end);

    /// @brief Begin input group consisting of tables with indices from `indices`.
    ///
    /// @tparam TCont Container with integers. Must support `std::begin` and `std::end` functions.
    /// @param indices Indices of tables to include in the group.
    template <typename TCont>
    TOutputGroup BeginOutputGroup(const TCont& indices);

    /// @brief Specify the schema for output table no `tableIndex`.
    ///
    /// @note All the output schemas must be specified either with this method, `NoOutputSchema` or `OutputDescription` with `inferSchema == true`
    TJobOperationPreparer& OutputSchema(int tableIndex, TTableSchema schema);

    /// @brief Mark the output table no. `tableIndex` as unschematized.
    TJobOperationPreparer& NoOutputSchema(int tableIndex);

    /// @brief Specify renaming of input columns for table no. `tableIndex`.
    TJobOperationPreparer& InputColumnRenaming(int tableIndex, const THashMap<TString, TString>& renaming);

    /// @brief Specify what input columns of table no. `tableIndex` to send to job
    ///
    /// @note Filter is applied before renaming, so it must specify original column names.
    TJobOperationPreparer& InputColumnFilter(int tableIndex, const TVector<TString>& columns);

    /// @brief Specify the type of input rows for table no. `tableIndex`.
    ///
    /// @tparam TRow type of input rows.
    template <typename TRow>
    TJobOperationPreparer& InputDescription(int tableIndex);

    /// @brief Specify the type of output rows for table no. `tableIndex`.
    ///
    /// @tparam TRow type of output rows.
    /// @param inferSchema Infer schema from `TRow` and specify it for the output tables.
    template <typename TRow>
    TJobOperationPreparer& OutputDescription(int tableIndex, bool inferSchema = true);

    /// @brief Set type of output rows for table no. `tableIndex` to TNode
    ///
    /// @note Set schema via `OutputSchema` if needed
    TJobOperationPreparer& NodeOutput(int tableIndex);

    /// @brief Specify input format hints.
    ///
    /// These hints have lower priority than ones specified in spec.
    TJobOperationPreparer& InputFormatHints(TFormatHints hints);

    /// @brief Specify output format hints.
    ///
    /// These hints have lower priority than ones specified in spec.
    TJobOperationPreparer& OutputFormatHints(TFormatHints hints);

    /// @brief Specify format hints.
    ///
    /// These hints have lower priority than ones specified in spec.
    TJobOperationPreparer& FormatHints(TUserJobFormatHints newFormatHints);

    /// @name "Private" members
    /// The following methods should not be used by clients in @ref NYT::IJob::PrepareOperation
    ///@{

    /// @brief Finish the building process.
    void Finish();

    /// @brief Get output table schemas as specified by the user.
    TVector<TTableSchema> GetOutputSchemas();

    /// @brief Get input column renamings as specified by the user.
    const TVector<THashMap<TString, TString>>& GetInputColumnRenamings() const;

    /// @brief Get input column filters as specified by the user.
    const TVector<TMaybe<TVector<TString>>>& GetInputColumnFilters() const;

    /// @brief Get input column descriptions as specified by the user.
    const TVector<TMaybe<TTableStructure>>& GetInputDescriptions() const;

    /// @brief Get output column descriptions as specified by the user.
    const TVector<TMaybe<TTableStructure>>& GetOutputDescriptions() const;

    /// @brief Get format hints as specified by the user.
    const TUserJobFormatHints& GetFormatHints() const;

    ///@}
private:

    /// @brief Validate that schema for output table no. `tableIndex` has not been set yet.
    void ValidateMissingOutputSchema(int tableIndex) const;

    /// @brief Validate that description for input table no. `tableIndex` has not been set yet.
    void ValidateMissingInputDescription(int tableIndex) const;

    /// @brief Validate that description for output table no. `tableIndex` has not been set yet.
    void ValidateMissingOutputDescription(int tableIndex) const;

    /// @brief Validate that `tableIndex` is in correct range for input table indices.
    ///
    /// @param message Message to add to the exception in case of violation.
    void ValidateInputTableIndex(int tableIndex, TStringBuf message) const;

    /// @brief Validate that `tableIndex` is in correct range for output table indices.
    ///
    /// @param message Message to add to the exception in case of violation.
    void ValidateOutputTableIndex(int tableIndex, TStringBuf message) const;

    /// @brief Validate that all the output schemas has been set.
    void FinallyValidate() const;

    static TTableSchema EmptyNonstrictSchema();

private:
    const IOperationPreparationContext& Context_;

    TVector<TMaybe<TTableSchema>> OutputSchemas_;
    TVector<THashMap<TString, TString>> InputColumnRenamings_;
    TVector<TMaybe<TVector<TString>>> InputColumnFilters_;
    TVector<TMaybe<TTableStructure>> InputTableDescriptions_;
    TVector<TMaybe<TTableStructure>> OutputTableDescriptions_;
    TUserJobFormatHints FormatHints_ = {};
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Interface for all user jobs.
class IJob
    : public TThrRefBase
{
public:

    ///
    /// @brief Type of job.
    enum EType
    {
        Mapper,
        Reducer,
        ReducerAggregator,
        RawJob,
        VanillaJob,
    };

    ///
    /// @brief Save job state to stream to be restored on cluster nodes.
    virtual void Save(IOutputStream& stream) const
    {
        Y_UNUSED(stream);
    }

    ///
    /// @brief Restore job state from a stream.
    virtual void Load(IInputStream& stream)
    {
        Y_UNUSED(stream);
    }

    ///
    /// @brief Get operation secure vault (specified in @ref NYT::TOperationOptions::SecureVault) inside a job.
    const TNode& SecureVault() const
    {
        return GetJobSecureVault();
    }

    ///
    /// @brief Get number of output tables.
    i64 GetOutputTableCount() const
    {
        Y_ABORT_UNLESS(NDetail::OutputTableCount > 0);

        return NDetail::OutputTableCount;
    }

    ///
    /// @brief Method allowing user to control some properties of input and output tables and formats.
    ///
    /// User can override this method in their job class to:
    ///   - specify output table schemas.
    ///     The most natural way is usually through @ref NYT::TJobOperationPreparer::OutputDescription (especially for protobuf),
    ///     but you can use @ref NYT::TJobOperationPreparer::OutputSchema directly
    ///   - specify output row type (@ref NYT::TJobOperationPreparer::OutputDescription)
    ///   - specify input row type (@ref NYT::TJobOperationPreparer::InputDescription)
    ///   - specify input column filter and renaming (@ref NYT::TJobOperationPreparer::InputColumnFilter and @ref NYT::TJobOperationPreparer::InputColumnRenaming)
    ///   - specify format hints (@ref NYT::TJobOperationPreparer::InputFormatHints,
    ///     NYT::TJobOperationPreparer::OutputFormatHints and @ref NYT::TJobOperationPreparer::FormatHints)
    ///   - maybe something more, cf. the methods of @ref NYT::TJobOperationPreparer.
    ///
    /// If one has several similar tables, groups can be used.
    /// Groups are delimited by @ref NYT::TJobOperationPreparer::BeginInputGroup /
    /// @ref NYT::TJobOperationPreparer::TInputGroup::EndInputGroup and
    /// @ref NYT::TJobOperationPreparer::BeginOutputGroup /
    /// @ref NYT::TJobOperationPreparer::TOutputGroup::EndOutputGroup.
    /// Example:
    /// @code{.cpp}
    ///   preparer
    ///     .BeginInputGroup({1,2,4,8})
    ///       .ColumnRenaming({{"a", "b"}, {"c", "d"}})
    ///       .ColumnFilter({"a", "c"})
    ///     .EndInputGroup();
    /// @endcode
    ///
    /// @note All the output table schemas must be set
    /// (possibly as empty nonstrict using @ref NYT::TJobOperationPreparer::NoOutputSchema or
    /// @ref NYT::TJobOperationPreparer::TOutputGroup::NoSchema).
    /// By default all the output table schemas are marked as empty nonstrict.
    virtual void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const;
};

///
/// @brief Declare what fields of currently declared job class to save and restore on cluster node.
#define Y_SAVELOAD_JOB(...) \
    virtual void Save(IOutputStream& stream) const override { Save(&stream); } \
    virtual void Load(IInputStream& stream) override { Load(&stream); } \
    Y_PASS_VA_ARGS(Y_SAVELOAD_DEFINE(__VA_ARGS__))

///
/// @brief Same as the macro above, but also calls Base class's SaveLoad methods.
#define Y_SAVELOAD_JOB_DERIVED(Base, ...) \
    virtual void Save(IOutputStream& stream) const override { \
        Base::Save(stream); \
        Save(&stream); \
    } \
    virtual void Load(IInputStream& stream) override { \
        Base::Load(stream); \
        Load(&stream); \
    } \
    Y_PASS_VA_ARGS(Y_SAVELOAD_DEFINE(__VA_ARGS__))

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Interface for jobs with typed inputs and outputs.
class IStructuredJob
    : public IJob
{
public:
    ///
    /// @brief This methods are called when creating table reader and writer for the job.
    ///
    /// Override them if you want to implement custom input logic. (e.g. additional bufferization)
    virtual TRawTableReaderPtr CreateCustomRawJobReader(int fd) const;
    virtual THolder<IProxyOutput> CreateCustomRawJobWriter(size_t outputTableCount) const;

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const = 0;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Create default raw job reader.
TRawTableReaderPtr CreateRawJobReader(int fd = 0);

///
/// @brief Create default raw job writer.
THolder<IProxyOutput> CreateRawJobWriter(size_t outputTableCount);

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Base interface for structured (typed) map jobs.
class IMapperBase
    : public IStructuredJob
{ };

///
/// @brief Base interface for structured (typed) map jobs with given reader and writer.
template <class TR, class TW>
class IMapper
    : public IMapperBase
{
public:
    using TReader = TR;
    using TWriter = TW;

public:
    /// Type of job implemented by this class.
    static constexpr EType JobType = EType::Mapper;

    ///
    /// @brief This method is called before feeding input rows to mapper (before `Do` method).
    virtual void Start(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    ///
    /// @brief This method is called exactly once for the whole job input.
    ///
    /// Read input rows from `reader` and write output ones to `writer`.
    virtual void Do(TReader* reader, TWriter* writer) = 0;

    ///
    /// @brief This method is called after feeding input rows to mapper (after `Do` method).
    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Base interface for structured (typed) reduce jobs.
///
/// It is common base for @ref NYT::IReducer and @ref NYT::IAggregatorReducer.
class IReducerBase
    : public IStructuredJob
{ };

///
/// @brief Base interface for structured (typed) reduce jobs with given reader and writer.
template <class TR, class TW>
class IReducer
    : public IReducerBase
{
public:
    using TReader = TR;
    using TWriter = TW;

public:
    /// Type of job implemented by this class.
    static constexpr EType JobType = EType::Reducer;

public:

    ///
    /// @brief This method is called before feeding input rows to reducer (before `Do` method).
    virtual void Start(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    ///
    /// @brief This method is called exactly once for each range with same value of `ReduceBy` (or `JoinBy`) keys.
    virtual void Do(TReader* reader, TWriter* writer) = 0;

    ///
    /// @brief This method is called after feeding input rows to reducer (after `Do` method).
    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    ///
    /// @brief Refuse to process the remaining row ranges and finish the job (successfully).
    void Break();

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Base interface of jobs used inside reduce operations.
///
/// Unlike @ref NYT::IReducer jobs their `Do' method is called only once
/// and takes whole range of records split by key boundaries.
///
/// Template argument `TR` must be @ref NYT::TTableRangesReader.
template <class TR, class TW>
class IAggregatorReducer
    : public IReducerBase
{
public:
    using TReader = TR;
    using TWriter = TW;

public:
    /// Type of job implemented by this class.
    static constexpr EType JobType = EType::ReducerAggregator;

public:
    ///
    /// @brief This method is called before feeding input rows to reducer (before `Do` method).
    virtual void Start(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    ///
    /// @brief This method is called exactly once for the whole job input.
    virtual void Do(TReader* reader, TWriter* writer) = 0;

    ///
    /// @brief This method is called after feeding input rows to reducer (after `Do` method).
    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Interface for raw jobs (i.e. reading and writing byte streams).
class IRawJob
    : public IJob
{
public:
    /// Type of job implemented by this class.
    static constexpr EType JobType = EType::RawJob;

    ///
    /// @brief This method is called exactly once for the whole job input.
    virtual void Do(const TRawJobContext& jobContext) = 0;
};

///
/// @brief Interface of jobs that run the given bash command.
class ICommandJob
    : public IJob
{
public:
    ///
    /// @brief Get bash command to run.
    ///
    /// @note This method is called on the client side.
    virtual const TString& GetCommand() const = 0;
};

///
/// @brief Raw job executing given bash command.
///
/// @note The binary will not be uploaded.
class TCommandRawJob
    : public IRawJob
    , public ICommandJob
{
public:
    ///
    /// @brief Create job with specified command.
    ///
    /// @param command Bash command to run.
    explicit TCommandRawJob(TStringBuf command = {});

    const TString& GetCommand() const override;
    void Do(const TRawJobContext& jobContext) override;

private:
    TString Command_;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Base interface for vanilla jobs.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla
class IVanillaJobBase
    : public virtual IStructuredJob
{
public:
    /// Type of job implemented by this class.
    static constexpr EType JobType = EType::VanillaJob;
};

template <class TW = void>
class IVanillaJob;

///
/// @brief Interface of vanilla job without outputs.
template <>
class IVanillaJob<void>
    : public IVanillaJobBase
{
public:
    ///
    /// @brief This method is called exactly once for each vanilla job.
    virtual void Do() = 0;

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

///
/// @brief Vanilla job executing given bash command.
///
/// @note The binary will not be uploaded.
class TCommandVanillaJob
    : public IVanillaJob<>
    , public ICommandJob
{
public:
    ///
    /// @brief Create job with specified command.
    ///
    /// @param command Bash command to run.
    explicit TCommandVanillaJob(TStringBuf command = {});

    const TString& GetCommand() const override;
    void Do() override;

private:
    TString Command_;
};

///
/// @brief Interface for vanilla jobs with output tables.
template <class TW>
class IVanillaJob
    : public IVanillaJobBase
{
public:
    using TWriter = TW;

public:
    ///
    /// @brief This method is called before `Do` method.
    virtual void Start(TWriter* /* writer */)
    { }

    ///
    /// @brief This method is called exactly once for each vanilla job.
    ///
    /// Write output rows to `writer`.
    virtual void Do(TWriter* writer) = 0;

    ///
    /// @brief This method is called after `Do` method.
    virtual void Finish(TWriter* /* writer */)
    { }

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Attributes to request for an operation.
enum class EOperationAttribute : int
{
    Id                /* "id" */,
    Type              /* "type" */,
    State             /* "state" */,
    AuthenticatedUser /* "authenticated_user" */,
    StartTime         /* "start_time" */,
    FinishTime        /* "finish_time" */,
    BriefProgress     /* "brief_progress" */,
    BriefSpec         /* "brief_spec" */,
    Suspended         /* "suspended" */,
    Result            /* "result" */,
    Progress          /* "progress" */,
    Events            /* "events" */,
    Spec              /* "spec" */,
    FullSpec          /* "full_spec" */,
    UnrecognizedSpec  /* "unrecognized_spec" */,
};

///
/// @brief Class describing which attributes to request in @ref NYT::IClient::GetOperation or @ref NYT::IClient::ListOperations.
struct TOperationAttributeFilter
{
    /// @cond Doxygen_Suppress
    using TSelf = TOperationAttributeFilter;
    /// @endcond

    TVector<EOperationAttribute> Attributes_;

    ///
    /// @brief Add attribute to the filter. Calls are supposed to be chained.
    TSelf& Add(EOperationAttribute attribute)
    {
        Attributes_.push_back(attribute);
        return *this;
    }
};

///
/// @brief Options for @ref NYT::IClient::GetOperation call.
struct TGetOperationOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetOperationOptions;
    /// @endcond

    ///
    /// @brief What attributes to request (if omitted, the default set of attributes will be requested).
    FLUENT_FIELD_OPTION(TOperationAttributeFilter, AttributeFilter);

    FLUENT_FIELD_OPTION(bool, IncludeRuntime);
};

///
/// @brief "Coarse-grained" state of an operation.
enum class EOperationBriefState : int
{
    InProgress    /* "in_progress" */,
    Completed     /* "completed" */,
    Aborted       /* "aborted" */,

    /// Failed
    Failed        /* "failed" */,
};

///
/// @brief Operation type.
enum class EOperationType : int
{
    Map         /* "map" */,
    Merge       /* "merge" */,
    Erase       /* "erase" */,
    Sort        /* "sort" */,
    Reduce      /* "reduce" */,
    MapReduce   /* "map_reduce" */,
    RemoteCopy  /* "remote_copy" */,
    JoinReduce  /* "join_reduce" */,
    Vanilla     /* "vanilla" */,
};

///
/// @brief Operation progress.
struct TOperationProgress
{
    ///
    /// @brief Total job statistics.
    TJobStatistics JobStatistics;

    ///
    /// @brief Job counter for various job states with hierarchy.
    TJobCounters JobCounters;

    ///
    /// @brief Time when this progress was built on scheduler or CA.
    TMaybe<TInstant> BuildTime;
};

///
/// @brief Brief operation progress (numbers of jobs in these states).
struct TOperationBriefProgress
{
    ui64 Aborted = 0;
    ui64 Completed = 0;
    ui64 Failed = 0;
    ui64 Lost = 0;
    ui64 Pending = 0;
    ui64 Running = 0;
    ui64 Total = 0;
};

///
/// @brief Operation result.
struct TOperationResult
{
    ///
    /// @brief For a unsuccessfully finished operation: description of error.
    TMaybe<TYtError> Error;
};

///
/// @brief Operation event (change of state).
struct TOperationEvent
{
    ///
    /// @brief New state of operation.
    TString State;

    ///
    /// @brief Time of state change.
    TInstant Time;
};

///
/// @brief Operation info.
///
/// A field may be `Nothing()` either if it was not requested (see @ref NYT::TGetOperationOptions::AttributeFilter)
/// or it is not available (i.e. `FinishTime` for a running operation).
/// @see https://ytsaurus.tech/docs/en/api/commands#get_operation
struct TOperationAttributes
{
    ///
    /// @brief Operation id.
    TMaybe<TOperationId> Id;

    ///
    /// @brief Operation type.
    TMaybe<EOperationType> Type;

    ///
    /// @brief Operation state.
    TMaybe<TString> State;

    ///
    /// @brief "Coarse-grained" operation state.
    TMaybe<EOperationBriefState> BriefState;

    ///
    /// @brief Name of user that started the operation.
    TMaybe<TString> AuthenticatedUser;

    ///
    /// @brief Operation start time.
    TMaybe<TInstant> StartTime;

    ///
    /// @brief Operation finish time (if the operation has finished).
    TMaybe<TInstant> FinishTime;

    ///
    /// @brief Brief progress of the operation.
    TMaybe<TOperationBriefProgress> BriefProgress;

    ///
    /// @brief Brief spec of operation (light-weight fields only).
    TMaybe<TNode> BriefSpec;

    ///
    /// @brief Spec of the operation as provided by the user.
    TMaybe<TNode> Spec;

    ///
    /// @brief Full spec of operation (all fields not specified by user are filled with default values).
    TMaybe<TNode> FullSpec;

    ///
    /// @brief Fields not recognized by scheduler.
    TMaybe<TNode> UnrecognizedSpec;

    ///
    /// @brief Is operation suspended.
    TMaybe<bool> Suspended;

    ///
    /// @brief Operation result.
    TMaybe<TOperationResult> Result;

    ///
    /// @brief Operation progress.
    TMaybe<TOperationProgress> Progress;

    ///
    /// @brief List of operation events (changes of state).
    TMaybe<TVector<TOperationEvent>> Events;

    ///
    /// @brief Map from alert name to its description.
    TMaybe<THashMap<TString, TYtError>> Alerts;
};

///
/// @brief Direction of cursor for paging, see @ref NYT::TListOperationsOptions::CursorDirection.
enum class ECursorDirection
{
    Past /* "past" */,
    Future /* "future" */,
};

///
/// @brief Options of @ref NYT::IClient::ListOperations command.
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#list_operations
struct TListOperationsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TListOperationsOptions;
    /// @endcond

    ///
    /// @name Time range specification
    ///
    /// List operations with start time in half-closed interval
    /// `[CursorTime, ToTime)` if `CursorDirection == Future` or
    /// `[FromTime, CursorTime)` if `CursorDirection == Past`.
    ///@{

    ///
    /// @brief Search for operations with start time >= `FromTime`.
    FLUENT_FIELD_OPTION(TInstant, FromTime);

    ///
    /// @brief Search for operations with start time < `ToTime`.
    FLUENT_FIELD_OPTION(TInstant, ToTime);

    ///
    /// @brief Additional restriction on operation start time (useful for pagination).
    ///
    /// Search for operations with start time >= `CursorTime` if `CursorDirection == Future`
    /// and with start time < `CursorTime` if `CursorDirection == Past`
    FLUENT_FIELD_OPTION(TInstant, CursorTime);

    ///
    /// @brief Direction of pagination (see @ref NYT::TListOperationsOptions::CursorTime).
    FLUENT_FIELD_OPTION(ECursorDirection, CursorDirection);

    ///@}

    ///
    /// @name Filters
    /// Choose operations satisfying given filters.
    ///@{

    ///
    /// @brief Search for `Filter` as a substring in operation text factors
    /// (e.g. title or input/output table paths).
    FLUENT_FIELD_OPTION(TString, Filter);

    ///
    /// @brief Choose operations whose pools include `Pool`.
    FLUENT_FIELD_OPTION(TString, Pool);

    ///
    /// @brief Choose operations with given @ref NYT::TOperationAttributes::AuthenticatedUser.
    FLUENT_FIELD_OPTION(TString, User);

    ///
    /// @brief Choose operations with given @ref NYT::TOperationAttributes::State.
    FLUENT_FIELD_OPTION(TString, State);

    ///
    /// @brief Choose operations with given @ref NYT::TOperationAttributes::Type.
    FLUENT_FIELD_OPTION(EOperationType, Type);

    ///
    /// @brief Choose operations having (or not having) any failed jobs.
    FLUENT_FIELD_OPTION(bool, WithFailedJobs);

    ///@}

    ///
    /// @brief Search for operations in the archive in addition to Cypress.
    FLUENT_FIELD_OPTION(bool, IncludeArchive);

    ///
    /// @brief Include the counters for different filter parameters in the response.
    ///
    /// Include number of operations for each pool, user, state, type
    /// and the number of operations having failed jobs.
    FLUENT_FIELD_OPTION(bool, IncludeCounters);

    ///
    /// @brief Return no more than `Limit` operations (current default and maximum value is 1000).
    FLUENT_FIELD_OPTION(i64, Limit);
};

///
/// @brief Response for @ref NYT::IClient::ListOperations command.
struct TListOperationsResult
{
    ///
    /// @brief Found operations' attributes.
    TVector<TOperationAttributes> Operations;

    ///
    /// @name Counters for different filter.
    ///
    /// If counters were requested (@ref NYT::TListOperationsOptions::IncludeCounters is `true`)
    /// the maps contain the number of operations found for each pool, user, state and type.
    /// NOTE:
    ///  1) Counters ignore CursorTime and CursorDirection,
    ///     they always are collected in the whole [FromTime, ToTime) interval.
    ///  2) Each next counter in the sequence [pool, user, state, type, with_failed_jobs]
    ///     takes into account all the previous filters (i.e. if you set User filter to "some-user"
    ///     type counts describe only operations with user "some-user").
    /// @{

    ///
    /// @brief Number of operations for each pool.
    TMaybe<THashMap<TString, i64>> PoolCounts;

    ///
    /// @brief Number of operations for each user (subject to previous filters).
    TMaybe<THashMap<TString, i64>> UserCounts;

    ///
    /// @brief Number of operations for each state (subject to previous filters).
    TMaybe<THashMap<TString, i64>> StateCounts;

    ///
    /// @brief Number of operations for each type (subject to previous filters).
    TMaybe<THashMap<EOperationType, i64>> TypeCounts;

    ///
    /// @brief Number of operations having failed jobs (subject to all previous filters).
    TMaybe<i64> WithFailedJobsCount;

    /// @}

    ///
    /// @brief Whether some operations were not returned due to @ref NYT::TListOperationsOptions::Limit.
    ///
    /// `Incomplete == true` means that not all operations satisfying filters
    /// were returned (limit exceeded) and you need to repeat the request with new @ref NYT::TListOperationsOptions::CursorTime
    /// (e.g. `CursorTime == *Operations.back().StartTime`, but don't forget to
    /// remove the duplicates).
    bool Incomplete;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Data source for @ref NYT::IClient::ListJobs command.
enum class EListJobsDataSource : int
{
    Runtime  /* "runtime" */,
    Archive  /* "archive" */,
    Auto     /* "auto" */,
    Manual   /* "manual" */,
};

///
/// @brief Job type.
enum class EJobType : int
{
    SchedulerFirst    /* "scheduler_first" */,
    Map               /* "map" */,
    PartitionMap      /* "partition_map" */,
    SortedMerge       /* "sorted_merge" */,
    OrderedMerge      /* "ordered_merge" */,
    UnorderedMerge    /* "unordered_merge" */,
    Partition         /* "partition" */,
    SimpleSort        /* "simple_sort" */,
    FinalSort         /* "final_sort" */,
    SortedReduce      /* "sorted_reduce" */,
    PartitionReduce   /* "partition_reduce" */,
    ReduceCombiner    /* "reduce_combiner" */,
    RemoteCopy        /* "remote_copy" */,
    IntermediateSort  /* "intermediate_sort" */,
    OrderedMap        /* "ordered_map" */,
    JoinReduce        /* "join_reduce" */,
    Vanilla           /* "vanilla" */,
    SchedulerUnknown  /* "scheduler_unknown" */,
    SchedulerLast     /* "scheduler_last" */,
    ReplicatorFirst   /* "replicator_first" */,
    ReplicateChunk    /* "replicate_chunk" */,
    RemoveChunk       /* "remove_chunk" */,
    RepairChunk       /* "repair_chunk" */,
    SealChunk         /* "seal_chunk" */,
    ReplicatorLast    /* "replicator_last" */,
};

///
/// @brief Well-known task names.
enum class ETaskName : int
{
    Map               /* "map" */,
    PartitionMap0     /* "partition_map(0)" */,
    SortedMerge       /* "sorted_merge" */,
    OrderedMerge      /* "ordered_merge" */,
    UnorderedMerge    /* "unordered_merge" */,
    Partition0        /* "partition(0)" */,
    Partition1        /* "partition(1)" */,
    Partition2        /* "partition(2)" */,
    SimpleSort        /* "simple_sort" */,
    FinalSort         /* "final_sort" */,
    SortedReduce      /* "sorted_reduce" */,
    PartitionReduce   /* "partition_reduce" */,
    ReduceCombiner    /* "reduce_combiner" */,
    RemoteCopy        /* "remote_copy" */,
    IntermediateSort  /* "intermediate_sort" */,
    OrderedMap        /* "ordered_map" */,
    JoinReduce        /* "join_reduce" */,
};

///
/// @brief Task name (can either well-known or just a string).
class TTaskName
{
public:

    // Constructors are implicit by design.

    ///
    /// @brief Construct a custom task name.
    TTaskName(TString taskName);

    ///
    /// @brief Construct a custom task name.
    TTaskName(const char* taskName);

    ///
    /// @brief Construct a well-known task name.
    TTaskName(ETaskName taskName);

    const TString& Get() const;

private:
    TString TaskName_;
};

///
/// @brief Job state.
enum class EJobState : int
{
    None       /* "none" */,
    Waiting    /* "waiting" */,
    Running    /* "running" */,
    Aborting   /* "aborting" */,
    Completed  /* "completed" */,
    Failed     /* "failed" */,
    Aborted    /* "aborted" */,
    Lost       /* "lost" */,
};

///
/// @brief Job sort field.
///
/// @see @ref NYT::TListJobsOptions.
enum class EJobSortField : int
{
    Type       /* "type" */,
    State      /* "state" */,
    StartTime  /* "start_time" */,
    FinishTime /* "finish_time" */,
    Address    /* "address" */,
    Duration   /* "duration" */,
    Progress   /* "progress" */,
    Id         /* "id" */,
};

///
/// @brief Job sort direction.
///
/// @see @ref NYT::TListJobsOptions.
enum class EJobSortDirection : int
{
    Ascending /* "ascending" */,
    Descending /* "descending" */,
};

///
/// @brief Options for @ref NYT::IClient::ListJobs.
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#list_jobs
struct TListJobsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TListJobsOptions;
    /// @endcond

    ///
    /// @name Filters
    /// Return only jobs with given value of parameter (type, state, address and existence of stderr).
    /// If a field is `Nothing()`, return jobs with all possible values of the corresponding parameter.
    /// @{

    ///
    /// @brief Job type.
    FLUENT_FIELD_OPTION(EJobType, Type);

    ///
    /// @brief Job state.
    FLUENT_FIELD_OPTION(EJobState, State);

    ///
    /// @brief Address of the cluster node where job was running.
    FLUENT_FIELD_OPTION(TString, Address);

    ///
    /// @brief Return only jobs whose stderr has been saved.
    FLUENT_FIELD_OPTION(bool, WithStderr);

    ///
    /// @brief Return only jobs whose spec has been saved.
    FLUENT_FIELD_OPTION(bool, WithSpec);

    ///
    /// @brief Return only jobs whose fail context has been saved.
    FLUENT_FIELD_OPTION(bool, WithFailContext);

    ///
    /// @brief Return only jobs with monitoring descriptor.
    FLUENT_FIELD_OPTION(bool, WithMonitoringDescriptor);

    /// @}

    ///
    /// @name Sort options
    /// @{

    ///
    /// @brief Sort by this field.
    FLUENT_FIELD_OPTION(EJobSortField, SortField);

    ///
    /// @brief Sort order.
    FLUENT_FIELD_OPTION(ESortOrder, SortOrder);

    /// @}

    ///
    /// @brief Data source.
    ///
    /// Where to search for jobs: in scheduler and Cypress ('Runtime'), in archive ('Archive'),
    /// automatically basing on operation presence in Cypress ('Auto') or choose manually (`Manual').
    FLUENT_FIELD_OPTION(EListJobsDataSource, DataSource);

    /// @deprecated
    FLUENT_FIELD_OPTION(bool, IncludeCypress);

    /// @deprecated
    FLUENT_FIELD_OPTION(bool, IncludeControllerAgent);

    /// @deprecated
    FLUENT_FIELD_OPTION(bool, IncludeArchive);

    ///
    /// @brief Maximum number of jobs to return.
    FLUENT_FIELD_OPTION(i64, Limit);

    ///
    /// @brief Number of jobs (in specified sort order) to skip.
    ///
    /// Together with @ref NYT::TListJobsOptions::Limit may be used for pagination.
    FLUENT_FIELD_OPTION(i64, Offset);
};

///
/// @brief Description of a core dump that happened in the job.
struct TCoreInfo
{
    i64 ProcessId;
    TString ExecutableName;
    TMaybe<ui64> Size;
    TMaybe<TYtError> Error;
};

///
/// @brief Job attributes.
///
/// A field may be `Nothing()` if it is not available (i.e. `FinishTime` for a running job).
///
/// @see https://ytsaurus.tech/docs/en/api/commands#get_job
struct TJobAttributes
{
    ///
    /// @brief Job id.
    TMaybe<TJobId> Id;

    ///
    /// @brief Job type
    TMaybe<EJobType> Type;

    ///
    /// @brief Job state.
    TMaybe<EJobState> State;

    ///
    /// @brief Address of a cluster node where job was running.
    TMaybe<TString> Address;

    ///
    /// @brief The name of the task that job corresponds to.
    TMaybe<TString> TaskName;

    ///
    /// @brief Job start time.
    TMaybe<TInstant> StartTime;

    ///
    /// @brief Job finish time (for a finished job).
    TMaybe<TInstant> FinishTime;

    ///
    /// @brief Estimated ratio of job's completed work.
    TMaybe<double> Progress;

    ///
    /// @brief Size of saved job stderr.
    TMaybe<i64> StderrSize;

    ///
    /// @brief Error for a unsuccessfully finished job.
    TMaybe<TYtError> Error;

    ///
    /// @brief Job brief statistics.
    TMaybe<TNode> BriefStatistics;

    ///
    /// @brief Job input paths (with ranges).
    TMaybe<TVector<TRichYPath>> InputPaths;

    ///
    /// @brief Infos for core dumps produced by job.
    TMaybe<TVector<TCoreInfo>> CoreInfos;
};

///
/// @brief Response for @ref NYT::IOperation::ListJobs.
struct TListJobsResult
{
    ///
    /// @brief Jobs.
    TVector<TJobAttributes> Jobs;

    ///
    /// @deprecated
    TMaybe<i64> CypressJobCount;

    ///
    /// @brief Number of jobs retrieved from controller agent.
    TMaybe<i64> ControllerAgentJobCount;

    ///
    /// @brief Number of jobs retrieved from archive.
    TMaybe<i64> ArchiveJobCount;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Options for @ref NYT::IClient::GetJob.
struct TGetJobOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetJobOptions;
    /// @endcond
};

///
/// @brief Options for @ref NYT::IClient::GetJobInput.
struct TGetJobInputOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetJobInputOptions;
    /// @endcond
};

///
/// @brief Options for @ref NYT::IClient::GetJobFailContext.
struct TGetJobFailContextOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetJobFailContextOptions;
    /// @endcond
};

///
/// @brief Options for @ref NYT::IClient::GetJobStderr.
struct TGetJobStderrOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetJobStderrOptions;
    /// @endcond
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Options for @ref NYT::IOperation::GetFailedJobInfo.
struct TGetFailedJobInfoOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetFailedJobInfoOptions;
    /// @endcond

    ///
    /// @brief How many jobs to download. Which jobs will be chosen is undefined.
    FLUENT_FIELD_DEFAULT(ui64, MaxJobCount, 10);

    ///
    /// @brief How much of stderr tail should be downloaded.
    FLUENT_FIELD_DEFAULT(ui64, StderrTailSize, 64 * 1024);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Interface representing an operation.
struct IOperation
    : public TThrRefBase
{
    virtual ~IOperation() = default;

    ///
    /// @brief Get operation id.
    virtual const TOperationId& GetId() const = 0;

    ///
    /// @brief Get URL of the operation in YT Web UI.
    virtual TString GetWebInterfaceUrl() const = 0;

    ///
    /// @brief Get last error for not started operations. Get state on YT cluster for started operations.
    ///
    /// For not started operations last error is an error that's being retried during operation
    /// preparation/start (e.g. lock files, start operation request).
    virtual TString GetStatus() const = 0;

    ///
    /// @brief Get preparation future.
    ///
    /// @return future that is set when operation is prepared.
    virtual ::NThreading::TFuture<void> GetPreparedFuture() = 0;

    ///
    /// @brief Start operation synchronously.
    ///
    /// @note: Do NOT call this method twice.
    ///
    /// If operation is not prepared yet, Start() will block waiting for preparation finish.
    /// Be ready to catch exception if operation preparation or start failed.
    virtual void Start() = 0;

    ///
    /// @brief Is the operation started
    ///
    /// Returns true if the operation is started on the cluster
    virtual bool IsStarted() const = 0;

    ///
    /// @brief Get start future.
    ///
    /// @return future that is set when operation is started.
    virtual ::NThreading::TFuture<void> GetStartedFuture() = 0;

    ///
    /// @brief Start watching operation.
    ///
    /// @return future that is set when operation is complete.
    ///
    /// @note: the user should check value of returned future to ensure that operation completed successfully e.g.
    /// @code{.cpp}
    ///     auto operationComplete = operation->Watch();
    ///     operationComplete.Wait();
    ///     operationComplete.GetValue(); /// will throw if operation completed with errors
    /// @endcode
    ///
    /// If operation is completed successfully the returned future contains void value.
    /// If operation is completed with error future contains @ref NYT::TOperationFailedError.
    /// In rare cases when error occurred while waiting (e.g. YT become unavailable) future might contain other exception.
    virtual ::NThreading::TFuture<void> Watch() = 0;

    ///
    /// @brief Get information about failed jobs.
    ///
    /// Can be called for operation in any stage.
    /// Though user should keep in mind that this method always fetches info from cypress
    /// and doesn't work when operation is archived. Successfully completed operations can be archived
    /// quite quickly (in about ~30 seconds).
    virtual TVector<TFailedJobInfo> GetFailedJobInfo(const TGetFailedJobInfoOptions& options = TGetFailedJobInfoOptions()) = 0;

    ///
    /// Get operation brief state.
    virtual EOperationBriefState GetBriefState() = 0;

    ///
    /// @brief Get error (if operation has failed).
    ///
    /// @return `Nothing()` if operation is in 'Completed' or 'InProgress' state (or reason for failed / aborted operation).
    virtual TMaybe<TYtError> GetError() = 0;

    ///
    /// Get job statistics.
    virtual TJobStatistics GetJobStatistics() = 0;

    ///
    /// Get operation progress.
    ///
    /// @return `Nothing()` if operation has no running jobs yet, e.g. when it is in "materializing" or "pending" state.
    virtual TMaybe<TOperationBriefProgress> GetBriefProgress() = 0;

    ///
    /// @brief Abort operation.
    ///
    /// Operation will be finished immediately.
    /// All results of completed/running jobs will be lost.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#abort_op
    virtual void AbortOperation() = 0;

    ///
    /// @brief Complete operation.
    ///
    /// Operation will be finished immediately.
    /// All results of completed jobs will appear in output tables.
    /// All results of running (not completed) jobs will be lost.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#complete_op
    virtual void CompleteOperation() = 0;

    ///
    /// @brief Suspend operation.
    ///
    /// Jobs will not be aborted by default, c.f. @ref NYT::TSuspendOperationOptions.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#suspend_op
    virtual void SuspendOperation(
        const TSuspendOperationOptions& options = TSuspendOperationOptions()) = 0;

    ///
    /// @brief Resume previously suspended operation.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#resume_op
    virtual void ResumeOperation(
        const TResumeOperationOptions& options = TResumeOperationOptions()) = 0;

    ///
    /// @brief Get operation attributes.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#get_operation
    virtual TOperationAttributes GetAttributes(
        const TGetOperationOptions& options = TGetOperationOptions()) = 0;

    ///
    /// @brief Update operation runtime parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#update_op_parameters
    virtual void UpdateParameters(
        const TUpdateOperationParametersOptions& options = TUpdateOperationParametersOptions()) = 0;

    ///
    /// @brief Get job attributes.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#get_job
    virtual TJobAttributes GetJob(
        const TJobId& jobId,
        const TGetJobOptions& options = TGetJobOptions()) = 0;

    ///
    /// List jobs satisfying given filters (see @ref NYT::TListJobsOptions).
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#list_jobs
    virtual TListJobsResult ListJobs(
        const TListJobsOptions& options = TListJobsOptions()) = 0;
};

///
/// @brief Interface of client capable of managing operations.
struct IOperationClient
{
    ///
    /// @brief Run Map operation.
    ///
    /// @param spec Operation spec.
    /// @param mapper Instance of a job to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map
    IOperationPtr Map(
        const TMapOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run Map operation.
    ///
    /// @param mapper Instance of a job to run.
    /// @param input Input table(s)
    /// @param output Output table(s)
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map
    IOperationPtr Map(
        ::TIntrusivePtr<IMapperBase> mapper,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TMapOperationSpec& spec = TMapOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run raw Map operation.
    ///
    /// @param spec Operation spec.
    /// @param rawJob Instance of a raw mapper to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map
    virtual IOperationPtr RawMap(
        const TRawMapOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> rawJob,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run Reduce operation.
    ///
    /// @param spec Operation spec.
    /// @param reducer Instance of a job to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce
    IOperationPtr Reduce(
        const TReduceOperationSpec& spec,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run Reduce operation.
    ///
    /// @param reducer Instance of a job to run.
    /// @param input Input table(s)
    /// @param output Output table(s)
    /// @param reduceBy Columns to group rows by.
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce
    IOperationPtr Reduce(
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TSortColumns& reduceBy,
        const TReduceOperationSpec& spec = TReduceOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run raw Reduce operation.
    ///
    /// @param spec Operation spec.
    /// @param rawJob Instance of a raw reducer to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce
    virtual IOperationPtr RawReduce(
        const TRawReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> rawJob,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run JoinReduce operation.
    ///
    /// @param spec Operation spec.
    /// @param reducer Instance of a job to run.
    /// @param options Optional parameters.
    ///
    /// @deprecated Use @ref NYT::IOperationClient::Reduce with @ref NYT::TReduceOperationSpec::EnableKeyGuarantee set to `false.
    IOperationPtr JoinReduce(
        const TJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run raw JoinReduce operation.
    ///
    /// @param spec Operation spec.
    /// @param rawJob Instance of a raw reducer to run.
    /// @param options Optional parameters.
    ///
    /// @deprecated Use @ref NYT::IOperationClient::RawReduce with @ref NYT::TReduceOperationSpec::EnableKeyGuarantee set to `false.
    virtual IOperationPtr RawJoinReduce(
        const TRawJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> rawJob,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run MapReduce operation.
    ///
    /// @param spec Operation spec.
    /// @param mapper Instance of a map job to run (identity mapper if `nullptr`).
    /// @param reducer Instance of a reduce job to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
    IOperationPtr MapReduce(
        const TMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run MapReduce operation.
    ///
    /// @param spec Operation spec.
    /// @param mapper Instance of a map job to run (identity mapper if `nullptr`).
    /// @param reducerCombiner Instance of a reduce combiner to run (identity reduce combiner if `nullptr`).
    /// @param reducer Instance of a reduce job to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
    IOperationPtr MapReduce(
        const TMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reduceCombiner,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run MapReduce operation.
    ///
    /// @param mapper Instance of mapper to run (identity mapper if `nullptr`).
    /// @param reducer Instance of reducer to run.
    /// @param input Input table(s)
    /// @param output Output table(s)
    /// @param reduceBy Columns to group rows by.
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
    IOperationPtr MapReduce(
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TSortColumns& reduceBy,
        TMapReduceOperationSpec spec = TMapReduceOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run MapReduce operation.
    ///
    /// @param mapper Instance of mapper to run (identity mapper if `nullptr`).
    /// @param reduceCombiner Instance of reduceCombiner to run (identity reduce combiner if `nullptr`).
    /// @param reducer Instance of reducer to run.
    /// @param input Input table(s)
    /// @param output Output table(s)
    /// @param reduceBy Columns to group rows by.
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
    IOperationPtr MapReduce(
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reduceCombiner,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TSortColumns& reduceBy,
        TMapReduceOperationSpec spec = TMapReduceOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run raw MapReduce operation.
    ///
    /// @param spec Operation spec.
    /// @param mapper Instance of a raw mapper to run (identity mapper if `nullptr`).
    /// @param mapper Instance of a raw reduce combiner to run (identity reduce combiner if `nullptr`).
    /// @param mapper Instance of a raw reducer to run.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce
    virtual IOperationPtr RawMapReduce(
        const TRawMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> mapper,
        ::TIntrusivePtr<IRawJob> reduceCombiner,
        ::TIntrusivePtr<IRawJob> reducer,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run Sort operation.
    ///
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/sort
    virtual IOperationPtr Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run Sort operation.
    ///
    /// @param input Input table(s).
    /// @param output Output table.
    /// @param sortBy Columns to sort input rows by.
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/sort
    IOperationPtr Sort(
        const TOneOrMany<TRichYPath>& input,
        const TRichYPath& output,
        const TSortColumns& sortBy,
        const TSortOperationSpec& spec = TSortOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    ///
    /// @brief Run Merge operation.
    ///
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/merge
    virtual IOperationPtr Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run Erase operation.
    ///
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/erase
    virtual IOperationPtr Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run RemoteCopy operation.
    ///
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/remote_copy
    virtual IOperationPtr RemoteCopy(
        const TRemoteCopyOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Run Vanilla operation.
    ///
    /// @param spec Operation spec.
    /// @param options Optional parameters.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla
    virtual IOperationPtr RunVanilla(
        const TVanillaOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    ///
    /// @brief Abort operation.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#abort_operation
    virtual void AbortOperation(
        const TOperationId& operationId) = 0;

    ///
    /// @brief Complete operation.
    ///
    /// @see https://ytsaurus.tech/docs/en/api/commands#complete_operation
    virtual void CompleteOperation(
        const TOperationId& operationId) = 0;

    ///
    /// @brief Wait for operation to finish.
    virtual void WaitForOperation(
        const TOperationId& operationId) = 0;

    ///
    /// @brief Check and return operation status.
    ///
    /// @note this function will never return @ref NYT::EOperationBriefState::Failed or @ref NYT::EOperationBriefState::Aborted status,
    /// it will throw @ref NYT::TOperationFailedError instead.
    virtual EOperationBriefState CheckOperation(
        const TOperationId& operationId) = 0;

    ///
    /// @brief Create an operation object given operation id.
    ///
    /// @throw @ref NYT::TErrorResponse if the operation doesn't exist.
    virtual IOperationPtr AttachOperation(const TOperationId& operationId) = 0;

private:
    virtual IOperationPtr DoMap(
        const TMapOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> mapper,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoReduce(
        const TReduceOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> reducer,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoJoinReduce(
        const TJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> reducer,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoMapReduce(
        const TMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> mapper,
        ::TIntrusivePtr<IStructuredJob> reduceCombiner,
        ::TIntrusivePtr<IStructuredJob> reducer,
        const TOperationOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OPERATION_INL_H_
#include "operation-inl.h"
#undef OPERATION_INL_H_
