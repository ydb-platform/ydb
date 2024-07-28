#pragma once

///
/// @file yt/cpp/mapreduce/interface/client_method_options.h
///
/// Header containing options for @ref NYT::IClient methods.

#include "common.h"
#include "config.h"
#include "format.h"
#include "public.h"
#include "retry_policy.h"

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// Type of the cypress node.
enum ENodeType : int
{
    NT_STRING               /* "string_node" */,
    NT_INT64                /* "int64_node" */,
    NT_UINT64               /* "uint64_node" */,
    NT_DOUBLE               /* "double_node" */,
    NT_BOOLEAN              /* "boolean_node" */,
    NT_MAP                  /* "map_node" */,
    NT_LIST                 /* "list_node" */,
    NT_FILE                 /* "file" */,
    NT_TABLE                /* "table" */,
    NT_DOCUMENT             /* "document" */,
    NT_REPLICATED_TABLE     /* "replicated_table" */,
    NT_TABLE_REPLICA        /* "table_replica" */,
    NT_USER                 /* "user" */,
    NT_SCHEDULER_POOL       /* "scheduler_pool" */,
    NT_LINK                 /* "link" */,
    NT_GROUP                /* "group" */,
    NT_PORTAL               /* "portal_entrance" */,
    NT_CHAOS_TABLE_REPLICA  /* "chaos_table_replica" */,
};

///
/// @brief Mode of composite type representation in yson.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/data-types#yson
enum class EComplexTypeMode : int
{
    Named /* "named" */,
    Positional /* "positional" */,
};

///
/// @brief Options for @ref NYT::ICypressClient::Create
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#create
struct TCreateOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TCreateOptions;
    /// @endcond

    /// Create missing parent directories if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    ///
    /// @brief Do not raise error if node already exists.
    ///
    /// Node is not recreated.
    /// Force and IgnoreExisting MUST NOT be used simultaneously.
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);

    ///
    /// @brief Recreate node if it exists.
    ///
    /// Force and IgnoreExisting MUST NOT be used simultaneously.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// @brief Set node attributes.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

///
/// @brief Options for @ref NYT::ICypressClient::Remove
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#remove
struct TRemoveOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TRemoveOptions;
    /// @endcond

    ///
    /// @brief Remove whole tree when removing composite cypress node (e.g. `map_node`).
    ///
    /// Without this option removing nonempty composite node will fail.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// @brief Do not fail if removing node doesn't exist.
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

/// Base class for options for operations that read from master.
template <typename TDerived>
struct TMasterReadOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Where to read from.
    FLUENT_FIELD_OPTION(EMasterReadKind, ReadFrom);
};

///
/// @brief Options for @ref NYT::ICypressClient::Exists
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#exists
struct TExistsOptions
    : public TMasterReadOptions<TExistsOptions>
{
};

///
/// @brief Options for @ref NYT::ICypressClient::Get
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#get
struct TGetOptions
    : public TMasterReadOptions<TGetOptions>
{
    /// @brief Attributes that should be fetched with each node.
    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);

    /// @brief Limit for the number of children node.
    FLUENT_FIELD_OPTION(i64, MaxSize);
};

///
/// @brief Options for @ref NYT::ICypressClient::Set
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#set
struct TSetOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSetOptions;
    /// @endcond

    /// Create missing parent directories if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Allow setting any nodes, not only attribute and document ones.
    FLUENT_FIELD_OPTION(bool, Force);
};

///
/// @brief Options for @ref NYT::ICypressClient::MultisetAttributes
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#multiset_attributes
struct TMultisetAttributesOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TMultisetAttributesOptions;
    /// @endcond

    FLUENT_FIELD_OPTION(bool, Force);
};

///
/// @brief Options for @ref NYT::ICypressClient::List
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#list
struct TListOptions
    : public TMasterReadOptions<TListOptions>
{
    /// @cond Doxygen_Suppress
    using TSelf = TListOptions;
    /// @endcond

    /// Attributes that should be fetched for each node.
    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);

    /// Limit for the number of children that will be fetched.
    FLUENT_FIELD_OPTION(i64, MaxSize);
};

///
/// @brief Options for @ref NYT::ICypressClient::Copy
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#copy
struct TCopyOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TCopyOptions;
    /// @endcond

    /// Create missing directories in destination path if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Allows to use existing node as destination, it will be overwritten.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// Whether to preserves account of source node.
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);

    /// Whether to preserve `expiration_time` attribute of source node.
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTime);
};

///
/// @brief Options for @ref NYT::ICypressClient::Move
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#move
struct TMoveOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TMoveOptions;
    /// @endcond

    /// Create missing directories in destination path if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Allows to use existing node as destination, it will be overwritten.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// Whether to preserves account of source node.
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);

    /// Whether to preserve `expiration_time` attribute of source node.
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTime);
};

///
/// @brief Options for @ref NYT::ICypressClient::Link
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#link
struct TLinkOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TLinkOptions;
    /// @endcond

    /// Create parent directories of destination if they don't exist.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Do not raise error if link already exists.
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);

    /// Force rewrite target node.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// Attributes of created link.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

///
/// @brief Options for @ref NYT::ICypressClient::Concatenate
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#concatenate
struct TConcatenateOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TConcatenateOptions;
    /// @endcond

    /// Whether we should append to destination or rewrite it.
    FLUENT_FIELD_OPTION(bool, Append);
};

///
/// @brief Options for @ref NYT::IIOClient::CreateBlobTableReader
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#read_blob_table
struct TBlobTableReaderOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TBlobTableReaderOptions;
    /// @endcond

    /// Name of the part index column. By default it is "part_index".
    FLUENT_FIELD_OPTION(TString, PartIndexColumnName);

    /// Name of the data column. By default it is "data".
    FLUENT_FIELD_OPTION(TString, DataColumnName);

    ///
    /// @brief Size of each part.
    ///
    /// All blob parts except the last part of the blob must be of this size
    /// otherwise blob table reader emits error.
    FLUENT_FIELD_DEFAULT(ui64, PartSize, 4 * 1024 * 1024);

    /// @brief Offset from which to start reading
    FLUENT_FIELD_DEFAULT(i64, Offset, 0);
};

///
/// @brief Resource limits for operation (or pool)
///
/// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/scheduler/scheduler-and-pools#resursy
/// @see NYT::TUpdateOperationParametersOptions
struct TResourceLimits
{
    /// @cond Doxygen_Suppress
    using TSelf = TResourceLimits;
    /// @endcond

    /// Number of slots for user jobs.
    FLUENT_FIELD_OPTION(i64, UserSlots);

    /// Number of cpu cores.
    FLUENT_FIELD_OPTION(double, Cpu);

    /// Network usage. Doesn't have precise physical unit.
    FLUENT_FIELD_OPTION(i64, Network);

    /// Memory in bytes.
    FLUENT_FIELD_OPTION(i64, Memory);
};

///
/// @brief Scheduling options for single pool tree.
///
/// @see NYT::TUpdateOperationParametersOptions
struct TSchedulingOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSchedulingOptions;
    /// @endcond

    ///
    /// @brief Pool to switch operation to.
    ///
    /// @note Switching is currently disabled on the server (will induce an exception).
    FLUENT_FIELD_OPTION(TString, Pool);

    /// @brief Operation weight.
    FLUENT_FIELD_OPTION(double, Weight);

    /// @brief Operation resource limits.
    FLUENT_FIELD_OPTION(TResourceLimits, ResourceLimits);
};

///
/// @brief Collection of scheduling options for multiple pool trees.
///
/// @see NYT::TUpdateOperationParametersOptions
struct TSchedulingOptionsPerPoolTree
{
    /// @cond Doxygen_Suppress
    using TSelf = TSchedulingOptionsPerPoolTree;
    /// @endcond

    TSchedulingOptionsPerPoolTree(const THashMap<TString, TSchedulingOptions>& options = {})
        : Options_(options)
    { }

    /// Add scheduling options for pool tree.
    TSelf& Add(TStringBuf poolTreeName, const TSchedulingOptions& schedulingOptions)
    {
        Y_ENSURE(Options_.emplace(poolTreeName, schedulingOptions).second);
        return *this;
    }

    THashMap<TString, TSchedulingOptions> Options_;
};

///
/// @brief Options for @ref NYT::IOperation::SuspendOperation
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#suspend_operation
struct TSuspendOperationOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSuspendOperationOptions;
    /// @endcond

    ///
    /// @brief Whether to abort already running jobs.
    ///
    /// By default running jobs are not aborted.
    FLUENT_FIELD_OPTION(bool, AbortRunningJobs);
};

///
/// @brief Options for @ref NYT::IOperation::ResumeOperation
///
/// @note They are empty for now but options might appear in the future.
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#resume_operation
struct TResumeOperationOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TResumeOperationOptions;
    /// @endcond
};

///
/// @brief Options for @ref NYT::IOperation::UpdateParameters
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#update_operation_parameters
struct TUpdateOperationParametersOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TUpdateOperationParametersOptions;
    /// @endcond

    /// New owners of the operation.
    FLUENT_VECTOR_FIELD(TString, Owner);

    /// Pool to switch operation to (for all pool trees it is running in).
    FLUENT_FIELD_OPTION(TString, Pool);

    /// New operation weight (for all pool trees it is running in).
    FLUENT_FIELD_OPTION(double, Weight);

    /// Scheduling options for each pool tree the operation is running in.
    FLUENT_FIELD_OPTION(TSchedulingOptionsPerPoolTree, SchedulingOptionsPerPoolTree);
};

///
/// @brief Base class for many options related to IO.
///
/// @ref NYT::TFileWriterOptions
/// @ref NYT::TFileReaderOptions
/// @ref NYT::TTableReaderOptions
/// @ref NYT::TTableWriterOptions
template <class TDerived>
struct TIOOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Advanced options for reader/writer.
    ///
    /// Readers/writers have many options not of all of them are supported by library.
    /// If you need such unsupported option, you might use `Config` option until
    /// option is supported.
    ///
    /// Example:
    ///
    ///     TTableWriterOptions().Config(TNode()("max_row_weight", 64 << 20)))
    ///
    /// @note We encourage you to ask yt@ to add native C++ support of required options
    /// and use `Config` only as temporary solution while native support is not ready.
    FLUENT_FIELD_OPTION(TNode, Config);

    ///
    /// @brief Whether to create internal client transaction for reading / writing table.
    ///
    /// This is advanced option.
    ///
    /// If `CreateTransaction` is set to `false`  reader/writer doesn't create internal transaction
    /// and doesn't lock table. This option is overridden (effectively `false`) for writers by
    /// @ref NYT::TTableWriterOptions::SingleHttpRequest
    ///
    /// WARNING: if `CreateTransaction` is `false`, read/write might become non-atomic.
    /// Change ONLY if you are sure what you are doing!
    FLUENT_FIELD_DEFAULT(bool, CreateTransaction, true);
};

/// @brief Options for reading file from YT.
struct TFileReaderOptions
    : public TIOOptions<TFileReaderOptions>
{
    ///
    /// @brief Offset to start reading from.
    ///
    /// By default reading is started from the beginning of the file.
    FLUENT_FIELD_OPTION(i64, Offset);

    ///
    /// @brief Maximum length to read.
    ///
    /// By default file is read until the end.
    FLUENT_FIELD_OPTION(i64, Length);
};

/// @brief Options that control how server side of YT stores data.
struct TWriterOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TWriterOptions;
    /// @endcond

    ///
    /// @brief Whether to wait all replicas to be written.
    ///
    /// When set to true upload will be considered successful as soon as
    /// @ref NYT::TWriterOptions::MinUploadReplicationFactor number of replicas are created.
    FLUENT_FIELD_OPTION(bool, EnableEarlyFinish);

    /// Number of replicas to be created.
    FLUENT_FIELD_OPTION(ui64, UploadReplicationFactor);

    ///
    /// Min number of created replicas needed to consider upload successful.
    ///
    /// @see NYT::TWriterOptions::EnableEarlyFinish
    FLUENT_FIELD_OPTION(ui64, MinUploadReplicationFactor);

    ///
    /// @brief Desired size of a chunk.
    ///
    /// @see @ref NYT::TWriterOptions::RetryBlockSize
    FLUENT_FIELD_OPTION(ui64, DesiredChunkSize);

    ///
    /// @brief Size of data block accumulated in memory to provide retries.
    ///
    /// Data is accumulated in memory buffer so in case error occurs data could be resended.
    ///
    /// If `RetryBlockSize` is not set buffer size is set to `DesiredChunkSize`.
    /// If neither `RetryBlockSize` nor `DesiredChunkSize` is set size of buffer is 64MB.
    ///
    /// @note Written chunks cannot be larger than size of this memory buffer.
    ///
    /// Since DesiredChunkSize is compared against data already compressed with compression codec
    /// it makes sense to set `RetryBlockSize = DesiredChunkSize / ExpectedCompressionRatio`
    ///
    /// @see @ref NYT::TWriterOptions::DesiredChunkSize
    /// @see @ref NYT::TTableWriterOptions::SingleHttpRequest
    FLUENT_FIELD_OPTION(size_t, RetryBlockSize);
};

///
/// @brief Options for writing file
///
/// @see NYT::IIOClient::CreateFileWriter
struct TFileWriterOptions
    : public TIOOptions<TFileWriterOptions>
{
    ///
    /// @brief Whether to compute MD5 sum of written file.
    ///
    /// If ComputeMD5 is set to `true` and we are appending to an existing file
    /// the `md5` attribute must be set (i.e. it was previously written only with `ComputeMD5 == true`).
    FLUENT_FIELD_OPTION(bool, ComputeMD5);

    ///
    /// @brief Wheter to call Finish automatically in writer destructor.
    ///
    /// If set to true (default) Finish() is called automatically in the destructor of writer.
    /// It is convenient for simple usecases but might be error-prone if writing exception safe code
    /// (In case of exceptions it's common to abort writer and not commit partial data).
    ///
    /// If set to false Finish() has to be called explicitly.
    FLUENT_FIELD_DEFAULT(bool, AutoFinish, true);

    ///
    /// @brief Options to control how YT server side writes data.
    ///
    /// @see NYT::TWriterOptions
    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

class TSkiffRowHints
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TSkiffRowHints;
    /// @endcond

    ///
    /// @brief Library doesn't interpret it, only pass it to CreateSkiffParser<...>() and GetSkiffSchema<...>() functions.
    ///
    /// You can set something in it to pass necessary information to CreateSkiffParser<...>() and GetSkiffSchema<...>() functions.
    FLUENT_FIELD_OPTION(TNode, Attributes);

    ///
    /// @brief Index of table in parallel table reader.
    ///
    /// For internal usage only. If you set it, it will be overriden by parallel table reader.
    FLUENT_FIELD_OPTION(int, TableIndex);
};

/// Options that control how C++ objects represent table rows when reading or writing a table.
class TFormatHints
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TFormatHints;
    /// @endcond

    ///
    /// @brief Whether to skip null values.
    ///
    /// When set to true TNode doesn't contain null column values
    /// (e.g. corresponding keys will be missing instead of containing null value).
    ///
    /// Only meaningful for TNode representation.
    ///
    /// Useful for sparse tables which have many columns in schema
    /// but only few columns are set in any row.
    FLUENT_FIELD_DEFAULT(bool, SkipNullValuesForTNode, false);

    ///
    /// @brief Whether to convert string to numeric and boolean types (e.g. "42u" -> 42u, "false" -> %false)
    /// when writing to schemaful table.
    FLUENT_FIELD_OPTION(bool, EnableStringToAllConversion);

    ///
    /// @brief Whether to convert numeric and boolean types to string (e.g., 3.14 -> "3.14", %true -> "true")
    /// when writing to schemaful table.
    FLUENT_FIELD_OPTION(bool, EnableAllToStringConversion);

    ///
    /// @brief Whether to convert uint64 <-> int64 when writing to schemaful table.
    ///
    /// On overflow the corresponding error with be raised.
    ///
    /// This options is enabled by default.
    FLUENT_FIELD_OPTION(bool, EnableIntegralTypeConversion);

    /// Whether to convert uint64 and int64 to double (e.g. 42 -> 42.0) when writing to schemaful table.
    FLUENT_FIELD_OPTION(bool, EnableIntegralToDoubleConversion);

    /// Shortcut for enabling all type conversions.
    FLUENT_FIELD_OPTION(bool, EnableTypeConversion);

    ///
    /// @brief Controls how complex types are represented in TNode or yson-strings.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/data-types#yson
    FLUENT_FIELD_OPTION(EComplexTypeMode, ComplexTypeMode);

    ///
    /// @brief Allow to use any meta-information for creating skiff schema and parser for reading ISkiffRow.
    FLUENT_FIELD_OPTION(TSkiffRowHints, SkiffRowHints);

    ///
    /// @brief Apply the patch to the fields.
    ///
    /// Non-default and non-empty values replace the default and empty ones.
    void Merge(const TFormatHints& patch);
};

/// Options that control which control attributes (like row_index) are added to rows during read.
class TControlAttributes
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TControlAttributes;
    /// @endcond

    ///
    /// @brief Whether to add "row_index" attribute to rows read.
    FLUENT_FIELD_DEFAULT(bool, EnableRowIndex, true);

    ///
    /// @brief Whether to add "range_index" attribute to rows read.
    FLUENT_FIELD_DEFAULT(bool, EnableRangeIndex, true);
};

/// Options for @ref NYT::IClient::CreateTableReader
struct TTableReaderOptions
    : public TIOOptions<TTableReaderOptions>
{
    /// @deprecated Size of internal client buffer.
    FLUENT_FIELD_DEFAULT(size_t, SizeLimit, 4 << 20);

    ///
    /// @brief Allows to fine tune format that is used for reading tables.
    ///
    /// Has no effect when used with raw-reader.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);

    ///
    /// @brief Allows to tune which attributes are added to rows while reading tables.
    ///
    FLUENT_FIELD_DEFAULT(TControlAttributes, ControlAttributes, TControlAttributes());
};

/// Options for @ref NYT::IClient::CreateTableWriter
struct TTableWriterOptions
    : public TIOOptions<TTableWriterOptions>
{
    ///
    /// @brief Enable or disable retryful writing.
    ///
    /// If set to true no retry is made but we also make less requests to master.
    /// If set to false writer can make up to `TConfig::RetryCount` attempts to send each block of data.
    ///
    /// @note Writers' methods might throw strange exceptions that might look like network error
    /// when `SingleHttpRequest == true` and YT node encounters an error
    /// (due to limitations of HTTP protocol YT node have no chance to report error
    /// before it reads the whole input so it just drops the connection).
    FLUENT_FIELD_DEFAULT(bool, SingleHttpRequest, false);

    ///
    /// @brief Allows to change the size of locally buffered rows before flushing to yt.
    ///
    /// Used only with @ref NYT::TTableWriterOptions::SingleHttpRequest
    FLUENT_FIELD_DEFAULT(size_t, BufferSize, 64 << 20);

    ///
    /// @brief Allows to fine tune format that is used for writing tables.
    ///
    /// Has no effect when used with raw-writer.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);

    /// @brief Try to infer schema of inexistent table from the type of written rows.
    ///
    /// @note Default values for this option may differ depending on the row type.
    /// For protobuf it's currently false by default.
    FLUENT_FIELD_OPTION(bool, InferSchema);

    ///
    /// @brief Wheter to call Finish automatically in writer destructor.
    ///
    /// If set to true (default) Finish() is called automatically in the destructor of writer.
    /// It is convenient for simple usecases but might be error-prone if writing exception safe code
    /// (In case of exceptions it's common to abort writer and not commit partial data).
    ///
    /// If set to false Finish() has to be called explicitly.
    FLUENT_FIELD_DEFAULT(bool, AutoFinish, true);

    ///
    /// @brief Options to control how YT server side writes data.
    ///
    /// @see NYT::TWriterOptions
    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

///
/// @brief Options for @ref NYT::IClient::StartTransaction
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#start_tx
struct TStartTransactionOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TStartTransactionOptions;
    /// @endcond

    FLUENT_FIELD_DEFAULT(bool, PingAncestors, false);

    ///
    /// @brief How long transaction lives after last ping.
    ///
    /// If server doesn't receive any pings for transaction for this time
    /// transaction will be aborted. By default timeout is 15 seconds.
    FLUENT_FIELD_OPTION(TDuration, Timeout);

    ///
    /// @brief Moment in the future when transaction is aborted.
    FLUENT_FIELD_OPTION(TInstant, Deadline);

    ///
    /// @brief Whether to ping created transaction automatically.
    ///
    /// When set to true library creates a thread that pings transaction.
    /// When set to false library doesn't ping transaction and it's user responsibility to ping it.
    FLUENT_FIELD_DEFAULT(bool, AutoPingable, true);

    ///
    /// @brief Set the title attribute of transaction.
    ///
    /// If title was not specified
    /// neither using this option nor using @ref NYT::TStartTransactionOptions::Attributes option
    /// library will generate default title for transaction.
    /// Such default title includes machine name, pid, user name and some other useful info.
    FLUENT_FIELD_OPTION(TString, Title);

    ///
    /// @brief Set custom transaction attributes
    ///
    /// @note @ref NYT::TStartTransactionOptions::Title option overrides `"title"` attribute.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

///
/// @brief Options for attaching transaction.
///
/// @see NYT::IClient::AttachTransaction
struct TAttachTransactionOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TAttachTransactionOptions;
    /// @endcond

    ///
    /// @brief Ping transaction automatically.
    ///
    /// When set to |true| library creates a thread that pings transaction.
    /// When set to |false| library doesn't ping transaction and
    /// it's user responsibility to ping it.
    FLUENT_FIELD_DEFAULT(bool, AutoPingable, false);

    ///
    /// @brief Abort transaction on program termination.
    ///
    /// Should the transaction be aborted on program termination
    /// (either normal or by a signal or uncaught exception -- two latter
    /// only if @ref TInitializeOptions::CleanupOnTermination is set).
    FLUENT_FIELD_DEFAULT(bool, AbortOnTermination, false);
};

///
/// @brief Type of the lock.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locking_mode
/// @see NYT::ITransaction::Lock
enum ELockMode : int
{
    /// Exclusive lock.
    LM_EXCLUSIVE    /* "exclusive" */,

    /// Shared lock.
    LM_SHARED       /* "shared" */,

    /// Snapshot lock.
    LM_SNAPSHOT     /* "snapshot" */,
};

///
/// @brief Options for locking cypress node
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locks
/// @see NYT::ITransaction::Lock
struct TLockOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TLockOptions;
    /// @endcond

    ///
    /// @brief Whether to wait already locked node to be unlocked.
    ///
    /// If `Waitable' is set to true Lock method will create
    /// waitable lock, that will be taken once other transactions
    /// that hold lock to that node are committed / aborted.
    ///
    /// @note Lock method DOES NOT wait until lock is actually acquired.
    /// Waiting should be done using corresponding methods of ILock.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locking_queue
    FLUENT_FIELD_DEFAULT(bool, Waitable, false);

    ///
    /// @brief Also take attribute_key lock.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locks_compatibility
    FLUENT_FIELD_OPTION(TString, AttributeKey);

    ///
    /// @brief Also take child_key lock.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locks_compatibility
    FLUENT_FIELD_OPTION(TString, ChildKey);
};

///
/// @brief Options for @ref NYT::ITransaction::Unlock
///
/// @note They are empty for now but options might appear in the future.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locks_compatibility
struct TUnlockOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TUnlockOptions;
    /// @endcond
};

/// Base class for options that deal with tablets.
template <class TDerived>
struct TTabletOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// Index of a first tablet to deal with.
    FLUENT_FIELD_OPTION(i64, FirstTabletIndex);

    /// Index of a last tablet to deal with.
    FLUENT_FIELD_OPTION(i64, LastTabletIndex);
};

///
/// @brief Options for @ref NYT::IClient::MountTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#mount_table
struct TMountTableOptions
    : public TTabletOptions<TMountTableOptions>
{
    /// @cond Doxygen_Suppress
    using TSelf = TMountTableOptions;
    /// @endcond

    /// If specified table will be mounted to this cell.
    FLUENT_FIELD_OPTION(TTabletCellId, CellId);

    /// If set to true tablets will be mounted in freezed state.
    FLUENT_FIELD_DEFAULT(bool, Freeze, false);
};

///
/// @brief Options for @ref NYT::IClient::UnmountTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#unmount_table
struct TUnmountTableOptions
    : public TTabletOptions<TUnmountTableOptions>
{
    /// @cond Doxygen_Suppress
    using TSelf = TUnmountTableOptions;
    /// @endcond

    /// Advanced option, don't use unless yt team told you so.
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

///
/// @brief Options for @ref NYT::IClient::RemountTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#remount_table
struct TRemountTableOptions
    : public TTabletOptions<TRemountTableOptions>
{ };

///
/// @brief Options for @ref NYT::IClient::ReshardTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#reshard_table
struct TReshardTableOptions
    : public TTabletOptions<TReshardTableOptions>
{ };

///
/// @brief Options for @ref NYT::IClient::FreezeTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#freeze_table
struct TFreezeTableOptions
    : public TTabletOptions<TFreezeTableOptions>
{ };

///
/// @brief Options for @ref NYT::IClient::UnfreezeTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#unfreeze_table
struct TUnfreezeTableOptions
    : public TTabletOptions<TUnfreezeTableOptions>
{ };

///
/// @brief Options for @ref NYT::IClient::AlterTable
///
/// @see https://ytsaurus.tech/docs/en/api/commands#alter_table
struct TAlterTableOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TAlterTableOptions;
    /// @endcond

    /// Change table schema.
    FLUENT_FIELD_OPTION(TTableSchema, Schema);

    /// Alter table between static and dynamic mode.
    FLUENT_FIELD_OPTION(bool, Dynamic);

    ///
    /// @brief Changes id of upstream replica on metacluster.
    ///
    /// @see https://ytsaurus.tech/docs/en/description/dynamic_tables/replicated_dynamic_tables
    FLUENT_FIELD_OPTION(TReplicaId, UpstreamReplicaId);
};

///
/// @brief Options for @ref NYT::IClient::LookupRows
///
/// @see https://ytsaurus.tech/docs/en/api/commands#lookup_rows
struct TLookupRowsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TLookupRowsOptions;
    /// @endcond

    /// Timeout for operation.
    FLUENT_FIELD_OPTION(TDuration, Timeout);

    /// Column names to return.
    FLUENT_FIELD_OPTION(TColumnNames, Columns);

    ///
    /// @brief Whether to return rows that were not found in table.
    ///
    /// If set to true List returned by LookupRows method will have same
    /// length as list of keys. If row is not found in table corresponding item in list
    /// will have null value.
    FLUENT_FIELD_DEFAULT(bool, KeepMissingRows, false);

    /// If set to true returned values will have "timestamp" attribute.
    FLUENT_FIELD_OPTION(bool, Versioned);
};

///
/// @brief Options for @ref NYT::IClient::SelectRows
///
/// @see https://ytsaurus.tech/docs/en/api/commands#select_rows
struct TSelectRowsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSelectRowsOptions;
    /// @endcond

    /// Timeout for operation.
    FLUENT_FIELD_OPTION(TDuration, Timeout);

    ///
    /// @brief Limitation for number of rows read by single node.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language#query-complexity-limits-options
    FLUENT_FIELD_OPTION(i64, InputRowLimit);

    ///
    /// @brief Limitation for number of output rows on single cluster node.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language#query-complexity-limits-options
    FLUENT_FIELD_OPTION(i64, OutputRowLimit);

    ///
    /// @brief Maximum row ranges derived from WHERE clause.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language#query-complexity-limits-options
    FLUENT_FIELD_DEFAULT(ui64, RangeExpansionLimit, 1000);

    ///
    /// @brief Whether to fail if InputRowLimit or OutputRowLimit is exceeded.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language#query-complexity-limits-options
    FLUENT_FIELD_DEFAULT(bool, FailOnIncompleteResult, true);

    /// @brief Enable verbose logging on server side.
    FLUENT_FIELD_DEFAULT(bool, VerboseLogging, false);

    FLUENT_FIELD_DEFAULT(bool, EnableCodeCache, true);
};

/// Options for NYT::CreateClient;
struct TCreateClientOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TCreateClientOptions;
    /// @endcond

    /// @brief Impersonated user name.
    ///
    /// If authenticated user is allowed to impersonate other YT users (e.g. yql_agent), this field may be used to override user name.
    FLUENT_FIELD_OPTION(TString, ImpersonationUser);

    /// @brief User token.
    ///
    /// @see NYT::TCreateClientOptions::TokenPath
    FLUENT_FIELD(TString, Token);

    /// @brief Path to the file where user token is stored.
    ///
    /// Token is looked in these places in following order:
    ///   - @ref NYT::TCreateClientOptions::Token
    ///   - @ref NYT::TCreateClientOptions::TokenPath
    ///   - `TConfig::Get()->Token` option.
    ///   - `YT_TOKEN` environment variable
    ///   - `YT_SECURE_VAULT_YT_TOKEN` environment variable
    ///   - File specified in `YT_TOKEN_PATH` environment variable
    ///   - `$HOME/.yt/token` file.
    FLUENT_FIELD(TString, TokenPath);

    /// @brief TVM service ticket producer.
    ///
    /// We store a wrapper of NYT::TIntrusivePtr here (not a NYT::TIntrusivePtr),
    /// because otherwise other projects will have build problems
    /// because of visibility of two different `TIntrusivePtr`-s (::TInstrusivePtr and NYT::TInstrusivePtr).
    ///
    /// @see NYT::NAuth::TServiceTicketClientAuth
    /// {@
    NAuth::IServiceTicketAuthPtrWrapperPtr ServiceTicketAuth_ = nullptr;
    TSelf& ServiceTicketAuth(const NAuth::IServiceTicketAuthPtrWrapper& wrapper);
    /// @}

    /// @brief Use tvm-only endpoints in cluster connection.
    FLUENT_FIELD_DEFAULT(bool, TvmOnly, false);

    /// @brief Use HTTPs (use HTTP client from yt/yt/core always).
    ///
    /// @see UseCoreHttpClient
    FLUENT_FIELD_OPTION(bool, UseTLS);

    /// @brief Use HTTP client from yt/yt/core.
    FLUENT_FIELD_DEFAULT(bool, UseCoreHttpClient, false);

    ///
    /// @brief RetryConfig provider allows to fine tune request retries.
    ///
    /// E.g. set total timeout for all retries.
    FLUENT_FIELD_DEFAULT(IRetryConfigProviderPtr, RetryConfigProvider, nullptr);

    /// @brief Override global config for the client.
    ///
    /// The config contains implementation parameters such as connection timeouts,
    /// access token, api version and more.
    /// @see NYT::TConfig
    FLUENT_FIELD_DEFAULT(TConfigPtr, Config, nullptr);

    /// @brief Proxy Address to be used for connection
    FLUENT_FIELD_OPTION(TString, ProxyAddress);
};

///
/// @brief Options for @ref NYT::IBatchRequest::ExecuteBatch
///
/// @see https://ytsaurus.tech/docs/en/api/commands#execute_batch
struct TExecuteBatchOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TExecuteBatchOptions;
    /// @endcond

    ///
    /// @brief How many requests will be executed in parallel on the cluster.
    ///
    /// This parameter could be used to avoid RequestLimitExceeded errors.
    FLUENT_FIELD_OPTION(ui64, Concurrency);

    ///
    /// @brief Maximum size of batch sent in one request to server.
    ///
    /// Huge batches are executed using multiple requests.
    /// BatchPartMaxSize is maximum size of single request that goes to server
    /// If not specified it is set to `Concurrency * 5'
    FLUENT_FIELD_OPTION(ui64, BatchPartMaxSize);
};

///
/// @brief Durability mode.
///
/// @see NYT::TTabletTransactionOptions::TDurability
/// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables
enum class EDurability
{
    /// Sync mode (default).
    Sync    /* "sync" */,

    /// Async mode (might reduce latency of write requests, but less reliable).
    Async   /* "async" */,
};

///
/// @brief Atomicity mode.
///
/// @see NYT::TTabletTransactionOptions::TDurability
/// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables
enum class EAtomicity
{
    /// Transactions are non atomic (might reduce latency of write requests).
    None    /* "none" */,

    /// Transactions are atomic (default).
    Full    /* "full" */,
};

///
/// @brief Table replica mode.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/replicated-dynamic-tables#attributes
enum class ETableReplicaMode
{
    Sync    /* "sync" */,
    Async   /* "async" */,
};

/// Base class for options dealing with io to dynamic tables.
template <typename TDerived>
struct TTabletTransactionOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    ///
    /// @brief Atomicity mode of operation
    ///
    /// Setting to NYT::EAtomicity::None allows to improve latency of operations
    /// at the cost of weakening contracts.
    ///
    /// @note Use with care.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables
    FLUENT_FIELD_OPTION(EAtomicity, Atomicity);

    ///
    /// @brief Durability mode of operation
    ///
    /// Setting to NYT::EDurability::Async allows to improve latency of operations
    /// at the cost of weakening contracts.
    ///
    /// @note Use with care.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables
    FLUENT_FIELD_OPTION(EDurability, Durability);
};

///
/// @brief Options for NYT::IClient::InsertRows
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#insert_rows
struct TInsertRowsOptions
    : public TTabletTransactionOptions<TInsertRowsOptions>
{
    ///
    /// @brief Whether to overwrite missing columns with nulls.
    ///
    /// By default all columns missing in input data are set to Null and overwrite currently stored value.
    /// If `Update' is set to true currently stored value will not be overwritten for columns that are missing in input data.
    FLUENT_FIELD_OPTION(bool, Update);

    ///
    /// @brief Whether to overwrite or aggregate aggregated columns.
    ///
    /// Used with aggregating columns.
    /// By default value in aggregating column will be overwritten.
    /// If `Aggregate' is set to true row will be considered as delta and it will be aggregated with currently stored value.
    FLUENT_FIELD_OPTION(bool, Aggregate);

    ///
    /// @brief Whether to fail when inserting to table without sync replica.
    ///
    /// Used for insert operation for tables without sync replica.
    /// https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/replicated-dynamic-tables#write
    /// Default value is 'false'. So insertion into table without sync replicas fails.
    FLUENT_FIELD_OPTION(bool, RequireSyncReplica);
};

///
/// @brief Options for NYT::IClient::DeleteRows
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#delete_rows
struct TDeleteRowsOptions
    : public TTabletTransactionOptions<TDeleteRowsOptions>
{
    ///
    /// @brief Whether to fail when deleting from table without sync replica.
    ///
    // Used for delete operation for tables without sync replica.
    /// https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/replicated-dynamic-tables#write
    // Default value is 'false'. So deletion into table without sync replicas fails.
    FLUENT_FIELD_OPTION(bool, RequireSyncReplica);
};

///
/// @brief Options for NYT::IClient::TrimRows
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#trim_rows
struct TTrimRowsOptions
    : public TTabletTransactionOptions<TTrimRowsOptions>
{ };

/// @brief Options for NYT::IClient::AlterTableReplica
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#alter_table_replica
/// https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/replicated-dynamic-tables
struct TAlterTableReplicaOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TAlterTableReplicaOptions;
    /// @endcond

    ///
    /// @brief Whether to enable or disable replica.
    ///
    /// Doesn't change state of replica if `Enabled' is not set.
    FLUENT_FIELD_OPTION(bool, Enabled);

    ///
    /// @brief Change replica mode.
    ///
    /// Doesn't change replica mode if `Mode` is not set.
    FLUENT_FIELD_OPTION(ETableReplicaMode, Mode);
};

///
/// @brief Options for @ref NYT::IClient::GetFileFromCache
///
/// @note They are empty for now but options might appear in the future.
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#get_file_from_cache
struct TGetFileFromCacheOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetFileFromCacheOptions;
    /// @endcond
};

///
/// @brief Options for @ref NYT::IClient::GetTableColumnarStatistics
///
/// @note They are empty for now but options might appear in the future.
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#put_file_to_cache
struct TPutFileToCacheOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TPutFileToCacheOptions;
    /// @endcond

    /// Whether to preserve `expiration_timeout` attribute of source node.
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTimeout);
};

///
/// Type of permission used in ACL.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/access-control
enum class EPermission : int
{
    /// Applies to: all objects.
    Read         /* "read" */,

    /// Applies to: all objects.
    Write        /* "write" */,

    /// Applies to: accounts / pools.
    Use          /* "use" */,

    /// Applies to: all objects.
    Administer   /* "administer" */,

    /// Applies to: schemas.
    Create       /* "create" */,

    /// Applies to: all objects.
    Remove       /* "remove" */,

    /// Applies to: tables.
    Mount        /* "mount" */,

    /// Applies to: operations.
    Manage       /* "manage" */,
};

/// Whether permission is granted or denied.
enum class ESecurityAction : int
{
    /// Permission is granted.
    Allow /* "allow" */,

    /// Permission is denied.
    Deny  /* "deny" */,
};

///
/// @brief Options for @ref NYT::IClient::CheckPermission
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#check_permission
struct TCheckPermissionOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TCheckPermissionOptions;
    /// @endcond

    /// Columns to check permission to (for tables only).
    FLUENT_VECTOR_FIELD(TString, Column);
};

///
/// @brief Columnar statistics fetching mode.
///
/// @ref NYT::TGetTableColumnarStatisticsOptions::FetcherMode
enum class EColumnarStatisticsFetcherMode
{
    /// Slow mode for fetching precise columnar statistics.
    FromNodes /* "from_nodes" */,

    ///
    /// @brief Fast mode for fetching lightweight columnar statistics.
    ///
    /// Relative precision is 1 / 256.
    ///
    /// @note Might be unavailable for old tables in that case some upper bound is returned.
    FromMaster /* "from_master" */,

    /// Use lightweight columnar statistics (FromMaster) if available otherwise switch to slow but precise mode (FromNodes).
    Fallback /* "fallback" */,
};

///
/// @brief Options for @ref NYT::IClient::GetTableColumnarStatistics
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#get_table_columnar_statistics
struct TGetTableColumnarStatisticsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetTableColumnarStatisticsOptions;
    /// @endcond

    ///
    /// @brief Mode of statistics fetching.
    ///
    /// @ref NYT::EColumnarStatisticsFetcherMode
    FLUENT_FIELD_OPTION(EColumnarStatisticsFetcherMode, FetcherMode);
};

///
/// @brief Table partitioning mode.
///
/// @ref NYT::TGetTablePartitionsOptions::PartitionMode
enum class ETablePartitionMode
{
    ///
    /// @brief Ignores the order of input tables and their chunk and sorting orders.
    ///
    Unordered /* "unordered" */,

    ///
    /// @brief The order of table ranges inside each partition obey the order of input tables and their chunk orders.
    ///
    Ordered /* "ordered" */,
};

///
/// @brief Options for @ref NYT::IClient::GetTablePartitions
///
struct TGetTablePartitionsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetTablePartitionsOptions;
    /// @endcond

    ///
    /// @brief Table partitioning mode.
    ///
    /// @ref NYT::ETablePartitionMode
    FLUENT_FIELD(ETablePartitionMode, PartitionMode);

    ///
    /// @brief Approximate data weight of each output partition.
    ///
    FLUENT_FIELD(i64, DataWeightPerPartition);

    ///
    /// @brief Maximum output partition count.
    ///
    /// Consider the situation when the `MaxPartitionCount` is given
    /// and the total data weight exceeds `MaxPartitionCount * DataWeightPerPartition`.
    /// If `AdjustDataWeightPerPartition` is |true|
    /// `GetTablePartitions` will yield partitions exceeding the `DataWeightPerPartition`.
    /// If `AdjustDataWeightPerPartition` is |false|
    /// the partitioning will be aborted as soon as the output partition count exceeds this limit.
    FLUENT_FIELD_OPTION(int, MaxPartitionCount);

    ///
    /// @brief Allow the data weight per partition to exceed `DataWeightPerPartition` when `MaxPartitionCount` is set.
    ///
    /// |True| by default.
    FLUENT_FIELD_DEFAULT(bool, AdjustDataWeightPerPartition, true);
};

///
/// @brief Options for @ref NYT::IClient::GetTabletInfos
///
/// @note They are empty for now but options might appear in the future.
///
/// @see https://ytsaurus.tech/docs/en/api/commands.html#get_tablet_infos
struct TGetTabletInfosOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetTabletInfosOptions;
    /// @endcond
};

/// Options for @ref NYT::IClient::SkyShareTable
struct TSkyShareTableOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSkyShareTableOptions;
    /// @endcond

    ///
    /// @brief Key columns that are used to group files in a table into torrents.
    ///
    /// One torrent is created for each value of `KeyColumns` columns.
    /// If not specified, all files go into single torrent.
    FLUENT_FIELD_OPTION(TColumnNames, KeyColumns);

    /// @brief Allow skynet manager to return fastbone links to skynet. See YT-11437
    FLUENT_FIELD_OPTION(bool, EnableFastbone);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
