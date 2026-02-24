#pragma once

#include "fwd.h"
#include "common.h"
#include "patchable_field.h"

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/yson_string/public.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NYson {

////////////////////////////////////////////////////////////////////////////////

struct IYsonConsumer;

enum class EYsonFormat : int;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson

////////////////////////////////////////////////////////////////////////////////

namespace NLogLevel {
    inline constexpr std::string_view Fatal = "fatal";
    inline constexpr std::string_view Error = "error";
    inline constexpr std::string_view Info = "info";
    inline constexpr std::string_view Debug = "debug";
} // namespace NLogLevel

////////////////////////////////////////////////////////////////////////////////

extern const TString DefaultHosts;
extern const TString DefaultRemoteTempTablesDirectory;
extern const TString DefaultRemoteTempFilesDirectory;

////////////////////////////////////////////////////////////////////////////////

enum EEncoding : int
{
    E_IDENTITY  /* "identity" */,
    E_GZIP      /* "gzip" */,
    E_BROTLI    /* "br" */,
    E_Z_LZ4     /* "z-lz4" */,
};

enum class ENodeReaderFormat : int
{
    Yson,  // Always use YSON format,
    Skiff, // Always use Skiff format, throw exception if it's not possible (non-strict schema, dynamic table etc.)
    Auto,  // Use Skiff format if it's possible, YSON otherwise
};

enum class ETraceHttpRequestsMode
{
    // Never dump http requests.
    Never /* "never" */,
    // Dump failed http requests.
    Error /* "error" */,
    // Dump all http requests.
    Always /* "always" */,
};

DEFINE_ENUM(EUploadDeduplicationMode,
    // For each file only one process' thread from all possible hosts can upload it to the file cache at the same time.
    // The others will wait for the uploading to finish and use already cached file.
    ((Global)   (0))

    // For each file and each particular host only one process' thread can upload it to the file cache at the same time.
    // The others will wait for the uploading to finish and use already cached file.
    ((Host)     (1))

    // All processes' threads will upload a file to the cache concurrently.
    ((Disabled) (2))
);

////////////////////////////////////////////////////////////////////////////////

/// Enum describing possible versions of table writer implemetation.
enum class ETableWriterVersion
{
    /// Allow library to choose version of writer.
    Auto,

    /// Stable but slower version of writer.
    V1,

    /// Unstable but faster version of writer (going to be default in the future).
    V2,
};

////////////////////////////////////////////////////////////////////////////////

struct TConfig
    : public TThrRefBase
{
    TString Hosts;
    TString Pool;
    TString Token;
    TString Prefix;
    TString ApiVersion;
    TString LogLevel;
    TString LogPath;
    THashSet<TString> LogExcludeCategories = {"Bus", "Net", "Dns", "Concurrency"};

    /// @brief Path to the structured log file for recording telemetry data in JSON format.
    /// This allows later retrieval and analysis of these metrics.
    TString StructuredLog;

    /// @brief Represents the role involved in HTTP proxy configuration.
    ///
    /// @note If the "Hosts" configuration option is specified, it is given priority over the HTTP proxy role.
    TString HttpProxyRole;

    /// @brief Represents the role involved in RPC proxy configuration.
    TString RpcProxyRole;

    /// @brief Proxy url aliasing rules to be used for connection.
    ///
    /// You can pass here "foo" => "fqdn:port" and afterwards use "foo" as handy alias,
    /// while all connections will be made to "fqdn:port" address.
    THashMap<TString, TString> ProxyUrlAliasingRules;

    ///
    /// For historical reasons mapreduce client uses its own logging system.
    ///
    /// Currently library uses yt/yt/core logging by default.
    /// But if user calls @ref NYT::SetLogger, library switches back to logger provided by user
    /// (except for messages from yt/yt/core).
    ///
    /// TODO: This is a temporary option for emergency fallback.
    /// Should be removed after eliminating all NYT::SetLogger references.
    bool LogUseCore = true;

    // Compression for data that is sent to YT cluster.
    EEncoding ContentEncoding;

    // Compression for data that is read from YT cluster.
    EEncoding AcceptEncoding;

    TString GlobalTxId;

    bool ForceIpV4;
    bool ForceIpV6;
    bool UseHosts;

    TDuration HostListUpdateInterval;

    TNode Spec;
    TNode TableWriter;

    TDuration ConnectTimeout;
    TDuration SocketTimeout;
    TDuration AddressCacheExpirationTimeout;
    TDuration TxTimeout;
    TDuration PingTimeout;
    TDuration PingInterval;

    int AsyncHttpClientThreads;
    int AsyncTxPingerPoolThreads;

    // How often should we poll for lock state
    TDuration WaitLockPollInterval;

    TDuration RetryInterval;
    TDuration ChunkErrorsRetryInterval;

    TDuration RateLimitExceededRetryInterval;
    TDuration StartOperationRetryInterval;

    int RetryCount;
    int ReadRetryCount;
    int StartOperationRetryCount;

    /// @brief Period for checking status of running operation.
    TDuration OperationTrackerPollPeriod = TDuration::Seconds(5);

    TString RemoteTempFilesDirectory;
    TString RemoteTempTablesDirectory;
    // @brief Keep temp tables produced by TTempTable (despite their name). Should not be used in user programs,
    // but may be useful for setting via environment variable for debugging purposes.
    bool KeepTempTables = false;

    //
    // Infer schemas for nonexstent tables from typed rows (e.g. protobuf)
    // when writing from operation or client writer.
    // This options can be overridden in TOperationOptions and TTableWriterOptions.
    bool InferTableSchema;

    bool UseClientProtobuf;
    ENodeReaderFormat NodeReaderFormat;
    bool ProtobufFormatWithDescriptors;

    int ConnectionPoolSize;

    /// Defines replication factor that is used for files that are uploaded to YT
    /// to use them in operations.
    int FileCacheReplicationFactor = 10;

    /// @brief Used when waiting for other process which uploads the same file to the file cache.
    ///
    /// If CacheUploadDeduplicationMode is not Disabled, current process can wait for some other
    /// process which is uploading the same file. This value is proportional to the timeout of waiting,
    /// actual timeout computes as follows: fileSizeGb * CacheLockTimeoutPerGb.
    /// Default timeout assumes that host has uploading speed equal to 20 Mb/s.
    /// If timeout was reached, the file will be uploaded by current process without any other waits.
    TDuration CacheLockTimeoutPerGb;

    /// @brief Used to prevent concurrent uploading of the same file to the file cache.
    /// NB: Each mode affects only users with the same mode enabled.
    EUploadDeduplicationMode CacheUploadDeduplicationMode;

    // @brief Minimum byte size for files to undergo deduplication at upload
    i64 CacheUploadDeduplicationThreshold;

    bool MountSandboxInTmpfs;

    /// @brief Set upload options (e.g.) for files created by library.
    ///
    /// Path itself is always ignored but path options (e.g. `BypassArtifactCache`) are used when uploading system files:
    /// cppbinary, job state, etc
    TRichYPath ApiFilePathOptions;

    // Testing options, should never be used in user programs.
    bool UseAbortableResponse = false;
    bool EnableDebugMetrics = false;

    //
    // There is optimization used with local YT that enables to skip binary upload and use real binary path.
    // When EnableLocalModeOptimization is set to false this optimization is completely disabled.
    bool EnableLocalModeOptimization = true;

    //
    // If you want see stderr even if you jobs not failed set this true.
    bool WriteStderrSuccessfulJobs = false;

    //
    // This configuration is useful for debug.
    // If set to ETraceHttpRequestsMode::Error library will dump all http error requests.
    // If set to ETraceHttpRequestsMode::All library will dump all http requests.
    // All tracing occurres as DEBUG level logging.
    ETraceHttpRequestsMode TraceHttpRequestsMode = ETraceHttpRequestsMode::Never;

    TString SkynetApiHost;

    // Sets SO_PRIORITY option on the socket
    TMaybe<int> SocketPriority;

    // Framing settings
    // (cf. https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#framing).
    THashSet<TString> CommandsWithFraming;

    /// Which implemetation of table writer to use.
    ETableWriterVersion TableWriterVersion = ETableWriterVersion::Auto;

    /// Redirects stdout to stderr for jobs.
    bool RedirectStdoutToStderr = false;

    /// Append job and operation IDs as shell command options.
    bool EnableDebugCommandLineArguments = true;

    /// Path to document node with cluster config for |IClient::GetDynamicConfiguration|.
    TString ConfigRemotePatchPath = "//sys/client_config";

    /// Pattern for generating operation web link in |GetOperationWebInterfaceUrl|.
    TPatchableField<TString> OperationLinkPattern = TPatchableField<TString>("operation_link_pattern", "https://yt.yandex-team.ru/{cluster_ui_host}/operations/{operation_id}");

    static bool GetBool(const char* var, bool defaultValue = false);
    static int GetInt(const char* var, int defaultValue);
    static TDuration GetDuration(const char* var, TDuration defaultValue);
    static EEncoding GetEncoding(const char* var);
    static EUploadDeduplicationMode GetUploadingDeduplicationMode(
        const char* var,
        EUploadDeduplicationMode defaultValue);

    static void ValidateToken(const TString& token);
    static TString LoadTokenFromFile(const TString& tokenPath);

    static TNode LoadJsonSpec(const TString& strSpec);

    static TRichYPath LoadApiFilePathOptions(const TString& ysonMap);

    void LoadToken();
    void LoadSpec();
    void LoadTimings();
    void LoadProxyUrlAliasingRules();

    void Reset();

    TConfig();

    static TConfigPtr Get();
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TConfig& config, NYson::IYsonConsumer* consumer);

void Deserialize(TConfig& config, const TNode& node);

////////////////////////////////////////////////////////////////////////////////

TString ConfigToYsonString(const TConfig& config, NYson::EYsonFormat format = NYson::EYsonFormat::Pretty);

TConfig ConfigFromYsonString(TString serializedConfig);

////////////////////////////////////////////////////////////////////////////////

struct TProcessState
{
    TString FqdnHostName;
    TString UserName;

    int Pid;
    TString ClientVersion;
    TString BinaryPath;
    TString BinaryName;

    TProcessState();

    static TProcessState* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
