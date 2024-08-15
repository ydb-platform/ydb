#pragma once

#include "fwd.h"
#include "common.h"
#include "node.h"

#include <library/cpp/yt/misc/enum.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>

#include <util/datetime/base.h>

namespace NYT {

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

    bool UseAsyncTxPinger;
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

    void Reset();

    TConfig();

    static TConfigPtr Get();
};

////////////////////////////////////////////////////////////////////////////////

struct TProcessState
{
    TString FqdnHostName;
    TString UserName;
    TVector<TString> CommandLine;

    // Command line with everything that looks like tokens censored.
    TVector<TString> CensoredCommandLine;
    int Pid;
    TString ClientVersion;

    TProcessState();

    void SetCommandLine(int argc, const char* argv[]);

    static TProcessState* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
