#include "config.h"

#include "operation.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/svnversion/svnversion.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yson/json/yson2json_adapter.h>

#include <util/string/strip.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/generic/singleton.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/type.h>
#include <util/system/hostname.h>
#include <util/system/user.h>
#include <util/system/env.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool TConfig::GetBool(const char* var, bool defaultValue)
{
    TString val = GetEnv(var, "");
    if (val.empty()) {
        return defaultValue;
    }
    return IsTrue(val);
}

int TConfig::GetInt(const char* var, int defaultValue)
{
    int result = 0;
    TString val = GetEnv(var, "");
    if (val.empty()) {
        return defaultValue;
    }
    try {
        result = FromString<int>(val);
    } catch (const yexception& e) {
        ythrow yexception() << "Cannot parse " << var << '=' << val << " as integer: " << e.what();
    }
    return result;
}

TDuration TConfig::GetDuration(const char* var, TDuration defaultValue)
{
    return TDuration::Seconds(GetInt(var, defaultValue.Seconds()));
}

EEncoding TConfig::GetEncoding(const char* var)
{
    const TString encodingName = GetEnv(var, "identity");
    EEncoding encoding;
    if (TryFromString(encodingName, encoding)) {
        return encoding;
    } else {
        ythrow yexception() << var << ": encoding '" << encodingName << "' is not supported";
    }
}

EUploadDeduplicationMode TConfig::GetUploadingDeduplicationMode(
        const char* var,
        EUploadDeduplicationMode defaultValue)
{
    const TString deduplicationMode = GetEnv(var, TEnumTraits<EUploadDeduplicationMode>::ToString(defaultValue));
    return TEnumTraits<EUploadDeduplicationMode>::FromString(deduplicationMode);
}

void TConfig::ValidateToken(const TString& token)
{
    for (size_t i = 0; i < token.size(); ++i) {
        ui8 ch = token[i];
        if (ch < 0x21 || ch > 0x7e) {
            ythrow yexception() << "Incorrect token character '" << ch << "' at position " << i;
        }
    }
}

TString TConfig::LoadTokenFromFile(const TString& tokenPath)
{
    TFsPath path(tokenPath);
    return path.IsFile() ? Strip(TIFStream(path).ReadAll()) : TString();
}

TNode TConfig::LoadJsonSpec(const TString& strSpec)
{
    TNode spec;
    TStringInput input(strSpec);
    TNodeBuilder builder(&spec);
    TYson2JsonCallbacksAdapter callbacks(&builder);

    Y_ENSURE(NJson::ReadJson(&input, &callbacks), "Cannot parse json spec: " << strSpec);
    Y_ENSURE(spec.IsMap(), "Json spec is not a map");

    return spec;
}

TRichYPath TConfig::LoadApiFilePathOptions(const TString& ysonMap)
{
    TNode attributes;
    try {
        attributes = NodeFromYsonString(ysonMap);
    } catch (const yexception& exc) {
        ythrow yexception() << "Failed to parse YT_API_FILE_PATH_OPTIONS (it must be yson map): " << exc;
    }
    TNode pathNode = "";
    pathNode.Attributes() = attributes;
    TRichYPath path;
    Deserialize(path, pathNode);
    return path;
}

void TConfig::LoadToken()
{
    if (auto envToken = GetEnv("YT_TOKEN")) {
        Token = envToken;
    } else if (auto envToken = GetEnv("YT_SECURE_VAULT_YT_TOKEN")) {
        // If this code runs inside an vanilla peration in YT
        // it should not use regular environment variable `YT_TOKEN`
        // because it would be visible in UI.
        // Token should be passed via `secure_vault` parameter in operation spec.
        Token = envToken;
    } else if (auto tokenPath = GetEnv("YT_TOKEN_PATH")) {
        Token = LoadTokenFromFile(tokenPath);
    } else {
        Token = LoadTokenFromFile(GetHomeDir() + "/.yt/token");
    }
    ValidateToken(Token);
}

void TConfig::LoadSpec()
{
    TString strSpec = GetEnv("YT_SPEC", "{}");
    Spec = LoadJsonSpec(strSpec);

    strSpec = GetEnv("YT_TABLE_WRITER", "{}");
    TableWriter = LoadJsonSpec(strSpec);
}

void TConfig::LoadTimings()
{
    ConnectTimeout = GetDuration("YT_CONNECT_TIMEOUT",
        TDuration::Seconds(10));

    SocketTimeout = GetDuration("YT_SOCKET_TIMEOUT",
        GetDuration("YT_SEND_RECEIVE_TIMEOUT", // common
            TDuration::Seconds(60)));

    AddressCacheExpirationTimeout = TDuration::Minutes(15);

    CacheLockTimeoutPerGb = TDuration::MilliSeconds(1000.0 * 1_GB * 8 / 20_MB); // 20 Mbps = 20 MBps / 8.

    TxTimeout = GetDuration("YT_TX_TIMEOUT",
        TDuration::Seconds(120));

    PingTimeout = GetDuration("YT_PING_TIMEOUT",
        TDuration::Seconds(5));

    PingInterval = GetDuration("YT_PING_INTERVAL",
        TDuration::Seconds(5));

    WaitLockPollInterval = TDuration::Seconds(5);

    RetryInterval = GetDuration("YT_RETRY_INTERVAL",
        TDuration::Seconds(3));

    ChunkErrorsRetryInterval = GetDuration("YT_CHUNK_ERRORS_RETRY_INTERVAL",
        TDuration::Seconds(60));

    RateLimitExceededRetryInterval = GetDuration("YT_RATE_LIMIT_EXCEEDED_RETRY_INTERVAL",
        TDuration::Seconds(60));

    StartOperationRetryInterval = GetDuration("YT_START_OPERATION_RETRY_INTERVAL",
        TDuration::Seconds(60));

    HostListUpdateInterval = TDuration::Seconds(60);
}

void TConfig::Reset()
{
    Hosts = GetEnv("YT_HOSTS", "hosts");
    Pool = GetEnv("YT_POOL");
    Prefix = GetEnv("YT_PREFIX");
    ApiVersion = GetEnv("YT_VERSION", "v3");
    LogLevel = GetEnv("YT_LOG_LEVEL", "error");
    LogPath = GetEnv("YT_LOG_PATH");

    ContentEncoding = GetEncoding("YT_CONTENT_ENCODING");
    AcceptEncoding = GetEncoding("YT_ACCEPT_ENCODING");

    GlobalTxId = GetEnv("YT_TRANSACTION", "");

    UseAsyncTxPinger = true;
    AsyncHttpClientThreads = 1;
    AsyncTxPingerPoolThreads = 1;

    ForceIpV4 = GetBool("YT_FORCE_IPV4");
    ForceIpV6 = GetBool("YT_FORCE_IPV6");
    UseHosts = GetBool("YT_USE_HOSTS", true);

    LoadToken();
    LoadSpec();
    LoadTimings();

    CacheUploadDeduplicationMode = GetUploadingDeduplicationMode("YT_UPLOAD_DEDUPLICATION", EUploadDeduplicationMode::Host);
    CacheUploadDeduplicationThreshold = 10_MB;

    RetryCount = Max(GetInt("YT_RETRY_COUNT", 10), 1);
    ReadRetryCount = Max(GetInt("YT_READ_RETRY_COUNT", 30), 1);
    StartOperationRetryCount = Max(GetInt("YT_START_OPERATION_RETRY_COUNT", 30), 1);

    RemoteTempFilesDirectory = GetEnv("YT_FILE_STORAGE",
        "//tmp/yt_wrapper/file_storage");
    RemoteTempTablesDirectory = GetEnv("YT_TEMP_TABLES_STORAGE",
        "//tmp/yt_wrapper/table_storage");
    RemoteTempTablesDirectory = GetEnv("YT_TEMP_DIR",
        RemoteTempTablesDirectory);

    InferTableSchema = false;

    UseClientProtobuf = GetBool("YT_USE_CLIENT_PROTOBUF", false);
    NodeReaderFormat = ENodeReaderFormat::Auto;
    ProtobufFormatWithDescriptors = true;

    MountSandboxInTmpfs = GetBool("YT_MOUNT_SANDBOX_IN_TMPFS");

    ApiFilePathOptions = LoadApiFilePathOptions(GetEnv("YT_API_FILE_PATH_OPTIONS", "{}"));

    ConnectionPoolSize = GetInt("YT_CONNECTION_POOL_SIZE", 16);

    TraceHttpRequestsMode = FromString<ETraceHttpRequestsMode>(to_lower(GetEnv("YT_TRACE_HTTP_REQUESTS", "never")));

    CommandsWithFraming = {
        "read_table",
        "get_table_columnar_statistics",
        "get_job_input",
        "concatenate",
        "partition_tables",
    };
}

TConfig::TConfig()
{
    Reset();
}

TConfigPtr TConfig::Get()
{
    struct TConfigHolder
    {
        TConfigHolder()
            : Config(::MakeIntrusive<TConfig>())
        { }

        TConfigPtr Config;
    };

    return Singleton<TConfigHolder>()->Config;
}

////////////////////////////////////////////////////////////////////////////////

TProcessState::TProcessState()
{
    try {
        FqdnHostName = ::FQDNHostName();
    } catch (const yexception& e) {
        try {
            FqdnHostName = ::HostName();
        } catch (const yexception& e) {
            ythrow yexception() << "Cannot get fqdn and host name: " << e.what();
        }
    }

    try {
        UserName = ::GetUsername();
    } catch (const yexception& e) {
#ifdef _win_
        ythrow yexception() << "Cannot get user name: " << e.what();
#else
        UserName = "u" + ToString(geteuid());
#endif
    }

    Pid = static_cast<int>(getpid());

    if (!ClientVersion) {
        ClientVersion = ::TStringBuilder() << "YT C++ native " << GetProgramCommitId();
    }
}

static TString CensorString(TString input)
{
    static const TString prefix = "AQAD-";
    if (input.find(prefix) == TString::npos) {
        return input;
    } else {
        return TString(input.size(), '*');
    }
}

void TProcessState::SetCommandLine(int argc, const char* argv[])
{
    for (int i = 0; i < argc; ++i) {
        CommandLine.push_back(argv[i]);
        CensoredCommandLine.push_back(CensorString(CommandLine.back()));
    }
}

TProcessState* TProcessState::Get()
{
    return Singleton<TProcessState>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
