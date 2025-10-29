#include "config.h"
#include "serialize.h"
#include "fluent.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/svnversion/svnversion.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yson/json/yson2json_adapter.h>

#include <library/cpp/yt/misc/cast.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/singleton.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/string/type.h>
#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/hostname.h>
#include <util/system/user.h>

namespace NYT {

const TString DefaultHosts = "hosts";
const TString DefaultRemoteTempTablesDirectory = "//tmp/yt_wrapper/table_storage";
const TString DefaultRemoteTempFilesDirectory = "//tmp/yt_wrapper/file_storage";

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

void TConfig::LoadProxyUrlAliasingRules()
{
    TString strConfig = GetEnv("YT_PROXY_URL_ALIASING_CONFIG");
    if (!strConfig) {
        return;
    }

    NYT::TNode nodeConfig;

    try {
        nodeConfig = NodeFromYsonString(strConfig);
        Y_ENSURE(nodeConfig.IsMap());
    } catch (const yexception& exc) {
        ythrow yexception()
            << "Failed to parse YT_PROXY_URL_ALIASING_CONFIG (it must be yson map): "
            << exc;
    }

    for (const auto& [key, value] : nodeConfig.AsMap()) {
        Y_ENSURE(value.IsString(), "Proxy url is not string");
        ProxyUrlAliasingRules.emplace(key, value.AsString());
    }
}

void TConfig::Reset()
{
    Hosts = GetEnv("YT_HOSTS", DefaultHosts);
    Pool = GetEnv("YT_POOL");
    Prefix = GetEnv("YT_PREFIX");
    ApiVersion = GetEnv("YT_VERSION", "v3");
    LogLevel = GetEnv("YT_LOG_LEVEL", "error");
    LogPath = GetEnv("YT_LOG_PATH");
    LogUseCore = GetBool("YT_LOG_USE_CORE", true);
    StructuredLog = GetEnv("YT_STRUCTURED_LOG");

    HttpProxyRole = GetEnv("YT_HTTP_PROXY_ROLE");
    RpcProxyRole = GetEnv("YT_RPC_PROXY_ROLE");

    ContentEncoding = GetEncoding("YT_CONTENT_ENCODING");
    AcceptEncoding = GetEncoding("YT_ACCEPT_ENCODING");

    GlobalTxId = GetEnv("YT_TRANSACTION", "");

    AsyncHttpClientThreads = 1;
    AsyncTxPingerPoolThreads = 1;

    ForceIpV4 = GetBool("YT_FORCE_IPV4");
    ForceIpV6 = GetBool("YT_FORCE_IPV6");
    UseHosts = GetBool("YT_USE_HOSTS", true);

    LoadToken();
    LoadSpec();
    LoadTimings();
    LoadProxyUrlAliasingRules();

    CacheUploadDeduplicationMode = GetUploadingDeduplicationMode("YT_UPLOAD_DEDUPLICATION", EUploadDeduplicationMode::Host);
    CacheUploadDeduplicationThreshold = 10_MB;

    RetryCount = Max(GetInt("YT_RETRY_COUNT", 10), 1);
    ReadRetryCount = Max(GetInt("YT_READ_RETRY_COUNT", 30), 1);
    StartOperationRetryCount = Max(GetInt("YT_START_OPERATION_RETRY_COUNT", 30), 1);

    RemoteTempFilesDirectory = GetEnv("YT_FILE_STORAGE", DefaultRemoteTempFilesDirectory);
    RemoteTempTablesDirectory = GetEnv("YT_TEMP_TABLES_STORAGE", DefaultRemoteTempTablesDirectory);
    RemoteTempTablesDirectory = GetEnv("YT_TEMP_DIR", RemoteTempTablesDirectory);
    KeepTempTables = GetBool("YT_KEEP_TEMP_TABLES");

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

template <std::integral T>
void Deserialize(T& value, const TNode& node)
{
    if (node.GetType() == TNode::EType::Int64) {
        value = CheckedIntegralCast<T>(node.AsInt64());
    } else if (node.GetType() == TNode::EType::Uint64) {
        value = CheckedIntegralCast<T>(node.AsUint64());
    } else {
        throw yexception() << "Cannot parse integral value from node of type " << node.GetType();
    }
}

template <typename T>
void Deserialize(THashSet<T>& hs, const TNode& node)
{
    if (node.GetType() != TNode::EType::List) {
        throw yexception() << "Cannot parse hashset from node of type " << node.GetType();
    }
    for (const auto& value : node.AsList()) {
        T deserialized;
        Deserialize(deserialized, value);
        hs.insert(deserialized);
    }
}

template <typename T>
requires TEnumTraits<T>::IsEnum
void Deserialize(T& value, const TNode& node)
{
    if (auto nodeType = node.GetType(); nodeType != TNode::EType::String) {
        throw yexception() << "Enum deserialization expects EType::String, got " << node.GetType();
    }
    value = TEnumTraits<T>::FromString(node.AsString());
}

template <typename T>
requires (!TEnumTraits<T>::IsEnum) && std::is_enum_v<T>
void Deserialize(T& value, const TNode& node)
{
    if (auto nodeType = node.GetType(); nodeType != TNode::EType::String) {
        throw yexception() << "Enum deserialization expects EType::String, got " << node.GetType();
    }
    value = ::FromString<T>(node.AsString());
}

void Deserialize(TDuration& value, const TNode& node)
{
    switch (node.GetType()) {
        case TNode::EType::Int64: {
            auto ms = node.AsInt64();
            if (ms < 0) {
                ythrow yexception() << "Duration cannot be negative";
            }
            value = TDuration::MilliSeconds(static_cast<ui64>(ms));
            break;
        }

        case TNode::EType::Uint64:
            value = TDuration::MilliSeconds(node.AsUint64());
            break;

        case TNode::EType::Double: {
            auto ms = node.AsDouble();
            if (ms < 0) {
                ythrow yexception() << "Duration cannot be negative";
            }
            value = TDuration::MicroSeconds(static_cast<ui64>(ms * 1'000.0));
            break;
        }

        case TNode::EType::String:
            value = TDuration::Parse(node.AsString());
            break;

        default:
            ythrow yexception() << "Cannot parse duration from " << node.GetType();
    }
}

// const auto& nodeMap = node.AsMap();
#define DESERIALIZE_ITEM(NAME, MEMBER) \
    if (const auto* item = nodeMap.FindPtr(NAME)) { \
        Deserialize(MEMBER, *item); \
    }

void Serialize(const TConfig& config, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("hosts").Value(config.Hosts)
        .Item("pool").Value(config.Pool)
        .Item("prefix").Value(config.Prefix)
        .Item("api_version").Value(config.ApiVersion)
        .Item("log_level").Value(config.LogLevel)
        .Item("log_path").Value(config.LogPath)
        .Item("log_exclude_categories")
            .BeginList()
                .DoFor(config.LogExcludeCategories, [&](TFluentList list, const auto& category){
                    list.Item().Value(category);
                })
            .EndList()
        .Item("structured_log").Value(config.StructuredLog)
        .Item("http_proxy_role").Value(config.HttpProxyRole)
        .Item("rpc_proxy_role").Value(config.RpcProxyRole)
        .Item("proxy_url_aliasing_rules")
            .BeginMap()
                .DoFor(config.ProxyUrlAliasingRules, [&](TFluentMap map, const auto& item){
                    map.Item(item.first).Value(item.second);
                })
            .EndMap()
        .Item("log_use_core").Value(config.LogUseCore)
        .Item("content_encoding").Value(::ToString(config.ContentEncoding))
        .Item("accept_encoding").Value(::ToString(config.AcceptEncoding))
        .Item("global_tx_id").Value(config.GlobalTxId)
        .Item("force_ipv4").Value(config.ForceIpV4)
        .Item("force_ipv6").Value(config.ForceIpV6)
        .Item("use_hosts").Value(config.UseHosts)
        .Item("host_list_update_interval").Value(config.HostListUpdateInterval.ToString())
        .Item("spec").Value(config.Spec)
        .Item("table_writer").Value(config.TableWriter)
        .Item("connect_timeout").Value(config.ConnectTimeout.ToString())
        .Item("socket_timeout").Value(config.SocketTimeout.ToString())
        .Item("address_cache_expiration_timeout").Value(config.AddressCacheExpirationTimeout.ToString())
        .Item("tx_timeout").Value(config.TxTimeout.ToString())
        .Item("ping_timeout").Value(config.PingTimeout.ToString())
        .Item("ping_interval").Value(config.PingInterval.ToString())
        .Item("async_http_client_threads").Value(config.AsyncHttpClientThreads)
        .Item("async_tx_pinger_pool_threads").Value(config.AsyncTxPingerPoolThreads)
        .Item("wait_lock_poll_interval").Value(config.WaitLockPollInterval.ToString())
        .Item("retry_interval").Value(config.RetryInterval.ToString())
        .Item("chunk_errors_retry_interval").Value(config.ChunkErrorsRetryInterval.ToString())
        .Item("rate_limit_exceeded_retry_interval").Value(config.RateLimitExceededRetryInterval.ToString())
        .Item("start_operation_retry_interval").Value(config.StartOperationRetryInterval.ToString())
        .Item("retry_count").Value(config.RetryCount)
        .Item("read_retry_count").Value(config.ReadRetryCount)
        .Item("start_operation_retry_count").Value(config.StartOperationRetryCount)
        .Item("operation_tracker_poll_period").Value(config.OperationTrackerPollPeriod.ToString())
        .Item("remote_temp_files_directory").Value(config.RemoteTempFilesDirectory)
        .Item("remote_temp_tables_directory").Value(config.RemoteTempTablesDirectory)
        .Item("keep_temp_tables").Value(config.KeepTempTables)
        .Item("infer_table_schema").Value(config.InferTableSchema)
        .Item("use_client_protobuf").Value(config.UseClientProtobuf)
        .Item("node_reader_format").Value(::ToString(config.NodeReaderFormat))
        .Item("protobuf_format_with_descriptors").Value(config.ProtobufFormatWithDescriptors)
        .Item("connection_pool_size").Value(config.ConnectionPoolSize)
        .Item("file_cache_replication_factor").Value(config.FileCacheReplicationFactor)
        .Item("cache_lock_timeout_per_gb").Value(config.CacheLockTimeoutPerGb.ToString())
        .Item("cache_upload_deduplication_mode")
            .Value(TEnumTraits<EUploadDeduplicationMode>::ToString(config.CacheUploadDeduplicationMode))
        .Item("cache_upload_deduplication_threshold").Value(config.CacheUploadDeduplicationThreshold)
        .Item("mount_sandbox_in_tmpfs").Value(config.MountSandboxInTmpfs)
        .Item("api_file_path_options").Value(config.ApiFilePathOptions)
        .Item("use_abortable_response").Value(config.UseAbortableResponse)
        .Item("enable_debug_metrics").Value(config.EnableDebugMetrics)
        .Item("enable_local_mode_optimization").Value(config.EnableLocalModeOptimization)
        .Item("write_stderr_successful_jobs").Value(config.WriteStderrSuccessfulJobs)
        .Item("trace_http_requests_mode").Value(::ToString(config.TraceHttpRequestsMode))
        .Item("skynet_api_host").Value(config.SkynetApiHost)
        .DoIf(config.SocketPriority.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("socket_priority").Value(*config.SocketPriority);
        })
        .Item("commands_with_framing")
            .BeginList()
                .DoFor(config.CommandsWithFraming, [&](TFluentList list, const auto& command){
                    list.Item().Value(command);
                })
            .EndList()
        .Item("table_writer_version").Value(::ToString(config.TableWriterVersion))
        .Item("redirect_stdout_to_stderr").Value(config.RedirectStdoutToStderr)
        .Item("enable_debug_command_line_arguments").Value(config.EnableDebugCommandLineArguments)
        .Item("config_remote_patch_path").Value(config.ConfigRemotePatchPath)
    .EndMap();
}

void Deserialize(TConfig& config, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("hosts", config.Hosts);
    DESERIALIZE_ITEM("pool", config.Pool);
    DESERIALIZE_ITEM("prefix", config.Prefix);
    DESERIALIZE_ITEM("api_version", config.ApiVersion);
    DESERIALIZE_ITEM("log_level", config.LogLevel);
    DESERIALIZE_ITEM("log_path", config.LogPath);
    DESERIALIZE_ITEM("log_exclude_categories", config.LogExcludeCategories);
    DESERIALIZE_ITEM("structured_log", config.StructuredLog);
    DESERIALIZE_ITEM("http_proxy_role", config.HttpProxyRole);
    DESERIALIZE_ITEM("rpc_proxy_role", config.RpcProxyRole);
    DESERIALIZE_ITEM("proxy_url_aliasing_rules", config.ProxyUrlAliasingRules);
    DESERIALIZE_ITEM("log_use_core", config.LogUseCore);
    DESERIALIZE_ITEM("content_encoding", config.ContentEncoding);
    DESERIALIZE_ITEM("accept_encoding", config.AcceptEncoding);
    DESERIALIZE_ITEM("global_tx_id", config.GlobalTxId);
    DESERIALIZE_ITEM("force_ipv4", config.ForceIpV4);
    DESERIALIZE_ITEM("force_ipv6", config.ForceIpV6);
    DESERIALIZE_ITEM("use_hosts", config.UseHosts);
    DESERIALIZE_ITEM("host_list_update_interval", config.HostListUpdateInterval);
    DESERIALIZE_ITEM("spec", config.Spec);
    DESERIALIZE_ITEM("table_writer", config.TableWriter);
    DESERIALIZE_ITEM("connection_timeout", config.ConnectTimeout);
    DESERIALIZE_ITEM("socket_timeout", config.SocketTimeout);
    DESERIALIZE_ITEM("address_cache_expiration_timeout", config.AddressCacheExpirationTimeout);
    DESERIALIZE_ITEM("tx_timeout", config.TxTimeout);
    DESERIALIZE_ITEM("ping_timeout", config.PingTimeout);
    DESERIALIZE_ITEM("ping_interval", config.PingInterval);
    DESERIALIZE_ITEM("async_http_client_threads", config.AsyncHttpClientThreads);
    DESERIALIZE_ITEM("async_tx_pinger_pool_threads", config.AsyncTxPingerPoolThreads);
    DESERIALIZE_ITEM("wait_lock_poll_interval", config.WaitLockPollInterval);
    DESERIALIZE_ITEM("retry_interval", config.RetryInterval);
    DESERIALIZE_ITEM("chunk_errors_retry_interval", config.ChunkErrorsRetryInterval);
    DESERIALIZE_ITEM("rate_limit_exceeded_retry_interval", config.RateLimitExceededRetryInterval);
    DESERIALIZE_ITEM("start_operation_retry_interval", config.StartOperationRetryInterval);
    DESERIALIZE_ITEM("retry_count", config.RetryCount);
    DESERIALIZE_ITEM("read_retry_count", config.ReadRetryCount);
    DESERIALIZE_ITEM("start_operation_retry_count", config.StartOperationRetryCount);
    DESERIALIZE_ITEM("operation_tracker_poll_period", config.OperationTrackerPollPeriod);
    DESERIALIZE_ITEM("remote_temp_files_directory", config.RemoteTempFilesDirectory);
    DESERIALIZE_ITEM("remote_temp_tables_directory", config.RemoteTempTablesDirectory);
    DESERIALIZE_ITEM("keep_temp_tables", config.KeepTempTables);
    DESERIALIZE_ITEM("infer_table_schema", config.InferTableSchema);
    DESERIALIZE_ITEM("use_client_protobuf", config.UseClientProtobuf);
    DESERIALIZE_ITEM("node_reader_format", config.NodeReaderFormat);
    DESERIALIZE_ITEM("protobuf_format_with_descriptors", config.ProtobufFormatWithDescriptors);
    DESERIALIZE_ITEM("connection_pool_size", config.ConnectionPoolSize);
    DESERIALIZE_ITEM("file_cache_replication_factor", config.FileCacheReplicationFactor);
    DESERIALIZE_ITEM("cache_lock_timeout_per_gb", config.CacheLockTimeoutPerGb);
    DESERIALIZE_ITEM("cache_upload_deduplication_mode", config.CacheUploadDeduplicationMode);
    DESERIALIZE_ITEM("cache_upload_deduplication_threshold", config.CacheUploadDeduplicationThreshold);
    DESERIALIZE_ITEM("mount_sandbox_in_tmpfs", config.MountSandboxInTmpfs);
    DESERIALIZE_ITEM("api_file_path_options", config.ApiFilePathOptions);
    DESERIALIZE_ITEM("use_abortable_response", config.UseAbortableResponse);
    DESERIALIZE_ITEM("enable_debug_metrics", config.EnableDebugMetrics);
    DESERIALIZE_ITEM("enable_local_mode_optimization", config.EnableLocalModeOptimization);
    DESERIALIZE_ITEM("write_stderr_successful_jobs", config.WriteStderrSuccessfulJobs);
    DESERIALIZE_ITEM("trace_http_requests_mode", config.TraceHttpRequestsMode);
    DESERIALIZE_ITEM("skynet_api_host", config.SkynetApiHost);
    DESERIALIZE_ITEM("socket_priority", config.SocketPriority);
    DESERIALIZE_ITEM("commands_with_framing", config.CommandsWithFraming);
    DESERIALIZE_ITEM("table_writer_version", config.TableWriterVersion);
    DESERIALIZE_ITEM("redirect_stdout_to_stderr", config.RedirectStdoutToStderr);
    DESERIALIZE_ITEM("enable_debug_command_line_arguments", config.EnableDebugCommandLineArguments);
    DESERIALIZE_ITEM("config_remote_patch_path", config.ConfigRemotePatchPath);
}

#undef DESERIALIZE_ITEM

////////////////////////////////////////////////////////////////////////////////

TString ConfigToYsonString(const TConfig& config, NYson::EYsonFormat format)
{
    TNode configNode;
    TNodeBuilder builder(&configNode);
    Serialize(config, &builder);

    return NodeToYsonString(configNode, format);
}

TConfig ConfigFromYsonString(TString serializedConfig)
{
    TNode configNode = NodeFromYsonString(serializedConfig);
    TConfig config;
    Deserialize(config, configNode);
    return config;
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

    ClientVersion = ::TStringBuilder() << "YT C++ native " << GetProgramCommitId();
    BinaryPath = GetExecPath();
    BinaryName = GetBaseName(BinaryPath);
}

TProcessState* TProcessState::Get()
{
    return Singleton<TProcessState>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
