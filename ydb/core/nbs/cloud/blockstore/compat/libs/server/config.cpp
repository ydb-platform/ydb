#include "config.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/system/sysstat.h>

namespace NCloud::NBlockStore::NServer {

namespace {

using TStrings = TVector<TString>;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MODE0660 = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;

constexpr TDuration Seconds(int s)
{
    return TDuration::Seconds(s);
}

////////////////////////////////////////////////////////////////////////////////
// clang-format off
#define BLOCKSTORE_SERVER_CONFIG(xxx)                                          \
    xxx(Host,                        TString,               "localhost"       )\
    xxx(Port,                        ui32,                  9766              )\
    xxx(DataHost,                    TString,               "localhost"       )\
    xxx(DataPort,                    ui32,                  9767              )\
    xxx(MaxMessageSize,              ui32,                  64*1024*1024      )\
    xxx(ThreadsCount,                ui32,                  1                 )\
    xxx(PreparedRequestsCount,       ui32,                  10                )\
    xxx(MemoryQuotaBytes,            ui32,                  0                 )\
    xxx(SecureHost,                  TString,               {}                )\
    xxx(SecurePort,                  ui32,                  0                 )\
    xxx(RootCertsFile,               TString,               {}                )\
    xxx(CertFile,                    TString,               {}                )\
    xxx(CertPrivateKeyFile,          TString,               {}                )\
    xxx(Certs,                       TVector<TCertificate>, {}                )\
    xxx(KeepAliveEnabled,            bool,                  false             )\
    xxx(KeepAliveIdleTimeout,        TDuration,             {}                )\
    xxx(KeepAliveProbeTimeout,       TDuration,             {}                )\
    xxx(KeepAliveProbesCount,        ui32,                  0                 )\
    xxx(StrictContractValidation,    bool,                  false             )\
    xxx(LoadCmsConfigs,              bool,                  false             )\
    xxx(ShutdownTimeout,             TDuration,             Seconds(30)       )\
    xxx(RequestTimeout,              TDuration,             Seconds(30)       )\
    xxx(UnixSocketPath,              TString,               {}                )\
    xxx(UnixSocketBacklog,           ui32,                  16                )\
    xxx(GrpcThreadsLimit,            ui32,                  4                 )\
    xxx(VhostEnabled,                bool,                  false             )\
    xxx(VhostThreadsCount,           ui32,                  1                 )\
    xxx(NvmfInitiatorEnabled,        bool,                  false             )\
    xxx(NodeType,                    TString,               {}                )\
    xxx(RootKeyringName,             TString,               "nbs"             )\
    xxx(EndpointsKeyringName,        TString,               {}                )\
    xxx(NbdEnabled,                  bool,                  false             )\
    xxx(NbdThreadsCount,             ui32,                  4                 )\
    xxx(NbdLimiterEnabled,           bool,                  false             )\
    xxx(MaxInFlightBytesPerThread,   ui64,                  128_MB            )\
    xxx(VhostAffinity,               TAffinity,             {}                )\
    xxx(NbdAffinity,                 TAffinity,             {}                )\
    xxx(NodeRegistrationMaxAttempts,    ui32,               10                )\
    xxx(NodeRegistrationTimeout,        TDuration,          Seconds(5)        )\
    xxx(NodeRegistrationErrorTimeout,   TDuration,          Seconds(1)        )\
    xxx(NbdSocketSuffix,             TString,               {}                )\
    xxx(GrpcKeepAliveTime,           ui32,                  7200000           )\
    xxx(GrpcKeepAliveTimeout,        ui32,                  20000             )\
    xxx(GrpcKeepAlivePermitWithoutCalls,    bool,           false             )\
    xxx(GrpcHttp2MinRecvPingIntervalWithoutData,    ui32,   300000            )\
    xxx(GrpcHttp2MinSentPingIntervalWithoutData,    ui32,   300000            )\
    xxx(NVMeEndpointEnabled,         bool,                  false             )\
    xxx(NVMeEndpointNqn,             TString,               {}                )\
    xxx(NVMeEndpointTransportIDs,    TStrings,              {}                )\
    xxx(SCSIEndpointEnabled,         bool,                  false             )\
    xxx(SCSIEndpointName,            TString,               {}                )\
    xxx(SCSIEndpointListenAddress,   TString,               {}                )\
    xxx(SCSIEndpointListenPort,      ui32,                  0                 )\
    xxx(RdmaEndpointEnabled,         bool,                  false             )\
    xxx(RdmaEndpointListenAddress,   TString,               {}                )\
    xxx(RdmaEndpointListenPort,      ui32,                  0                 )\
    xxx(ThrottlingEnabled,           bool,                  false             )\
    xxx(MaxReadBandwidth,            ui64,                  0                 )\
    xxx(MaxWriteBandwidth,           ui64,                  0                 )\
    xxx(MaxReadIops,                 ui32,                  0                 )\
    xxx(MaxWriteIops,                ui32,                  0                 )\
    xxx(MaxBurstTime,                TDuration,             Seconds(0)        )\
    xxx(RdmaClientEnabled,           bool,                  false             )\
    xxx(UseFakeRdmaClient,           bool,                  false             )\
    xxx(DisableClientThrottlers,     bool,                  false             )\
    xxx(EndpointStorageType,                                                   \
        NCloud::NProto::EEndpointStorageType,                                  \
        NCloud::NProto::ENDPOINT_STORAGE_KEYRING                              )\
    xxx(EndpointStorageDir,          TString,               {}                )\
    xxx(VhostServerPath,             TString,               {}                )\
    xxx(NbdDevicePrefix,             TString,               "/dev/nbd"        )\
    xxx(SocketAccessMode,            ui32,                  MODE0660          )\
    xxx(NbdNetlink,                  bool,                  false             )\
    xxx(NbdRequestTimeout,           TDuration,             Seconds(600)      )\
    xxx(NbdConnectionTimeout,        TDuration,             Seconds(86400)    )\
    xxx(AllowAllRequestsViaUDS,      bool,                  false             )\
    xxx(NodeRegistrationToken,       TString,               "root@builtin"    )\
    xxx(EndpointStorageNotImplementedErrorIsFatal,  bool,   false             )\
    xxx(VhostServerTimeoutAfterParentExit, TDuration,       Seconds(60)       )\
    xxx(ChecksumFlags,               NProto::TChecksumFlags, {}               )\
    xxx(VhostDiscardEnabled,         bool,                   false            )\
    xxx(VhostDiscardOnlyEnabled,     bool,                   false            )\
    xxx(VhostWriteZeroesEnabled,     bool,                   false            )\
    xxx(DropDiscardRequests,         bool,                   false            )\
    xxx(VhostOptimalIoSize,          ui32,                   0                )\
    xxx(MaxZeroBlocksSubRequestSize, ui32,                   0                )\
    xxx(EncryptZeroPolicy,                                                     \
        NProto::EEncryptZeroPolicy,                                            \
        NProto::EZP_WRITE_ENCRYPTED_ZEROS                                     )\
    xxx(VhostPteFlushByteThreshold,  ui64,                   0                )\
    xxx(AutomaticNbdDeviceManagement,bool,                   false            )\
    xxx(EnableOverlappingRequestsGuard,  bool,               false            )\
    xxx(EnableRequestSplitter,       bool,                   false            )\
    xxx(ExternalVhostServerThreadPoolSize, ui64,             0                )\
    xxx(RefreshCertsPeriod,          TDuration,              Seconds(0)       )\
// BLOCKSTORE_SERVER_CONFIG

// clang-format on

#define BLOCKSTORE_SERVER_DECLARE_CONFIG(name, type, value)                    \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_SERVER_DECLARE_CONFIG

BLOCKSTORE_SERVER_CONFIG(BLOCKSTORE_SERVER_DECLARE_CONFIG)

#undef BLOCKSTORE_SERVER_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TDuration ConvertValue<TDuration, ui32>(const ui32& value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    return { value.begin(), value.end() };
}

template <>
TVector<TCertificate> ConvertValue(
    const google::protobuf::RepeatedPtrField<NCloud::NProto::TCertificate>& value)
{
    TVector<TCertificate> v;
    for (const auto& x: value) {
        v.push_back({x.GetCertFile(), x.GetCertPrivateKeyFile()});
    }
    return v;
}

template <>
TAffinity ConvertValue<TAffinity, NProto::TAffinity>(
    const NProto::TAffinity& value)
{
    TVector<ui8> vec(value.GetCPU().begin(), value.GetCPU().end());
    return TAffinity(std::move(vec));
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

template <>
void DumpImpl(const TVector<TString>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << value[i];
    }
}

template <>
void DumpImpl(const TVector<TCertificate>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os
          << "{ "
          << value[i].CertFile
          << ", "
          << value[i].CertPrivateKeyFile
          << " }";
    }
}

template <>
void DumpImpl(const TAffinity& value, IOutputStream& os)
{
    const auto& cores = value.GetCores();

    for (size_t i = 0; i < cores.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << cores[i];
    }
}

template <>
void DumpImpl(
    const NCloud::NProto::EEndpointStorageType& value,
    IOutputStream& os)
{
    switch (value) {
        case NCloud::NProto::ENDPOINT_STORAGE_DEFAULT:
            os << "ENDPOINT_STORAGE_DEFAULT";
            break;
        case NCloud::NProto::ENDPOINT_STORAGE_KEYRING:
            os << "ENDPOINT_STORAGE_KEYRING";
            break;
        case NCloud::NProto::ENDPOINT_STORAGE_FILE:
            os << "ENDPOINT_STORAGE_FILE";
            break;
        default:
            os << "(Unknown EEndpointStorageType value "
                << static_cast<int>(value)
                << ")";
            break;
    }
}

template <>
void DumpImpl(const NProto::EEncryptZeroPolicy& value, IOutputStream& os)
{
    os << NProto::EEncryptZeroPolicy_Name(value);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TServerAppConfig::TServerAppConfig(NProto::TServerAppConfig appConfig)
    : AppConfig(std::move(appConfig))
{
    ServerConfig = &AppConfig.GetServerConfig();

    if (AppConfig.HasKikimrServiceConfig()) {
        KikimrServiceConfig = &AppConfig.GetKikimrServiceConfig();
    }

    if (AppConfig.HasLocalServiceConfig()) {
        LocalServiceConfig = &AppConfig.GetLocalServiceConfig();
    }

    if (AppConfig.HasNullServiceConfig()) {
        NullServiceConfig = &AppConfig.GetNullServiceConfig();
    }
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TServerAppConfig::Get##name() const                                       \
{                                                                              \
    return NYdb::NBS::HasField(*ServerConfig, #name)                           \
                ? ConvertValue<type>(ServerConfig->Get##name())                \
                : Default##name;                                               \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_SERVER_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TServerAppConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_SERVER_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TServerAppConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_SERVER_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

bool TServerAppConfig::DeprecatedGetRdmaClientEnabled() const
{
    return GetRdmaClientEnabled();
}

const ::NCloud::NProto::TRdmaClient&
TServerAppConfig::DeprecatedGetRdmaClientConfig() const
{
    return ServerConfig->GetRdmaClientConfig();
}

TVector<TCertificate> TServerAppConfig::GetCertsWithLegacyFallback() const
{
    auto certs = GetCerts();
    if (!certs.empty()) {
        return certs;
    }

    certs.push_back({GetCertFile(), GetCertPrivateKeyFile()});

    return certs;
}

}   // namespace NCloud::NBlockStore::NServer
