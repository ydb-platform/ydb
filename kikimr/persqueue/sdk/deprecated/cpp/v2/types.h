#pragma once
#include "credentials_provider.h"
#include "logger.h"
#include <kikimr/yndx/api/protos/persqueue.pb.h>


#include <library/cpp/logger/priority.h>

#include <util/generic/string.h>
#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/datetime/base.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#include <grpc++/channel.h>

#include <limits>

namespace NPersQueue {
using ECodec = NPersQueueCommon::ECodec;
using TCredentials = NPersQueueCommon::TCredentials;
using TError = NPersQueueCommon::TError;

using TProducerSeqNo = ui64; // file offset or something with equivalent meaning.
using TConsumerOffset = i64; // partition offset. Have special value -1. All other values are >= 0.
using TCredProviderPtr = std::shared_ptr<ICredentialsProvider>;

constexpr ui32 DEFAULT_MEMORY_USAGE = 100 << 20; //100mb
constexpr ui32 DEFAULT_READS_INFLY = 10;
constexpr ui32 DEFAULT_BATCH_SIZE = 1 << 20; //1mb
constexpr TDuration DEFAULT_CHANNEL_CREATION_TIMEOUT = TDuration::Seconds(10);
constexpr TDuration DEFAULT_RECONNECTION_DELAY = TDuration::MilliSeconds(10);
constexpr TDuration DEFAULT_MAX_RECONNECTION_DELAY = TDuration::Seconds(1);
constexpr TDuration DEFAULT_START_SESSION_TIMEOUT = TDuration::Seconds(30);
constexpr TDuration DEFAULT_CHECK_TOKEN_PERIOD = TDuration::Hours(1);
inline const THashMap<ECodec, TString> CodecIdByType{{ECodec::DEFAULT, TString(1, '\0')}, {ECodec::RAW, TString(1, '\0')}, {ECodec::GZIP, TString(1, '\1')}, {ECodec::LZOP, TString(1, '\2')}, {ECodec::ZSTD, TString(1, '\3')}};

struct TPQLibSettings {
    size_t ThreadsCount = 1;
    size_t CompressionPoolThreads = 2;
    size_t GRpcThreads = 1;
    TDuration ChannelCreationTimeout = DEFAULT_CHANNEL_CREATION_TIMEOUT;
    TIntrusivePtr<ILogger> DefaultLogger = {};

    bool EnableGRpcV1 = std::getenv("PERSQUEUE_GRPC_API_V1_ENABLED");
};

enum class EClusterDiscoveryUsageMode {
    Auto,
    Use,
    DontUse,
};

// destination server
struct TServerSetting {
    // Endpoint of logbroker cluster to connect.
    // Possible variants:
    //
    // 1. lbkx cluster.
    // Set address to lbkx.logbroker.yandex.net
    //
    // 2. logbroker federation.
    // If you want to write/read to/from concrete cluster
    // (with ReadMirroredPartitions or not, but without ReadFromAllClusterSources option),
    // use the following addresses:
    // iva.logbroker.yandex.net
    // man.logbroker.yandex.net
    // myt.logbroker.yandex.net
    // sas.logbroker.yandex.net
    // vla.logbroker.yandex.net
    //
    // If you create consumer with ReadFromAllClusterSources option,
    // use logbroker.yandex.net
    TString Address;

    ui16 Port = 2135;
    TString Database = GetRootDatabase();
    EClusterDiscoveryUsageMode UseLogbrokerCDS = EClusterDiscoveryUsageMode::Auto;
    bool UseSecureConnection = false;
    TString CaCert = TString();

    TServerSetting(TString address, ui16 port = 2135)
        : Address(std::move(address))
        , Port(port)
    {
        Y_VERIFY(!Address.empty());
        Y_VERIFY(Port > 0);
    }

    TServerSetting() = default;
    TServerSetting(const TServerSetting&) = default;
    TServerSetting(TServerSetting&&) = default;

    TServerSetting& operator=(const TServerSetting&) = default;
    TServerSetting& operator=(TServerSetting&&) = default;

    TString GetFullAddressString() const {
        return TStringBuilder() << Address << ':' << Port;
    }
    void EnableSecureConnection(const TString& caCert) {
        UseSecureConnection = true;
        CaCert = caCert;
    }
    void DisableSecureConnection() {
        UseSecureConnection = false;
        CaCert.clear();
    }

    const TString& GetRootDatabase();

    bool operator < (const TServerSetting& rhs) const {
        return std::tie(Address, Port, Database, UseSecureConnection) < std::tie(rhs.Address, rhs.Port, rhs.Database, rhs.UseSecureConnection);
    }
};

//
// consumer/producer settings
//

struct TProducerSettings {
    TServerSetting Server;
    std::shared_ptr<ICredentialsProvider> CredentialsProvider = CreateInsecureCredentialsProvider();

    TString Topic;
    TString SourceId;

    ui32 PartitionGroup = 0;
    ECodec Codec = ECodec::GZIP;
    int Quality = -1;
    THashMap<TString, TString> ExtraAttrs;

    // Should we reconnect when session breaks?
    bool ReconnectOnFailure = false;

    //
    // Setting for reconnection
    //

    // In case of lost connection producer will try
    // to reconnect and resend data <= MaxAttempts times.
    // Then producer will be dead.
    unsigned MaxAttempts = std::numeric_limits<unsigned>::max();

    // Time delay before next producer reconnection attempt.
    // The second attempt will be in 2 * ReconnectionDelay,
    // the third - in 4 * ReconnectionDelay, etc. Maximum value
    // of delay is MaxReconnectionDelay.
    TDuration ReconnectionDelay = DEFAULT_RECONNECTION_DELAY;
    TDuration MaxReconnectionDelay = DEFAULT_MAX_RECONNECTION_DELAY;

    // Timeout for session initialization operation.
    TDuration StartSessionTimeout = DEFAULT_START_SESSION_TIMEOUT;
    // Producer will check token from 'ICredentialsProvider' with this period and if changed - send to server as soon as possible.
    // Available only with gRPC data plane API v1.
    TDuration CheckTokenPeriod = DEFAULT_CHECK_TOKEN_PERIOD;

    // Preferred cluster to write.
    // This setting is used only in case of cluster discovery is used.
    // Case insensitive.
    // If PreferredCluster is not available,
    // producer will connect to other cluster.
    TString PreferredCluster;

    bool DisableCDS = false; // only for grpcv1 mode
};

struct TMultiClusterProducerSettings {
    struct TServerWeight {
        // Setting of producer connected to concrete DC.
        // ReconnectOnFailure must be true.
        // MaxAttempts must be a finite number.
        // SourceId will be complemented with source id prefix and producer type. Can be empty.
        // There is no guarantee that messages will arrive in the order of writes in case of one DC is absent.
        TProducerSettings ProducerSettings;
        unsigned Weight;

        explicit TServerWeight(TProducerSettings producerSettings, unsigned weight = 1)
            : ProducerSettings(std::move(producerSettings))
            , Weight(weight)
        {
            Y_VERIFY(Weight > 0);
        }

        TServerWeight() = default;
        TServerWeight(const TServerWeight&) = default;
        TServerWeight(TServerWeight&&) = default;

        TServerWeight& operator=(const TServerWeight&) = default;
        TServerWeight& operator=(TServerWeight&&) = default;
    };

    std::vector<TServerWeight> ServerWeights;
    size_t MinimumWorkingDcsCount = 1; // Minimum Dcs to start and not to die.
    TString SourceIdPrefix; // There is no guarantee that messages will arrive in the order of writes in case of one DC is absent.
};

struct TConsumerSettings {
    TVector<TString> Topics;
    bool ReadMirroredPartitions = true;

    // Discovers all clusters where topic is presented and create consumer that reads from all of them.
    // Conflicts with ReadMirroredPartitions option (should be set to "no").
    // Will ignore the following options:
    // ReconnectOnFailure (forced to "yes")
    // MaxAttempts (forced to infinity == std::numeric_limits<unsigned>::max())
    // Server.UseLogbrokerCDS (forced to "Use")
    //
    // Also see description of option ReconnectOnFailure.
    bool ReadFromAllClusterSources = false;

    TServerSetting Server;
    std::shared_ptr<ICredentialsProvider> CredentialsProvider = CreateInsecureCredentialsProvider();
    TString ClientId;

    bool UseLockSession = false;
    ui32 PartsAtOnce = 0;

    bool Unpack = true;
    bool SkipBrokenChunks = false;

    ui64 MaxMemoryUsage = DEFAULT_MEMORY_USAGE;
    ui32 MaxInflyRequests = DEFAULT_READS_INFLY;

    // read settings
    ui32 MaxCount = 0; // zero means unlimited
    ui32 MaxSize = DEFAULT_BATCH_SIZE; // zero means unlimited
    ui32 MaxTimeLagMs = 0; // zero means unlimited
    ui64 ReadTimestampMs = 0; // read data from this timestamp

    ui32 MaxUncommittedCount = 0; // zero means unlimited
    ui64 MaxUncommittedSize = 0; // zero means unlimited

    TVector<ui32> PartitionGroups; // groups to read

    bool PreferLocalProxy = false;
    bool BalanceRightNow = false;
    bool CommitsDisabled = false;

    // Should we reconnect when session breaks?
    //
    // Keep in mind that it is possible to receive
    // message duplicates when reconnection occures.
    bool ReconnectOnFailure = false;

    //
    // Settings for reconnection
    //

    // In case of lost connection consumer will try
    // to reconnect and resend requests <= MaxAttempts times.
    // Then consumer will be dead.
    unsigned MaxAttempts = std::numeric_limits<unsigned>::max();

    // Time delay before next consumer reconnection attempt.
    // The second attempt will be in 2 * ReconnectionDelay,
    // the third - in 3 * ReconnectionDelay, etc. Maximum value
    // of delay is MaxReconnectionDelay.
    TDuration ReconnectionDelay = DEFAULT_RECONNECTION_DELAY;
    TDuration MaxReconnectionDelay = DEFAULT_MAX_RECONNECTION_DELAY;
    TDuration StartSessionTimeout = DEFAULT_START_SESSION_TIMEOUT;

    bool DisableCDS = false; // only for grpcv1 mode

    bool UseV2RetryPolicyInCompatMode = false;
};

struct TProcessorSettings {
    ui64 MaxOriginDataMemoryUsage;
    ui64 MaxProcessedDataMemoryUsage;
    TDuration SourceIdIdleTimeout;

    std::shared_ptr<ICredentialsProvider> CredentialsProvider;

    //UseLockSession parameter will be ignored
    TConsumerSettings ConsumerSettings;

    //Topic and SourceId pararameters will be ignored
    TProducerSettings ProducerSettings;

    TProcessorSettings()
        : MaxOriginDataMemoryUsage(DEFAULT_MEMORY_USAGE)
        , MaxProcessedDataMemoryUsage(DEFAULT_MEMORY_USAGE)
        , SourceIdIdleTimeout(TDuration::Hours(1))
        , CredentialsProvider(CreateInsecureCredentialsProvider())
    {
        ConsumerSettings.CredentialsProvider = ProducerSettings.CredentialsProvider = CredentialsProvider;
    }
};

class TData {
public:
    // empty: not for passing to producer
    TData() = default;

    // implicit constructor from source data to be encoded later by producer with default codec from settings
    // or explicitly specified one
    TData(TString sourceData, ECodec codec = ECodec::DEFAULT, TInstant timestamp = TInstant::Now())
        : TData(timestamp, sourceData, codec == ECodec::RAW ? sourceData : TString(), codec, sourceData.size())
    {
        Y_VERIFY(!SourceData.empty());
        Y_VERIFY(timestamp != TInstant::Zero());
    }

    TData(TString sourceData, TInstant timestamp)
        : TData(std::move(sourceData), ECodec::DEFAULT, timestamp)
    {
    }

    // construct already encoded TData
    static TData Encoded(TString encodedData, ECodec codec, TInstant timestamp = TInstant::Now(), ui32 originalSize = 0) {
        Y_VERIFY(!encodedData.empty());
        Y_VERIFY(codec != ECodec::RAW && codec != ECodec::DEFAULT);
        Y_VERIFY(timestamp != TInstant::Zero());
        return TData(timestamp, TString(encodedData), encodedData, codec, originalSize == 0 ? encodedData.size() * 10 : originalSize);
    }

    // construct user defined raw data that shouldn't be encoded by producer
    static TData Raw(TString rawData, TInstant timestamp = TInstant::Now()) {
        Y_VERIFY(!rawData.empty());
        Y_VERIFY(timestamp != TInstant::Zero());
        return TData(timestamp, rawData, rawData, ECodec::RAW, rawData.size());
    }

    TData(const TData&) = default;
    TData(TData&&) = default;

    TData& operator=(const TData&) = default;
    TData& operator=(TData&&) = default;

    ECodec GetCodecType() const {
        return Codec;
    }

    bool IsEncoded() const {
        return !EncodedData.empty();
    }

    const TString& GetEncodedData() const {
        Y_VERIFY(IsEncoded());
        return EncodedData;
    }

    // gets some data for error report
    const TString& GetSourceData() const {
        return SourceData;
    }

    TInstant GetTimestamp() const {
        return Timestamp;
    }

    bool Empty() const {
        return GetTimestamp() == TInstant::Zero();
    }

    // encoding of data by default codec or data codec that is not encoded
    // if data's codec is default, uses defaultCodec parameter,
    // otherwise uses data's codec
    static TData Encode(TData data, ECodec defaultCodec, int quality = -1);

    ui32 GetOriginalSize() const {
        return OriginalSize;
    }

    // special function to make raw enconding that doesn't need cpu time.
    static TData MakeRawIfNotEncoded(TData data);

    bool operator==(const TData& data) const {
        return Codec == data.Codec
            && Timestamp == data.Timestamp
            && SourceData == data.SourceData
            && EncodedData == data.EncodedData;
    }

    bool operator!=(const TData& data) const {
        return !operator==(data);
    }

private:
    TData(TInstant timestamp, TString sourceData, TString encodedData, ECodec codec, ui32 originalSize)
        : Timestamp(timestamp)
        , SourceData(std::move(sourceData))
        , EncodedData(std::move(encodedData))
        , Codec(codec)
        , OriginalSize(originalSize)
    {
    }

    static THolder<IOutputStream> CreateCoder(ECodec codec, TData& data, int quality);

private:
    TInstant Timestamp;
    TString SourceData;
    TString EncodedData;
    ECodec Codec = ECodec::DEFAULT;
    ui32 OriginalSize = 0;
};

}
