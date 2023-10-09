#include "session_config.h"

#include <util/generic/strbuf.h>
#include <util/string/hex.h>

using namespace NBus;

TBusSessionConfig::TSecret::TSecret()
    : TimeoutPeriod(TDuration::Seconds(1))
    , StatusFlushPeriod(TDuration::MilliSeconds(400))
{
}

TBusSessionConfig::TBusSessionConfig()
    : BUS_SESSION_CONFIG_MAP(STRUCT_FIELD_INIT, COMMA)
{
}

TString TBusSessionConfig::PrintToString() const {
    TStringStream ss;
    BUS_SESSION_CONFIG_MAP(STRUCT_FIELD_PRINT, )
    return ss.Str();
}

static int ParseDurationForMessageBus(const char* option) {
    return TDuration::Parse(option).MilliSeconds();
}

static int ParseToSForMessageBus(const char* option) {
    int tos;
    TStringBuf str(option);
    if (str.StartsWith("0x")) {
        str = str.Tail(2);
        Y_ABORT_UNLESS(str.length() == 2, "ToS must be a number between 0x00 and 0xFF");
        tos = String2Byte(str.data());
    } else {
        tos = FromString<int>(option);
    }
    Y_ABORT_UNLESS(tos >= 0 && tos <= 255, "ToS must be between 0x00 and 0xFF");
    return tos;
}

template <class T>
static T ParseWithKmgSuffixT(const char* option) {
    TStringBuf str(option);
    T multiplier = 1;
    if (str.EndsWith('k')) {
        multiplier = 1024;
        str = str.Head(str.size() - 1);
    } else if (str.EndsWith('m')) {
        multiplier = 1024 * 1024;
        str = str.Head(str.size() - 1);
    } else if (str.EndsWith('g')) {
        multiplier = 1024 * 1024 * 1024;
        str = str.Head(str.size() - 1);
    }
    return FromString<T>(str) * multiplier;
}

static ui64 ParseWithKmgSuffix(const char* option) {
    return ParseWithKmgSuffixT<ui64>(option);
}

static i64 ParseWithKmgSuffixS(const char* option) {
    return ParseWithKmgSuffixT<i64>(option);
}

void TBusSessionConfig::ConfigureLastGetopt(NLastGetopt::TOpts& opts,
                                            const TString& prefix) {
    opts.AddLongOption(prefix + "total-timeout")
        .RequiredArgument("MILLISECONDS")
        .DefaultValue(ToString(TotalTimeout))
        .StoreMappedResultT<const char*>(&TotalTimeout,
                                         &ParseDurationForMessageBus);
    opts.AddLongOption(prefix + "connect-timeout")
        .RequiredArgument("MILLISECONDS")
        .DefaultValue(ToString(ConnectTimeout))
        .StoreMappedResultT<const char*>(&ConnectTimeout,
                                         &ParseDurationForMessageBus);
    opts.AddLongOption(prefix + "send-timeout")
        .RequiredArgument("MILLISECONDS")
        .DefaultValue(ToString(SendTimeout))
        .StoreMappedResultT<const char*>(&SendTimeout,
                                         &ParseDurationForMessageBus);
    opts.AddLongOption(prefix + "send-threshold")
        .RequiredArgument("BYTES")
        .DefaultValue(ToString(SendThreshold))
        .StoreMappedResultT<const char*>(&SendThreshold, &ParseWithKmgSuffix);

    opts.AddLongOption(prefix + "max-in-flight")
        .RequiredArgument("COUNT")
        .DefaultValue(ToString(MaxInFlight))
        .StoreMappedResultT<const char*>(&MaxInFlight, &ParseWithKmgSuffix);
    opts.AddLongOption(prefix + "max-in-flight-by-size")
        .RequiredArgument("BYTES")
        .DefaultValue(
            ToString(MaxInFlightBySize))
        .StoreMappedResultT<const char*>(&MaxInFlightBySize, &ParseWithKmgSuffixS);
    opts.AddLongOption(prefix + "per-con-max-in-flight")
        .RequiredArgument("COUNT")
        .DefaultValue(ToString(PerConnectionMaxInFlight))
        .StoreMappedResultT<const char*>(&PerConnectionMaxInFlight,
                                         &ParseWithKmgSuffix);
    opts.AddLongOption(prefix + "per-con-max-in-flight-by-size")
        .RequiredArgument("BYTES")
        .DefaultValue(
            ToString(PerConnectionMaxInFlightBySize))
        .StoreMappedResultT<const char*>(&PerConnectionMaxInFlightBySize,
                                         &ParseWithKmgSuffix);

    opts.AddLongOption(prefix + "default-buffer-size")
        .RequiredArgument("BYTES")
        .DefaultValue(ToString(DefaultBufferSize))
        .StoreMappedResultT<const char*>(&DefaultBufferSize,
                                         &ParseWithKmgSuffix);
    opts.AddLongOption(prefix + "max-buffer-size")
        .RequiredArgument("BYTES")
        .DefaultValue(ToString(MaxBufferSize))
        .StoreMappedResultT<const char*>(&MaxBufferSize, &ParseWithKmgSuffix);
    opts.AddLongOption(prefix + "max-message-size")
        .RequiredArgument("BYTES")
        .DefaultValue(ToString(MaxMessageSize))
        .StoreMappedResultT<const char*>(&MaxMessageSize, &ParseWithKmgSuffix);
    opts.AddLongOption(prefix + "socket-recv-buffer-size")
        .RequiredArgument("BYTES")
        .DefaultValue(ToString(SocketRecvBufferSize))
        .StoreMappedResultT<const char*>(&SocketRecvBufferSize,
                                         &ParseWithKmgSuffix);
    opts.AddLongOption(prefix + "socket-send-buffer-size")
        .RequiredArgument("BYTES")
        .DefaultValue(ToString(SocketSendBufferSize))
        .StoreMappedResultT<const char*>(&SocketSendBufferSize,
                                         &ParseWithKmgSuffix);

    opts.AddLongOption(prefix + "socket-tos")
        .RequiredArgument("[0x00, 0xFF]")
        .StoreMappedResultT<const char*>(&SocketToS, &ParseToSForMessageBus);
    ;
    opts.AddLongOption(prefix + "tcp-cork")
        .RequiredArgument("BOOL")
        .DefaultValue(ToString(TcpCork))
        .StoreResult(&TcpCork);
    opts.AddLongOption(prefix + "cork")
        .RequiredArgument("SECONDS")
        .DefaultValue(
            ToString(Cork.Seconds()))
        .StoreMappedResultT<const char*>(&Cork, &TDuration::Parse);

    opts.AddLongOption(prefix + "on-message-in-pool")
        .RequiredArgument("BOOL")
        .DefaultValue(ToString(ExecuteOnMessageInWorkerPool))
        .StoreResult(&ExecuteOnMessageInWorkerPool);
    opts.AddLongOption(prefix + "on-reply-in-pool")
        .RequiredArgument("BOOL")
        .DefaultValue(ToString(ExecuteOnReplyInWorkerPool))
        .StoreResult(&ExecuteOnReplyInWorkerPool);
}
