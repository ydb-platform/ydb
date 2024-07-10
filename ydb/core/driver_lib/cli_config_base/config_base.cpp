#include <ydb/core/util/address_classifier.h>

#include "config_base.h"

namespace NKikimr {

TCommandConfig CommandConfig;

TDuration ParseDuration(const TStringBuf& str) {
    TDuration result;
    if (TDuration::TryParse(str, result))
        return result;
    ythrow yexception() << "wrong TDuration format";
}

TCommandConfig::TServerEndpoint TCommandConfig::ParseServerAddress(const TString& address) {
    TServerEndpoint endpoint = {EServerType::GRpc, address};
    if (address.empty()) {
        endpoint.Address = "localhost";
    }
    endpoint.ServerType = EServerType::GRpc;
    if (address.StartsWith("grpc://")) {
        endpoint.Address = endpoint.Address.substr(7);
        endpoint.EnableSsl = false;
    } else if (address.StartsWith("grpcs://")) {
        endpoint.Address = endpoint.Address.substr(8);
        endpoint.EnableSsl = true;
    }
    TString hostname;
    ui32 port = 2135; // default
    NKikimr::NAddressClassifier::ParseAddress(endpoint.Address, hostname, port);
    endpoint.Address = hostname + ':' + ToString(port);
    return endpoint;
}

const TString ArgFormatDescription() {
    return R"___(
Common option formats:
  NUM       - integer number, bounds should be deducted from a command context
  STR       - string, format should be deducted from a command context
  ADDR      - network address, e.g. kikimr0000.search.yandex.net (or 5.45.222.113, 2a02:6b8:b000:2000::52d:de71), localhost (or 127.0.0.1, ::1)
  PATH      - filesystem path, e.g. /Berkanavt/kikimr/bin or directory/file.txt
  BYTES     - integer size in bytes
              possible suffixes:
                * k|K - kilobytes ( x 2^10 )
                * m|M - megabytes ( x 2^20 )
                * g|G - gigabytes ( x 2^30 )
                * t|T - terabytes ( x 2^40 )
              examples: 1024, 4k, 100M, 2G, 1T
  TIME      - floating point duration in seconds
              possible suffixes:
                * s  - seconds      ( x 1     )
                * ms - milliseconds ( x 10^-3 )
                * us - microseconds ( x 10^-6 )
                * ns - nanoseconds  ( x 10^-9 )
                * m  - minutes      ( x 60    )
                * h  - hours        ( x 3600  )
                * d  - days         ( x 86400 )
              precision: 1us
              examples: 1.5, 1.5s, 10.5ms, 100us, 1500ns, 30m, 2.5h, 7d
    )___";
}

} // namespace NKikimr
