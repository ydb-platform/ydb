#include <ydb/core/grpc_services/local_rate_limiter.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

#include <cstddef>
#include <cstdint>
#include <util/generic/hash.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 5) {
        return 0;
    }

    try {
        const size_t chunk = size / 5;
        TString database(reinterpret_cast<const char*>(data), chunk);
        TString coordinationNode(reinterpret_cast<const char*>(data + chunk), chunk);
        TString resourcePath(reinterpret_cast<const char*>(data + 2 * chunk), chunk);
        TString key(reinterpret_cast<const char*>(data + 3 * chunk), chunk);
        TString value(reinterpret_cast<const char*>(data + 4 * chunk), size - 4 * chunk);

        NKikimr::NRpcService::TRlConfig config(coordinationNode, resourcePath, {});
        THashMap<TString, TString> attrs;
        attrs[key] = value;

        auto rlPath = NKikimr::NRpcService::MakeRlPath(database, config, attrs);
        if (rlPath) {
            NKikimr::NRpcService::MakeRequests(config, *rlPath);
        }
    } catch (...) {}

    return 0;
}
