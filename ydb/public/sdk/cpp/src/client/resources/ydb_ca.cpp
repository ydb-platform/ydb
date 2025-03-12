#include <library/cpp/resource/resource.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_ca.h>

namespace NYdb::inline Dev {

std::string GetRootCertificate() {
    return NResource::Find("ydb_root_ca_v3.pem");
}

} // namespace NYdb