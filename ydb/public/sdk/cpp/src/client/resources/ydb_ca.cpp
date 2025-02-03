#include <library/cpp/resource/resource.h>

#include <ydb-cpp-sdk/client/resources/ydb_ca.h>

namespace NYdb::inline V3 {

std::string GetRootCertificate() {
    return NResource::Find("ydb_root_ca_v3.pem");
}

} // namespace NYdb