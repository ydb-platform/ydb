#include <library/cpp/resource/resource.h>

#include <ydb-cpp-sdk/client/resources/ydb_ca.h>

namespace NYdb {

std::string GetRootCertificate() {
    return NResource::Find("ydb_root_ca.pem");
}

} // namespace NYdb