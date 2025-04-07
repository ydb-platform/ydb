#include <library/cpp/resource/resource.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_ca.h>

#include <ydb/public/sdk/cpp/src/version.h>

namespace NYdb::inline Dev {

std::string GetRootCertificate() {
    return NResource::Find(YDB_CERTIFICATE_FILE_KEY);
}

} // namespace NYdb
