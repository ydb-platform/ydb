#include <library/cpp/resource/resource.h>

#include "ydb_ca.h"

namespace NYdb {

TString GetRootCertificate() {
    return NResource::Find("ydb_root_ca.pem");
}

} // namespace NYdb