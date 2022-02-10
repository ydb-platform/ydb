#include "load_arcadia_root_certs.h"

#include <library/cpp/resource/resource.h> 

namespace grpc_core {
    grpc_slice LoadArcadiaRootCerts() {
        TString cacert = NResource::Find("/builtin/cacert");
        return grpc_slice_from_copied_buffer(cacert.data(), cacert.size() + 1); // With \0.
    }
}
