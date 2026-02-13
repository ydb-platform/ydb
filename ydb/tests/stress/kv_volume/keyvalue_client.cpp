#include "keyvalue_client.h"

#include "keyvalue_client_v1.h"
#include "keyvalue_client_v2.h"

#include <util/string/builder.h>

#include <stdexcept>

namespace NKvVolumeStress {

std::unique_ptr<IKeyValueClient> MakeKeyValueClient(const TString& hostPort, const TString& version) {
    if (version == "v1") {
        return std::make_unique<TKeyValueClientV1>(hostPort);
    }

    if (version == "v2") {
        return std::make_unique<TKeyValueClientV2>(hostPort);
    }

    throw std::runtime_error(TStringBuilder() << "unsupported --version: " << version);
}

} // namespace NKvVolumeStress
