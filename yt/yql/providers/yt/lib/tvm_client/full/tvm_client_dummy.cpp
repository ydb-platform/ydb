#include "tvm_client.h"

#include <yt/yql/providers/yt/lib/tvm_client/dummy/dummy_tvm_client.h>

namespace NYql {

Y_WEAK ITvmClient::TPtr CreateTvmClient(const TYtTvmConfig& /*config*/) {
    return CreateDummyTvmClient();
}

} // namespace NYql
