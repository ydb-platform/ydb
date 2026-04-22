#include "yt_access_provider.h"

#include <yt/yql/providers/yt/lib/access_provider/dummy/yt_dummy_access_provider.h>

namespace NYql {

Y_WEAK IYtAccessProvider::TPtr CreateYtAccessProvider(const ITvmClient::TPtr& /*tvmClient*/, const TYtGatewayConfig& /*ytGatewayConfig*/) {
    return CreateYtDummyAccessProvider();
}

}; // namespace NYql
