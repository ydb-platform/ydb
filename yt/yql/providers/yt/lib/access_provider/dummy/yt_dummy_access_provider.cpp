#include "yt_dummy_access_provider.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

class TYtDummyAccessProvider : public IYtAccessProvider {
public:
    void RequestAccess(
        TStringBuf /*ytCluster*/,
        EIdentityType /*type*/,
        TStringBuf /*path*/,
        TStringBuf /*requester*/,
        const TYqlOperationOptions& /*operationOptions*/) override
    {
        YQL_ENSURE(false, "YT access provider implementation is not present");
    }
};

IYtAccessProvider::TPtr CreateYtDummyAccessProvider() {
    return MakeIntrusive<TYtDummyAccessProvider>();
}

}; // namespace NYql
