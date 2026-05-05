#include "dummy_tvm_client.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

class TDummyTvmClient : public ITvmClient {
public:
    TString GetServiceTicketFor(const TString& /*serviceAlias*/) override {
        YQL_ENSURE(false, "TVM client implementation is not present");
        return {};
    }
};

ITvmClient::TPtr CreateDummyTvmClient() {
    return MakeIntrusive<TDummyTvmClient>();
}

} // namespace NYql
