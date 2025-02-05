#include "yql_yt_fmr.h"

#include <yql/essentials/utils/log/profile.h>

#include <util/generic/ptr.h>

using namespace NThreading;

namespace NYql {

namespace {

class TFmrYtGateway final: public TYtForwardingGatewayBase {
public:
    TFmrYtGateway(IYtGateway::TPtr&& slave)
        : TYtForwardingGatewayBase(std::move(slave))
    {
    }
};

} // namespace

IYtGateway::TPtr CreateYtFmrGateway(IYtGateway::TPtr slave) {
    return MakeIntrusive<TFmrYtGateway>(std::move(slave));
}

} // namspace NYql
