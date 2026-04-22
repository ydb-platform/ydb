#pragma once

#include <util/generic/ptr.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NYql {

class ITvmClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITvmClient>;

    virtual TString GetServiceTicketFor(const TString& serviceAlias) = 0;
};

}; // namespace NYql
