#pragma once

#include <util/generic/ptr.h>

namespace NYql {

class ITvmClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITvmClient>;

    virtual TString GetServiceTicketFor(const TString& serviceAlias) = 0;
};

}; // namespace NYql
