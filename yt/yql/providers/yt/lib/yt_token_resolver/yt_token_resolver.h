#pragma once

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>


namespace NYql {

class IYtTokenResolver: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IYtTokenResolver>;

    virtual TMaybe<TString> ResolveClusterToken(const TString& cluster) = 0;
};

} // namespace NYql
