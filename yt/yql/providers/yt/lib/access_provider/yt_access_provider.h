#pragma once

#include <yt/yql/providers/yt/lib/tvm_client/tvm_client.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/datetime/base.h>

namespace NYql {

enum class EIdentityType {
    User        /* "user" */,
    YqlProject  /* "yql_project" */,
};

class IYtAccessProvider : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IYtAccessProvider>;

    virtual void RequestAccess(
        TStringBuf ytCluster,
        TStringBuf path,
        TStringBuf requester,
        EIdentityType identityType,
        TStringBuf identity,
        TMaybe<TDuration> period = {}) = 0;
};

}; // namespace NYql
