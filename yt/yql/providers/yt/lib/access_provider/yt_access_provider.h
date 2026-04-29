#pragma once

#include <yt/yql/providers/yt/lib/tvm_client/tvm_client.h>

#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/ptr.h>

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
        EIdentityType type,
        TStringBuf path,
        TStringBuf requester,
        const TYqlOperationOptions& operationOptions) = 0;
};

}; // namespace NYql
