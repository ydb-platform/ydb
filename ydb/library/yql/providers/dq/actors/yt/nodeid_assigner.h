#pragma once

#include <ydb/library/yql/providers/dq/common/attrs.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NYql {

struct TAssignNodeIdOptions {
    TString ClusterName;
    TString User;
    TString Token;
    TString Prefix;
    TString Role;
    TString NodeName;

    THashMap<TString, TString> Attributes;

    ui32 MinNodeId = 0;
    ui32 MaxNodeId = 1<<18;

    TMaybe<ui32> NodeId; // for debug only
};

ui32 AssignNodeId(const TAssignNodeIdOptions& options);

} // namespace NYql
