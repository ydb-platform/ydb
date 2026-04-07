#pragma once

#include <library/cpp/yson/node/node.h>
#include "exec_ctx.h"

namespace NYql {

template <class TExecParamsPtr>
void PrepareCommonAttributes(
    NYT::TNode& attrs,
    const TExecParamsPtr& execCtx,
    const TString& cluster,
    bool createTable
);

template <class TExecParamsPtr>
static void PrepareAttributes(
    NYT::TNode& attrs,
    const TOutputInfo& out,
    const TExecParamsPtr& execCtx,
    const TString& cluster,
    bool createTable,
    const TSet<TString>& securityTags = {}
);


} // namespace NYql

#include "yt_attrs-inl.h"
