#pragma once

#include "yql_issue.h"

#include <util/generic/ylimits.h>
#include <util/system/types.h>

namespace NYql {

struct TTruncateIssueOpts {

#define YQL_TRUNC_DECL_FIELD(type, name, def)    \
    TTruncateIssueOpts& Set##name(type arg##name)& {   \
        name = arg##name;                               \
        return *this;                                   \
    }                                                   \
    TTruncateIssueOpts&& Set##name(type arg##name)&& { \
        name = arg##name;                               \
        return std::move(*this);                        \
    }                                                   \
    type name = def;

    YQL_TRUNC_DECL_FIELD(ui32, MaxLevels, Max<ui32>())
    YQL_TRUNC_DECL_FIELD(ui32, KeepTailLevels, 1)

#undef YQL_TRUNC_DECL_FIELD
};

TIssue TruncateIssueLevels(const TIssue& topIssue, TTruncateIssueOpts opts = {});

} // namespace NYql
