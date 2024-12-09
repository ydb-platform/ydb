#pragma once

#include "yql_result_format_common.h"

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/yson/node/node.h>

namespace NYql::NResult {

struct TFullResultRef {
    TVector<TString> Reference;
    TMaybe<TVector<TString>> Columns;
    bool Remove = false;
};

struct TWrite {
    const NYT::TNode* Type = nullptr;
    const NYT::TNode* Data = nullptr;
    bool IsTruncated = false;
    TVector<TFullResultRef> Refs;
};

struct TResult {
    TMaybe<TPosition> Position;
    TMaybe<TString> Label;
    TVector<TWrite> Writes;
    bool IsUnordered = false;
    bool IsTruncated = false;
};

TVector<TResult> ParseResponse(const NYT::TNode& responseNode);

}
