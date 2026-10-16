#pragma once

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/maybe.h>

namespace NFq {

enum class EExternalObjectKind {
    Unknown,
    Table,
    MessageStream,
};

struct TExternalObjectKindResult {
    TMaybe<EExternalObjectKind> Kind;
    NYql::TIssues Issues;
};

} // namespace NFq
