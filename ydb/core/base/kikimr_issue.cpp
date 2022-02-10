#include "kikimr_issue.h"

namespace NKikimr {

const char IssueMapResource[] = "kikimr_issue.txt";

static_assert(NYql::DEFAULT_ERROR == NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
    "value of particular and common error mismatched for \"DEFAULT_ERROR\"");
static_assert(NYql::UNEXPECTED_ERROR == NKikimrIssues::TIssuesIds::UNEXPECTED,
    "value of particular and common error mismatched for \"UNEXPECTED_ERROR\"");
}
