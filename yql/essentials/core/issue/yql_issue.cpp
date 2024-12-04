#include "yql_issue.h"

namespace NYql {

const char IssueMapResource[] = "yql_issue.txt";

static_assert(DEFAULT_ERROR == TIssuesIds::DEFAULT_ERROR,
    "value of particular and common error mismatched for \"DEFAULT_ERROR\"");
static_assert(UNEXPECTED_ERROR == TIssuesIds::UNEXPECTED,
    "value of particular and common error mismatched for \"UNEXPECTED_ERROR\"");
}
