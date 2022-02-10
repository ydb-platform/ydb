#pragma once

#include "yql_ast.h"
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

namespace NYql {

class TTypeAnnotationNode;

TAstNode* ParseType(TStringBuf str, TMemoryPool& pool, TIssues& issues,
        TPosition position = {1, 1});

TString FormatType(const TTypeAnnotationNode* typeNode);

} // namespace NYql
