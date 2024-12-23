#pragma once

#include "ast_nodes.h"
#include "binary.h"

namespace NYql::NJsonPath {

const TAstNodePtr ParseJsonPathAst(const TStringBuf path, TIssues& issues, size_t maxParseErrors);

const TJsonPathPtr PackBinaryJsonPath(const TAstNodePtr ast, TIssues& issues);

const TJsonPathPtr ParseJsonPath(const TStringBuf path, TIssues& issues, size_t maxParseErrors);

}
