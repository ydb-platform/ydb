#pragma once

#include <yql/essentials/ast/yql_errors.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql {

using NKikimr::NMiniKQL::TRuntimeNode;

struct TConvertResult {
    TRuntimeNode Node;
    TIssues Errors;
};


struct TMiniKQLCompileResult {
    TMiniKQLCompileResult() = default;
    explicit TMiniKQLCompileResult(const TIssue& error);
    explicit TMiniKQLCompileResult(const TIssues& errors);
    explicit TMiniKQLCompileResult(const TString& compiledProgram);
    TIssues Errors;
    TString CompiledProgram;
};

} // namespace NYql
