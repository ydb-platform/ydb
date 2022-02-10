#include "compile_result.h"


namespace NYql {

TMiniKQLCompileResult::TMiniKQLCompileResult(const TIssue& error) {
    Errors.AddIssue(error);
}

TMiniKQLCompileResult::TMiniKQLCompileResult(const TIssues& errors)
    : Errors(errors)
{
}

TMiniKQLCompileResult::TMiniKQLCompileResult(const TString& compiledProgram)
    : CompiledProgram(compiledProgram)
{
}

} // namespace NYql
