#include "error.h"

namespace NAST {

IErrorCollector::IErrorCollector(size_t maxErrors)
    : MaxErrors_(maxErrors)
    , NumErrors_(0)
{
    Y_ENSURE(0 < MaxErrors_);
}

IErrorCollector::~IErrorCollector()
{
}

void IErrorCollector::Error(ui32 line, ui32 col, const TString& message) {
    GuardTooManyErrors();
    AddError(line, col, message);
    ++NumErrors_;
}

void IErrorCollector::Report(NYql::TIssue&& issue) {
    GuardTooManyErrors();
    bool isError = issue.GetSeverity() >= NYql::TSeverityIds::S_WARNING;
    AddIssue(std::forward<NYql::TIssue>(issue));
    if (isError) {
        ++NumErrors_;
    }
}

void IErrorCollector::GuardTooManyErrors() {
    if (NumErrors_ + 1 == MaxErrors_) {
        AddError(0, 0, "Too many errors");
        ++NumErrors_;
    }

    if (NumErrors_ >= MaxErrors_) {
        ythrow TTooManyErrors() << "Too many errors";
    }
}

} // namespace NAST
