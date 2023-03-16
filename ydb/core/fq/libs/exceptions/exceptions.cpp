#include "exceptions.h"

#include <util/string/builder.h>

namespace NFq {

TCodeLineException::TCodeLineException(TIssuesIds::EIssueCode code)
    : SourceLocation("", 0)
    , Code(code)
{}

TCodeLineException::TCodeLineException(const TSourceLocation& sl, const TCodeLineException& t)
    : yexception(t)
    , SourceLocation(sl)
    , Code(t.Code)
{}

const char* TCodeLineException::GetRawMessage() const {
    return yexception::what();
}

const char* TCodeLineException::what() const noexcept {
    try {
        if (!Message) {
            Message = TStringBuilder{} << SourceLocation << TStringBuf(": ") << yexception::what();
        }
        return Message.c_str();
    } catch(...) {
        return "Unexpected exception in TCodeLineException::what()";
    }
}

TCodeLineException operator+(const TSourceLocation& sl, TCodeLineException&& t) {
    return TCodeLineException(sl, t);
}

} // namespace NFq