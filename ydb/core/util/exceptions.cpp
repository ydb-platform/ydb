#include "exceptions.h"

#include <util/string/builder.h>

namespace NKikimr {

TCodeLineException::TCodeLineException(ui32 code)
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
    if (!Message) {
        Message = TStringBuilder() << SourceLocation << ": " << yexception::what();
    }
    return Message.c_str();
}

TCodeLineException operator+(const TSourceLocation& sl, TCodeLineException&& t) {
    return TCodeLineException(sl, t);
}

} // namespace NKikimr
