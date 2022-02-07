#include "exceptions.h"

#include <util/string/builder.h>

namespace NYq {

TControlPlaneStorageException::TControlPlaneStorageException(TIssuesIds::EIssueCode code)
    : SourceLocation("", 0)
    , Code(code)
{}

TControlPlaneStorageException::TControlPlaneStorageException(const TSourceLocation& sl, const TControlPlaneStorageException& t)
    : yexception(t)
    , SourceLocation(sl)
    , Code(t.Code)
{}

const char* TControlPlaneStorageException::GetRawMessage() const {
    return yexception::what();
}

const char* TControlPlaneStorageException::what() const noexcept {
    try {
        if (!Message) {
            Message = TStringBuilder{} << SourceLocation << TStringBuf(": ") << yexception::what();
        }
        return Message.c_str();
    } catch(...) {
        return "Unexpected exception in TControlPlaneStorageException::what()";
    }
}

TControlPlaneStorageException operator+(const TSourceLocation& sl, TControlPlaneStorageException&& t) {
    return TControlPlaneStorageException(sl, t);
}

} // namespace NYq