#include "exceptions.h"

#include <util/string/builder.h>

#include <cerrno>
#include <cstring>

using namespace NYsonPull::NException;

const char* TBadStream::what() const noexcept {
    TStringBuilder stream;
    stream << "Invalid YSON";
    if (Position_.Offset || Position_.Line || Position_.Column) {
        bool first = true;
        stream << " at ";
        if (Position_.Offset) {
            stream << "offset " << *Position_.Offset;
            first = false;
        }
        if (Position_.Line) {
            if (!first) {
                stream << ", ";
            }
            stream << "line " << *Position_.Line;
            first = false;
        }
        if (Position_.Column) {
            if (!first) {
                stream << ", ";
            }
            stream << "column " << *Position_.Column;
        }
    }
    stream << ": " << Message_;
    FormattedMessage_ = stream;
    return FormattedMessage_.c_str();
}

NYsonPull::NException::TSystemError::TSystemError()
    : SavedErrno_{errno} {
}

const char* NYsonPull::NException::TSystemError::what() const noexcept {
    return ::strerror(SavedErrno_);
}
