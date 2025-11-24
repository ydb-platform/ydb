#include "result.h"

#include <util/stream/output.h>

namespace NSQLTranslationV1 {

bool Unwrap(TSQLStatus status) {
    EnsureUnwrappable(status);
    return static_cast<bool>(status);
}

} // namespace NSQLTranslationV1

template <>
void Out<NSQLTranslationV1::ESQLError>(IOutputStream& out, NSQLTranslationV1::ESQLError value) {
    switch (value) {
        case NSQLTranslationV1::ESQLError::Basic:
            out << "Basic";
            break;
        case NSQLTranslationV1::ESQLError::UnsupportedYqlSelect:
            out << "UnsupportedYqlSelect";
            break;
    }
}
