#include "result.h"

#include "context.h"

#include <util/stream/output.h>

namespace NSQLTranslationV1 {

bool Unwrap(TSQLStatus status) {
    EnsureUnwrappable(status);
    return static_cast<bool>(status);
}

std::unexpected<ESQLError> UnsupportedYqlSelect(TContext& ctx, TStringBuf message) {
    if (ctx.GetYqlSelectMode() == EYqlSelectMode::Force) {
        ctx.Error() << "YqlSelect unsupported: " << message;
    }

    return std::unexpected(ESQLError::UnsupportedYqlSelect);
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
