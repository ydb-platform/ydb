#include "result.h"

#include "context.h"

#include <util/stream/output.h>

namespace NSQLTranslationV1 {

bool Unwrap(TSQLStatus status) {
    EnsureUnwrappable(status);
    return static_cast<bool>(status);
}

std::unexpected<ESQLError> UnsupportedYqlSelect(TContext& ctx, TStringBuf message) {
    if (ctx.GetYqlSelectMode() == EYqlSelect::Force) {
        ctx.Error() << "YqlSelect unsupported: " << message;
    }

    return std::unexpected(ESQLError::UnsupportedYqlSelect);
}

} // namespace NSQLTranslationV1

// TODO(YQL-21521): use GENERATE_ENUM_SERIALIZATION
Y_DECLARE_OUT_SPEC(, NSQLTranslationV1::ESQLError, out, value) {
    switch (value) {
        case NSQLTranslationV1::ESQLError::Basic:
            out << "Basic";
            break;
        case NSQLTranslationV1::ESQLError::UnsupportedYqlSelect:
            out << "UnsupportedYqlSelect";
            break;
    }
}
