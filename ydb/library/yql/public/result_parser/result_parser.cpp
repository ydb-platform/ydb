#include "result_parser.h"

namespace NYql {

#define THWROW_IF_NOT_IMPL \
    if constexpr (ThrowIfNotImplemented) { \
        throw yexception() << __func__ << " not implemented."; \
    }

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnLabel(const TString&) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnPosition(const TPosition&) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnWriteBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnWriteEnd() {
    THWROW_IF_NOT_IMPL;
}

template struct TResultVisitorBase<true>;
template struct TResultVisitorBase<false>;

void ParseResult(const std::string_view& yson, IResultVisitor& visitor, const TResultParseOptions& options) {
    visitor.OnLabel("TODO");
    Y_UNUSED(yson, visitor, options);
}

}
