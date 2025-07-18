#include "generate.h"

namespace NSQLHighlight {

    bool IsPlain(EUnitKind kind) {
        return (kind != EUnitKind::Comment) &&
               (kind != EUnitKind::StringLiteral) &&
               (kind != EUnitKind::QuotedIdentifier) &&
               (kind != EUnitKind::BindParameterIdentifier);
    }

    bool IsIgnored(EUnitKind kind) {
        return kind == EUnitKind::Whitespace ||
               kind == EUnitKind::Error;
    }

} // namespace NSQLHighlight
