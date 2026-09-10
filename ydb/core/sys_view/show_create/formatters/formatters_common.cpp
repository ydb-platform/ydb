#include "formatters_common.h"

#include <yql/essentials/ast/yql_ast_escaping.h>

namespace NKikimr::NSysView {

void EscapeName(const TString& str, TStringStream& stream) {
    NYql::EscapeArbitraryAtom(str, '`', &stream);
}

void EscapeString(const TString& str, TStringStream& stream) {
    NYql::EscapeArbitraryAtom(str, '\'', &stream);
}

void EscapeBinary(const TString& str, TStringStream& stream) {
    NYql::EscapeBinaryAtom(str, '\'', &stream);
}

void EscapeValue(bool value, TStringStream& stream) {
    if (value) {
        EscapeName("true", stream);
    } else {
        EscapeName("false", stream);
    }
}

}
