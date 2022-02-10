#pragma once

#include <util/generic/string.h>

namespace NProtobufJson {
    void ToSnakeCase(TString* const name);

    void ToSnakeCaseDense(TString* const name);

    /**
     * "FOO_BAR" ~ "foo_bar" ~ "fooBar"
     */
    bool EqualsIgnoringCaseAndUnderscores(TStringBuf s1, TStringBuf s2);
}
