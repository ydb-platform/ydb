#include "naming_conventions.h"

#include <util/string/builder.h>

namespace NKikimr::NNaming {

    TString SnakeToCamelCase(const TString& name) {
        TStringBuilder builder;
        bool nextUpper = true;
        for (char c : name) {
            if (c == '_') {
                nextUpper = true;
            } else {
                builder << (char)(nextUpper ? toupper(c) : c);
                nextUpper = false;
            }
        }
        return builder;
    }

    TString CamelToSnakeCase(const TString& name) {
        TStringBuilder builder;
        bool first = true;
        for (char c : name) {
            if (isupper(c)) {
                if (!first) {
                    builder << "_";
                }
            }
            first = false;
            builder << (char)tolower(c);
        }
        return builder;
    }

}