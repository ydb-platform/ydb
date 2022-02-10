#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>

#define LWTRACE_DEFINE_SYMBOL(variable, text)         \
    static TString variable##_holder(text);           \
    ::NLWTrace::TSymbol variable(&variable##_holder); \
    /**/

#define LWTRACE_INLINE_SYMBOL(text)           \
    [&] {                                     \
        static TString _holder(text);         \
        return ::NLWTrace::TSymbol(&_holder); \
    }() /**/

#define LWTRACE_LOCATION_SYMBOL                                                          \
    [](const char* func) {                                                               \
        static TString _holder(TStringBuilder() << func << " (" << __LOCATION__ << ")"); \
        return ::NLWTrace::TSymbol(&_holder);                                            \
    }(Y_FUNC_SIGNATURE) /**/

namespace NLWTrace {
    struct TSymbol {
        TString* Str;

        TSymbol()
            : Str(nullptr)
        {
        }

        explicit TSymbol(TString* str)
            : Str(str)
        {
        }

        TSymbol& operator=(const TSymbol& o) {
            Str = o.Str;
            return *this;
        }

        TSymbol(const TSymbol& o)
            : Str(o.Str)
        {
        }

        bool operator<(const TSymbol& rhs) const {
            return Str < rhs.Str;
        }
        bool operator>(const TSymbol& rhs) const {
            return Str > rhs.Str;
        }
        bool operator<=(const TSymbol& rhs) const {
            return Str <= rhs.Str;
        }
        bool operator>=(const TSymbol& rhs) const {
            return Str >= rhs.Str;
        }
        bool operator==(const TSymbol& rhs) const {
            return Str == rhs.Str;
        }
        bool operator!=(const TSymbol& rhs) const {
            return Str != rhs.Str;
        }
    };

}
