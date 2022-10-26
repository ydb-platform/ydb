#include "quoted_pair.h"

#include <util/generic/strbuf.h>

namespace NUnicode {
    EUnicodeQuotedPairType ResolveUnicodeQuotedPair(wchar32 escapedSymbol, wchar32& symbol, TUnicodeSet& set) {
        switch (escapedSymbol) {
            case wchar32('a'): // \a -> U+0007 Bell
                symbol = wchar32('\a');
                return UQPT_SYMBOL;
            case wchar32('b'): // \b -> U+0008 Backspace
                symbol = wchar32('\b');
                return UQPT_SYMBOL;
            case wchar32('t'): // \t -> U+0009 Horizontal Tab
                symbol = wchar32('\t');
                return UQPT_SYMBOL;
            case wchar32('n'): // \n -> U+000A Line Feed
                symbol = wchar32('\n');
                return UQPT_SYMBOL;
            case wchar32('v'): // \v -> U+000B Vertical Tab
                symbol = wchar32('\v');
                return UQPT_SYMBOL;
            case wchar32('f'): // \f -> U+000C Form Feed
                symbol = wchar32('\f');
                return UQPT_SYMBOL;
            case wchar32('r'): // \r -> U+000D Carriage Return
                symbol = wchar32('\r');
                return UQPT_SYMBOL;
            case wchar32('s'):
                set.AddCategory(TStringBuf("Z"));
                return UQPT_SET;
            case wchar32('S'):
                set.Add(TUnicodeSet().AddCategory(TStringBuf("Z")).Invert());
                return UQPT_SET;
            case wchar32('w'):
                set.AddCategory(TStringBuf("L"));
                return UQPT_SET;
            case wchar32('W'):
                set.Add(TUnicodeSet().AddCategory(TStringBuf("L")).Invert());
                return UQPT_SET;
            case wchar32('d'):
                set.AddCategory(TStringBuf("Nd"));
                return UQPT_SET;
            case wchar32('D'):
                set.Add(TUnicodeSet().AddCategory(TStringBuf("Nd")).Invert());
                return UQPT_SET;
            default:
                symbol = escapedSymbol;
                return UQPT_SYMBOL;
        }
    }

}
