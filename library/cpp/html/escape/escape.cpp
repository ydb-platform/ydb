#include "escape.h"

#include <util/generic/array_size.h>
#include <util/generic/strbuf.h>

namespace NHtml {
    namespace {
        struct TReplace {
            char Char;
            bool ForText;
            TStringBuf Entity;
        };

        TReplace Escapable[] = {
            {'"', false, TStringBuf("&quot;")},
            {'&', true, TStringBuf("&amp;")},
            {'<', true, TStringBuf("&lt;")},
            {'>', true, TStringBuf("&gt;")},
        };

        TString EscapeImpl(const TString& value, bool isText) {
            auto ci = value.begin();
            // Looking for escapable characters.
            for (; ci != value.end(); ++ci) {
                for (size_t i = (isText ? 1 : 0); i < Y_ARRAY_SIZE(Escapable); ++i) {
                    if (*ci == Escapable[i].Char) {
                        goto escape;
                    }
                }
            }

            // There is no escapable characters, so return original value.
            return value;

        escape:
            TString tmp = TString(value.begin(), ci);

            for (; ci != value.end(); ++ci) {
                size_t i = (isText ? 1 : 0);

                for (; i < Y_ARRAY_SIZE(Escapable); ++i) {
                    if (*ci == Escapable[i].Char) {
                        tmp += Escapable[i].Entity;
                        break;
                    }
                }

                if (i == Y_ARRAY_SIZE(Escapable)) {
                    tmp += *ci;
                }
            }

            return tmp;
        }

    }

    TString EscapeAttributeValue(const TString& value) {
        return EscapeImpl(value, false);
    }

    TString EscapeText(const TString& value) {
        return EscapeImpl(value, true);
    }

}
