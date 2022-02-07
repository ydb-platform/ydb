#pragma once

#include <contrib/libs/libxml/include/libxml/parser.h>

namespace NXml {
    enum class EOption : int {
        // clang-format off
        Recover    = XML_PARSE_RECOVER,
        NoEnt      = XML_PARSE_NOENT,
        DTDLoad    = XML_PARSE_DTDLOAD,
        DTDAttr    = XML_PARSE_DTDATTR,
        DTDValid   = XML_PARSE_DTDVALID,
        NoError    = XML_PARSE_NOERROR,
        NoWarning  = XML_PARSE_NOWARNING,
        Pedantic   = XML_PARSE_PEDANTIC,
        NoBlanks   = XML_PARSE_NOBLANKS,
        SAX1       = XML_PARSE_SAX1,
        XInclude   = XML_PARSE_XINCLUDE,
        NoNet      = XML_PARSE_NONET,
        NoDict     = XML_PARSE_NODICT,
        NSClean    = XML_PARSE_NSCLEAN,
        NoCData    = XML_PARSE_NOCDATA,
        NoXInclude = XML_PARSE_NOXINCNODE,
        Compact    = XML_PARSE_COMPACT,
        Old10      = XML_PARSE_OLD10,
        NoBaseFix  = XML_PARSE_NOBASEFIX,
        Huge       = XML_PARSE_HUGE,
        OldSAX     = XML_PARSE_OLDSAX,
        IgnoreEnc  = XML_PARSE_IGNORE_ENC,
        BigLines   = XML_PARSE_BIG_LINES,
        // clang-format on
    };

    class TOptions {
    public:
        TOptions()
            : Mask(0)
        {
        }

        template <typename... TArgs>
        TOptions(TArgs... args)
            : Mask(0)
        {
            Set(args...);
        }

        TOptions& Set(EOption option) {
            Mask |= static_cast<int>(option);
            return *this;
        }

        template <typename... TArgs>
        TOptions& Set(EOption arg, TArgs... args) {
            Set(arg);
            return Set(args...);
        }

        int GetMask() const {
            return Mask;
        }

    private:
        int Mask;
    };

}
