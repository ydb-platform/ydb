#include "init.h"

#include <libxml/xmlIO.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>

#include <library/cpp/charset/recyr.hh>
#include <util/generic/singleton.h>

namespace {
    int CharEncodingInput(unsigned char* out, int* outlen, const unsigned char* in, int* inlen) {
        size_t read = 0, written = 0;
        RECODE_RESULT r = Recode(CODES_WIN, CODES_UTF8, (const char*)in, (char*)out, (size_t)*inlen, (size_t)*outlen, read, written);
        if (r == RECODE_EOOUTPUT)
            return -1;
        if (r != RECODE_OK)
            return -2;
        *inlen = (int)read;
        *outlen = (int)written;
        return (int)written;
    }

    class TLibXml2 {
    public:
        inline TLibXml2() {
            xmlInitParser();
            xmlNewCharEncodingHandler("windows-1251", CharEncodingInput, nullptr);
        }

        inline ~TLibXml2() {
            xmlCleanupParser();
        }
    };
}

void NXml::InitEngine() {
    Singleton<TLibXml2>();
}
