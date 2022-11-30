#pragma once

#include <util/generic/string.h>
#include <library/cpp/charset/codepage.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

#include <library/cpp/langmask/langmask.h>

// Mix-in class for loading configuration files built of language sections. Handles version, encoding,
// comments, and language section switching; delegates actual processing to derived classes
// via ParseLine() function (pure virtual).

class TWordListReader {
private:
    ELanguage LangCode;
    ECharset Encoding;
    int Version;
    bool SkippingByError;

public:
    TWordListReader()
        : LangCode(LANG_UNK)
        , Encoding(CODES_YANDEX)
        , Version(0)
        , SkippingByError(false)
    {
    }
    virtual ~TWordListReader() {
    }

protected:
    virtual void ParseLine(const TUtf16String& line, ELanguage langcode, int version) = 0;

    void ReadDataFile(const char* filename) {
        TBuffered<TUnbufferedFileInput> src(4096, filename);
        ReadDataFile(src);
    }
    void ReadDataFile(IInputStream& src);

private:
    void ProcessLine(const TString& line);
};
