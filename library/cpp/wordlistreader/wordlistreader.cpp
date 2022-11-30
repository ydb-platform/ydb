#include "wordlistreader.h"

#include <library/cpp/charset/wide.h>
#include <library/cpp/langs/langs.h>

#include <library/cpp/charset/recyr.hh>
#include <util/string/cast.h>
#include <util/generic/yexception.h>
#include <util/string/vector.h>
#include <util/string/split.h>

void TWordListReader::ProcessLine(const TString& line) {
    if (line.find('[') == 0 && line.find(']') != TString::npos) {
        size_t endpos = line.find(']');
        TString langname = line.substr(1, endpos - 1);
        LangCode = LanguageByName(langname);
        if (LangCode != LANG_UNK) {
            SkippingByError = false;
        } else {
            Cerr << "Unknown language name: " << langname.c_str() << Endl;
            SkippingByError = true;
        }
    } else if (!SkippingByError) {
        TUtf16String recodedLine = CharToWide(line, Encoding);
        ParseLine(recodedLine, LangCode, Version);
    }
}

void TWordListReader::ReadDataFile(IInputStream& src) {
    // Read header for version and encoding
    LangCode = LANG_UNK;
    Encoding = CODES_YANDEX;
    Version = 0;
    SkippingByError = false;

    TString line;
    while (src.ReadLine(line)) {
        if (line[0] == '#')
            continue; // comment
        TVector<TString> tokens = StringSplitter(line).SplitBySet(" \t\r\n:,").SkipEmpty();
        if (tokens.size() == 2) {
            if (stricmp(tokens[0].c_str(), "version") == 0) {
                Version = FromString<int>(tokens[1]);
                continue;
            } else if (stricmp(tokens[0].c_str(), "encoding") == 0) {
                Encoding = CharsetByName(tokens[1].c_str());
                if (Encoding == CODES_UNKNOWN)
                    ythrow yexception() << "Invalid encoding name";
                continue;
            }
        }
        break;
    }

    // Read the body
    ProcessLine(line);
    while (src.ReadLine(line)) {
        if (line[0] == '#')
            continue; // skip comments
        ProcessLine(line);
    }
}
