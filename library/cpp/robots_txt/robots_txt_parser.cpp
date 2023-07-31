#include "robots_txt_parser.h"
#include <util/generic/string.h>
#include <util/stream/output.h>

TRobotsTxtParser::TRobotsTxtParser(IInputStream& inputStream)
    : InputStream(inputStream)
    , LineNumber(0)
    , IsLastSymbolCR(false)
{
}

int TRobotsTxtParser::GetLineNumber() {
    return LineNumber;
}

const char* TRobotsTxtParser::ReadLine() {
    Line = "";
    char c;

    if (IsLastSymbolCR) {
        if (!InputStream.ReadChar(c))
            return nullptr;
        if (c != '\n')
            Line.append(c);
    }

    bool hasMoreSymbols;
    while (hasMoreSymbols = InputStream.ReadChar(c)) {
        if (c == '\r') {
            IsLastSymbolCR = true;
            break;
        } else {
            IsLastSymbolCR = false;
            if (c == '\n')
                break;
            Line.append(c);
        }
    }
    if (!hasMoreSymbols && Line.empty())
        return nullptr;

    // BOM UTF-8: EF BB BF
    if (0 == LineNumber && Line.size() >= 3 && Line[0] == '\xEF' && Line[1] == '\xBB' && Line[2] == '\xBF')
        Line = Line.substr(3, Line.size() - 3);

    ++LineNumber;
    int i = Line.find('#');
    if (i == 0)
        Line = "";
    else if (i > 0)
        Line = Line.substr(0, i);
    return Line.data();
}

bool TRobotsTxtParser::IsBlankLine(const char* s) {
    for (const char* p = s; *p; ++p)
        if (!isspace(*p))
            return 0;
    return 1;
}

char* TRobotsTxtParser::Trim(char* s) {
    while (isspace(*s))
        ++s;
    char* p = s + strlen(s) - 1;
    while (s < p && isspace(*p))
        --p;
    *(p + 1) = 0;
    return s;
}

inline bool TRobotsTxtParser::IsRobotsLine(const char* s) {
    return strchr(s, ':');
}

bool TRobotsTxtParser::HasRecord() {
    while (!IsRobotsLine(Line.data()))
        if (!ReadLine())
            return 0;
    return 1;
}

TRobotsTxtRulesRecord TRobotsTxtParser::NextRecord() {
    return TRobotsTxtRulesRecord(*this);
}

TRobotsTxtRulesRecord::TRobotsTxtRulesRecord(TRobotsTxtParser& parser)
    : Parser(parser)
{
}

bool TRobotsTxtRulesRecord::NextPair(TString& field, TString& value, bool handleErrors, TVector<int>& nonRobotsLines, bool* wasBlank) {
    if (wasBlank) {
        *wasBlank = false;
    }
    while (!Parser.IsRobotsLine(Parser.Line.data())) {
        if (!Parser.ReadLine())
            return 0;
        if (Parser.IsBlankLine(Parser.Line.data())) {
            if (wasBlank) {
                *wasBlank = true;
            }
            continue;
        }
        if (handleErrors && !Parser.IsRobotsLine(Parser.Line.data()))
            nonRobotsLines.push_back(Parser.GetLineNumber());
    }

    char* s = strchr(Parser.Line.begin(), ':');
    *s = 0;
    char* p = s + 1;

    field = TRobotsTxtParser::Trim(strlwr(Parser.Line.begin()));
    value = TRobotsTxtParser::Trim(p);
    return 1;
}
