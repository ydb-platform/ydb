#pragma once

#include <algorithm>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/input.h>

class TRobotsTxtParser;

class TRobotsTxtRulesRecord {
private:
    TRobotsTxtParser& Parser;

public:
    TRobotsTxtRulesRecord(TRobotsTxtParser& parser);
    bool NextPair(TString& field, TString& value, bool handleErrors, TVector<int>& nonRobotsLines, bool* wasBlank = nullptr);
};

class TRobotsTxtParser {
    friend class TRobotsTxtRulesRecord;

private:
    IInputStream& InputStream;
    TString Line;
    int LineNumber;
    bool IsLastSymbolCR;

    const char* ReadLine();
    static bool IsBlankLine(const char*);
    static bool IsRobotsLine(const char*);

public:
    static char* Trim(char*);
    TRobotsTxtParser(IInputStream& inputStream);
    bool HasRecord();
    TRobotsTxtRulesRecord NextRecord();
    int GetLineNumber();
};
