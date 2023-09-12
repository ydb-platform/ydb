#pragma once

#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <regex>


namespace NYql {
namespace NLog {

struct TLogRow {
    TInstant Time;
    ELevel Level;
    TString ProcName;
    pid_t ProcId;
    ui64 ThreadId;
    EComponent Component;
    TString FileName;
    ui32 LineNumber;
    TString Message;
};

static TLogRow ParseLogRow(const TString& str) {
    static std::regex rowRe(
                "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}) " // (1) time
                "([A-Z ]{5}) "                                                         // (2) level
                "([a-zA-Z0-9_\\.-]+)"                                                  // (3) process name
                ".pid=([0-9]+),"                                                       // (4) process id
                " tid=(0?x?[0-9a-fA-F]+). "                                            // (5) thread id
                ".([a-zA-Z0-9_\\. ]+). "                                               // (6) component name
                "([^:]+):"                                                             // (7) file name
                "([0-9]+): "                                                           // (8) line number
                "([^\n]*)\n?$"                                                         // (9) message
                , std::regex_constants::extended);

    std::cmatch match;
    bool isMatch = std::regex_match(str.c_str(), match, rowRe);

    UNIT_ASSERT_C(isMatch, "log row does not match format: '" << str << '\'');
    UNIT_ASSERT_EQUAL_C(match.size(), 10, "expected 10 groups in log row: '" << str << '\'');

    TLogRow logRow;
    logRow.Time = TInstant::ParseIso8601(match[1].str()) - TDuration::Hours(4);
    logRow.Level = ELevelHelpers::FromString(match[2].str());
    logRow.ProcName = match[3].str();
    logRow.ProcId = FromString<pid_t>(match[4].str());
    logRow.ThreadId = match[5].str().substr(0, 2) == "0x" ?
        IntFromString<ui64, 16, TStringBuf>(match[5].str().substr(2)) :
        IntFromString<ui64, 10, TStringBuf>(match[5].str());
    logRow.Component = EComponentHelpers::FromString(match[6].str());
    logRow.FileName = match[7].str();
    logRow.LineNumber = FromString<ui32>(match[8].str());
    logRow.Message = match[9].str();
    return logRow;
}

} // namspace NLog
} // namspace NYql
