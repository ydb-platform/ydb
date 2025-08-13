#include "log_parser.h"

#include <regex>

namespace NYql::NLog {

    TLogRow ParseJsonLogRow(TStringBuf str) {
        NJson::TJsonMap json;
        UNIT_ASSERT_C(NJson::ReadJsonTree(str, &json), "invalid json '" << str << "'");

        return {
            .Time = TInstant::ParseIso8601(json["@fields"]["datetime"].GetStringSafe()) - TDuration::Hours(4),
            .Level = ELevelHelpers::FromString(json["@fields"]["level"].GetStringSafe()),
            .ProcName = json["@fields"]["procname"].GetStringSafe(),
            .ProcId = FromString<pid_t>(json["@fields"]["pid"].GetStringSafe()),
            .ThreadId = [&] {
                TString string = json["@fields"]["tid"].GetStringSafe();
                if (string.substr(0, 2) == "0x") {
                    return IntFromString<ui64, 16, TStringBuf>(string.substr(2));
                } else {
                    return IntFromString<ui64, 10, TStringBuf>(string);
                }
            }(),
            .Component = EComponentHelpers::FromString(json["@fields"]["component"].GetStringSafe()),
            .FileName = json["@fields"]["filename"].GetStringSafe(),
            .LineNumber = FromString<ui32>(json["@fields"]["line"].GetStringSafe()),
            .Path = json["@fields"]["path"].GetStringRobust(),
            .Message = json["message"].GetStringSafe(),
        };
    }

    TLogRow ParseLegacyLogRow(TStringBuf str) {
        static std::regex rowRe(
            "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}) " // (1) time
            "([A-Z ]{5}) "                                                         // (2) level
            "([a-zA-Z0-9_\\.-]+)"                                                  // (3) process name
            ".pid=([0-9]+),"                                                       // (4) process id
            " tid=(0?x?[0-9a-fA-F]+). "                                            // (5) thread id
            ".([a-zA-Z0-9_\\. ]+). "                                               // (6) component name
            "([^:]+):"                                                             // (7) file name
            "([0-9]+): "                                                           // (8) line number
            "(\\{[^\n]*\\} )?"                                                     // (9) path
            "([^\n]*)\n?$"                                                         // (10) message
            , std::regex_constants::extended);

        std::cmatch match;
        bool isMatch = std::regex_match(str.data(), match, rowRe);

        UNIT_ASSERT_C(isMatch, "log row does not match format: '" << str << '\'');
        UNIT_ASSERT_EQUAL_C(match.size(), 11, "expected 11 groups in log row: '" << str << '\'');

        return {
            .Time = TInstant::ParseIso8601(match[1].str()) - TDuration::Hours(4),
            .Level = ELevelHelpers::FromString(match[2].str()),
            .ProcName = match[3].str(),
            .ProcId = FromString<pid_t>(match[4].str()),
            .ThreadId = match[5].str().substr(0, 2) == "0x"
                            ? IntFromString<ui64, 16, TStringBuf>(match[5].str().substr(2))
                            : IntFromString<ui64, 10, TStringBuf>(match[5].str()),
            .Component = EComponentHelpers::FromString(match[6].str()),
            .FileName = match[7].str(),
            .LineNumber = FromString<ui32>(match[8].str()),
            .Path = match[9].str() != ""
                        ? match[9].str().substr(1, match[9].str().size() - 3)
                        : "",
            .Message = match[10].str(),
        };
    }

} // namespace NYql::NLog
