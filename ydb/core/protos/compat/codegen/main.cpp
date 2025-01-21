#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

const char* HEADERS_END_MARKER = "// x-compat: body";
const char* APP_CONFIG_START = "message TAppConfig {\n";
const char* APP_CONFIG_END = "} // message TAppConfig";
const char* EXTEND_MARKER = "// x-compat: extended";
const char* REPLACE_POINT = "    optional T";
const char* APP_CONFIG_COMPAT_START = "message TAppConfigCompat {\n";
const char* APP_CONFIG_COMPAT_END = "}\n";
const char* ADDITIONAL_IMPORTS = R"(
import "ydb/library/yaml_config/protos/config.proto";
import "ydb/core/protos/config.proto";
)";

TString ReplaceCompat(TString in) {
    TString result;
    auto pos = in.find(REPLACE_POINT) + strlen(REPLACE_POINT);
    result.append(in.substr(0, pos));
    result.append("Extended");
    result.append(in.substr(pos, TString::npos));
    return result;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        Cerr << "Usage: " << argv[0] << " INPUT OUTPUT" << Endl;
        return 1;
    }

    TString in = TFileInput(argv[1]).ReadAll();
    TString out;

    out = ADDITIONAL_IMPORTS;

    auto pos = in.find(HEADERS_END_MARKER);

    if (pos == TString::npos) {
        Cerr << "Can't find headers end marker" << Endl;
        return 1;
    }

    out.append(in.substr(0, pos));

    pos = in.find(APP_CONFIG_START, pos);

    if (pos == TString::npos) {
        Cerr << "Can't find TAppConfig" << Endl;
        return 1;
    }

    pos = pos + strlen(APP_CONFIG_START);
    auto endpos = in.find(APP_CONFIG_END, pos);

    if (endpos == TString::npos) {
        Cerr << "Can't find end of TAppConfig" << Endl;
        return 1;
    }

    out.append(APP_CONFIG_COMPAT_START);

    auto appConfig = in.substr(pos, endpos - pos);

    size_t prevExtendPos = 0;
    auto extendPos = appConfig.find(EXTEND_MARKER);
    while (extendPos != TString::npos) {
        auto strBegin = appConfig.rfind("\n", extendPos);

        out.append(appConfig.substr(prevExtendPos, strBegin + 1));
        out.append(ReplaceCompat(appConfig.substr(strBegin + 1, extendPos - strBegin - 1)));
        out.append("\n");

        prevExtendPos = extendPos + strlen(EXTEND_MARKER);
        extendPos = appConfig.find(EXTEND_MARKER, extendPos + strlen(EXTEND_MARKER));
    }

    out.append(appConfig.substr(prevExtendPos + 1, TString::npos));

    out.append(APP_CONFIG_COMPAT_END);

    TFileOutput(argv[2]).Write(out);
    Cout << "Generated " << argv[2] << " from " << argv[1] << Endl;

    return 0;
}
