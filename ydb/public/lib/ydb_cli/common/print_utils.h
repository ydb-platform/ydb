#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <library/cpp/colorizer/colors.h>
#include <util/system/file.h>

namespace NYdb::NConsoleClient {

void PrintSchemeEntry(IOutputStream& o, const NScheme::TSchemeEntry& entry, NColorizer::TColors colors);
TString FormatTime(TInstant time);
TString FormatDuration(TDuration duration);
TString FormatBytes(ui64 bytes);
TString FormatSpeed(double bytesPerSecond);
TString FormatEta(TDuration duration);
TString PrettySize(ui64 size);
TString PrettyNumber(ui64 number);
TString EntryTypeToString(NScheme::ESchemeEntryType entry);
    int PrintProtoJsonBase64(const google::protobuf::Message& msg, IOutputStream& out);
    FHANDLE GetStdinFileno();
    TString BlurSecret(const TString& in);

    void PrintPermissions(const std::vector<NScheme::TPermissions>& permissions, IOutputStream& out);
    void PrintAllPermissions(
        const std::string& owner,
        const std::vector<NScheme::TPermissions>& permissions,
        const std::vector<NScheme::TPermissions>& effectivePermissions,
        IOutputStream& out = Cout
    );

} // namespace NYdb::NConsoleClient
