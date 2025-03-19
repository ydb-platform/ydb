#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <library/cpp/colorizer/colors.h>

namespace NYdb {
namespace NConsoleClient {

void PrintSchemeEntry(IOutputStream& o, const NScheme::TSchemeEntry& entry, NColorizer::TColors colors);
TString FormatTime(TInstant time);
TString FormatDuration(TDuration duration);
TString PrettySize(ui64 size);
TString PrettyNumber(ui64 number);
TString EntryTypeToString(NScheme::ESchemeEntryType entry);

int PrintProtoJsonBase64(const google::protobuf::Message& msg);

}
}
