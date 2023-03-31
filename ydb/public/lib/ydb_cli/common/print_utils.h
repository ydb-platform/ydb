#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <library/cpp/colorizer/colors.h>

namespace NYdb {
namespace NConsoleClient {

void PrintSchemeEntry(IOutputStream& o, const NScheme::TSchemeEntry& entry, NColorizer::TColors colors);
TString FormatTime(TInstant time);
TString FormatDuration(TDuration duration);
TString PrettySize(size_t size);
TString EntryTypeToString(NScheme::ESchemeEntryType entry);

}
}
