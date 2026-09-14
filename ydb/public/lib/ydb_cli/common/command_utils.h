#pragma once

#include "command.h"

namespace NYdb::NConsoleClient::NUtils {

TString FormatOption(const NLastGetopt::TOpt* option, const NColorizer::TColors& colors);

// Option not to show in parent command help
bool NeedToHideOption(const NLastGetopt::TOpt* opt);

void PrintOptionsDescription(IOutputStream& os, const NLastGetopt::TOpts* opts, NColorizer::TColors& colors, const TString& command);

void PrintParentOptions(TStringStream& stream, TClientCommand::TConfig& config, NColorizer::TColors& colors);

} // namespace NYdbNConsoleClient::NUtils
