#pragma once

#include <yql/essentials/core/project_settings/yql_project_settings.h>

namespace NYql {

namespace NProto {
class TProjectSettings;
} // namespace NProto

TProjectSettings ProjectSettingsFromJson(TStringBuf json);
TString ProjectSettingsToJson(const TProjectSettings& settings);

TProjectSettings ProjectSettingsFromYson(const NYT::TNode& settings);
NYT::TNode ProjectSettingsToYson(const TProjectSettings& settings);

TProjectSettings ProjectSettingsFromProto(const NProto::TProjectSettings& proto);

} // namespace NYql
