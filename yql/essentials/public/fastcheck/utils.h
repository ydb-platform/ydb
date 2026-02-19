#pragma once

#include "linter.h"

#include <yql/essentials/sql/settings/translation_settings.h>

#include <library/cpp/json/json_reader.h>

namespace NYql::NFastCheck {

TUdfFilter ParseUdfFilter(const NJson::TJsonValue& json);

void FillClusters(const TChecksRequest& request, NSQLTranslation::TTranslationSettings& settings);

} // namespace NYql::NFastCheck
