#pragma once

#include <yql/essentials/public/fastcheck/linter.h>

namespace NSQLTranslation {
struct TTranslationSettings;
} // namespace NSQLTranslation

namespace NYql::NFastCheck {

void FillClusters(const TChecksRequest& request, NSQLTranslation::TTranslationSettings& settings);

} // namespace NYql::NFastCheck
