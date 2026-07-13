#pragma once

#include <util/generic/fwd.h>

namespace NSQLTranslation {

using TSqlFlags = THashSet<TString>;

using TExtendedSqlFlags = THashMap<TString, TVector<TString>>;

#define TRANSLATOR_FLAGS_IN_MIGRATION_MODE 1

} // namespace NSQLTranslation
