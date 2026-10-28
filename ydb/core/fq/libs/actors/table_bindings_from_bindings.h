#pragma once

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <yql/essentials/sql/settings/translation_settings.h>

namespace NFq {

void AddTableBindingsFromBindings(const TVector<FederatedQuery::Binding>& bindings,
    const THashMap<TString, FederatedQuery::Connection>& connections, NSQLTranslation::TTranslationSettings& sqlSettings);

}   // namespace NFq
