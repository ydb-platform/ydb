#pragma once

#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq {

void AddTableBindingsFromBindings(const TVector<FederatedQuery::Binding>& bindings, const THashMap<TString, FederatedQuery::Connection>& connections, NSQLTranslation::TTranslationSettings& sqlSettings);

} //NFq
