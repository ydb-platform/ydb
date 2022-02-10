#pragma once

#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/public/api/protos/yq.pb.h>

namespace NYq {

void AddTableBindingsFromBindings(const TVector<YandexQuery::Binding>& bindings, const THashMap<TString, YandexQuery::Connection>& connections, NSQLTranslation::TTranslationSettings& sqlSettings);

} //NYq
