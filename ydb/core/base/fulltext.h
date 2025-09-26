#pragma once

#include "defs.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NFulltext {

TVector<TString> Analyze(const TString& text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings);

bool ValidateColumnsMatches(const NProtoBuf::RepeatedPtrField<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);
bool ValidateColumnsMatches(const TVector<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error);
bool FillSetting(Ydb::Table::FulltextIndexSettings& settings, const TString& name, const TString& value, TString& error);

}
