#pragma once

#include "defs.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NFulltext {

TVector<TString> Analyze(const TString& text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings);

bool ValidateSettings(const NProtoBuf::RepeatedPtrField<TString>& keyColumns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);
Ydb::Table::FulltextIndexSettings FillSettings(const TString& keyColumn, const TVector<std::pair<TString, TString>>& values, TString& error);

}
