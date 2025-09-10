#pragma once

#include "defs.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NFulltext {

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error);
Ydb::Table::FulltextIndexSettings FillSettings(const TString& column, const TVector<std::pair<TString, TString>>& values, TString& error);

}
