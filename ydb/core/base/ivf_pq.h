#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NIvfPq {

bool FillSetting(Ydb::Table::IvfPqSettings& settings, const TString& name, const TString& value, TString& error);

bool ValidateSettings(const Ydb::Table::IvfPqSettings& settings, TString& error);

} // NKikimr::NIvfPq
