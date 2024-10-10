#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/string.h>

namespace NKikimr::NDataShard {

TString DecimalToString(const std::pair<ui64, i64>& loHi, const NScheme::TTypeInfo& typeInfo);
TString DyNumberToString(TStringBuf data);
TString PgToString(TStringBuf data, const NScheme::TTypeInfo& typeInfo);
bool DecimalToStream(const std::pair<ui64, i64>& loHi, IOutputStream& out, TString& err, const NScheme::TTypeInfo& typeInfo);
bool DyNumberToStream(TStringBuf data, IOutputStream& out, TString& err);
bool PgToStream(TStringBuf data, const NScheme::TTypeInfo& typeInfo, IOutputStream& out, TString& err);
bool UuidToStream(const std::pair<ui64, ui64>& loHi, IOutputStream& out, TString& err);

} // NKikimr::NDataShard
