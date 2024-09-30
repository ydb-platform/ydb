#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/memory/pool.h>

namespace NKikimr::NFormats {

bool MakeCell(TCell& cell, TStringBuf value, const NScheme::TTypeInfo& typeInfo, TMemoryPool& pool, TString& err);
bool MakeCell(TCell& cell, const NJson::TJsonValue& value, const NScheme::TTypeInfo& typeInfo, TMemoryPool& pool, TString& err);
bool CheckCellValue(const TCell& cell, const NScheme::TTypeInfo& typeInfo);

}
