#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/memory/pool.h>

namespace NKikimr::NFormats {

bool MakeCell(TCell& cell, TStringBuf value, NScheme::TTypeInfo type, TMemoryPool& pool, TString& err);
bool MakeCell(TCell& cell, const NJson::TJsonValue& value, NScheme::TTypeInfo type, TMemoryPool& pool, TString& err);
bool CheckCellValue(const TCell& cell, NScheme::TTypeInfo type);

}
