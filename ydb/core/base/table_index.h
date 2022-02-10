#pragma once

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NTableIndex {

struct TTableColumns {
    THashSet<TString> Columns;
    TVector<TString> Keys;
};

struct TIndexColumns {
    TVector<TString> KeyColumns;
    TVector<TString> DataColumns;
};

bool IsCompatibleIndex(const TTableColumns& table, const TIndexColumns& index, TString& explain);
TTableColumns CalcTableImplDescription(const TTableColumns& table, const TIndexColumns &index);

}
}
