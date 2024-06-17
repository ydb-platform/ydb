#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NReplication::NService {

struct TLightweightSchema: public TThrRefBase {
    using TPtr = TIntrusivePtr<TLightweightSchema>;
    using TCPtr = TIntrusiveConstPtr<TLightweightSchema>;

    struct TColumn {
        NTable::TTag Tag;
        NScheme::TTypeInfo Type;
    };

    TVector<NScheme::TTypeInfo> KeyColumns;
    THashMap<TString, TColumn> ValueColumns;
    ui64 Version = 0;
};

}
