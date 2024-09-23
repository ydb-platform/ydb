#pragma once

#include "flat_row_eggs.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTable {

    struct TLead {
        void To(TTagsRef tags, TArrayRef<const TCell> key, ESeek seek)
        {
            Valid = true;
            Tags.assign(tags.begin(), tags.end());
            Relation = seek;
            Key = TSerializedCellVec(key);
            StopKey = { };
        }

        void Until(TArrayRef<const TCell> key, bool inclusive)
        {
            Y_ABORT_UNLESS(Valid, "Until must be called after To");
            StopKey = TSerializedCellVec(key);
            StopKeyInclusive = inclusive;
        }

        explicit operator bool() const noexcept
        {
            return Valid;
        }

        void Clear()
        {
            Valid = false;
        }

        bool Valid = false;
        ESeek Relation = ESeek::Exact;
        TVector<ui32> Tags;
        TSerializedCellVec Key;
        TSerializedCellVec StopKey;
        bool StopKeyInclusive = true;
    };

}
}
