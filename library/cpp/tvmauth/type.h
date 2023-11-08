#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/hash.h>

namespace NTvmAuth {
    struct TUserExtFields;

    using TScopes = TSmallVec<TStringBuf>;
    using TTvmId = ui32;
    using TUid = ui64;
    using TUids = TSmallVec<TUid>;
    using TUidsExtFieldsMap = THashMap<TUid, TUserExtFields>;
    using TAlias = TString;
    using TPorgId = ui64;

    struct TUserExtFields {
        bool operator==(const TUserExtFields& o) const {
            return Uid == o.Uid &&
                   CurrentPorgId == o.CurrentPorgId;
        }

        TUid Uid = 0;
        TPorgId CurrentPorgId = 0;
    };
}
