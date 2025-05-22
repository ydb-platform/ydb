#pragma once

#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

    class IRanking: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IRanking>;

        virtual void CropToSortedPrefix(TVector<TGenericName>& names, size_t limit) const = 0;
        virtual ~IRanking() = default;
    };

    IRanking::TPtr MakeDefaultRanking();

    IRanking::TPtr MakeDefaultRanking(TFrequencyData frequency);

} // namespace NSQLComplete
