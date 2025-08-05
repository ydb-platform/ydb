#pragma once

#include <yql/essentials/sql/v1/complete/name/service/ranking/frequency.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

    class IRanking: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IRanking>;

        ~IRanking() override = default;
        virtual void CropToSortedPrefix(
            TVector<TGenericName>& names,
            const TNameConstraints& constraints,
            size_t limit) const = 0;
    };

    // TODO(YQL-19747): Migrate YDB CLI to MakeDefaultRanking(...)
    IRanking::TPtr MakeDefaultRanking();

    IRanking::TPtr MakeDefaultRanking(const TFrequencyData& frequency);

} // namespace NSQLComplete
