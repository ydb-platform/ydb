#include "dummy.h"

namespace NSQLComplete {

    namespace {

        class TRanking: public IRanking {
        public:
            void CropToSortedPrefix(
                TVector<TGenericName>& names,
                const TNameConstraints& /* constraints */,
                size_t limit) const override {
                names.crop(limit);
            }
        };

    } // namespace

    IRanking::TPtr MakeDummyRanking() {
        return new TRanking();
    }

} // namespace NSQLComplete
