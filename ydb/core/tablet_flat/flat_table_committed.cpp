#include "flat_table_committed.h"

namespace NKikimr::NTable {

    class TEmptyTransactionSet : public ITransactionSet {
    public:
        bool Contains(ui64) const {
            return false;
        }
    };

    // Note: binding to reference extends the object lifetime
    const ITransactionSet& ITransactionSet::None = TEmptyTransactionSet();

} // namespace NKikimr::NTable
