#include "mediator.h"
#include "mediator_impl.h"

#include <util/generic/intrlist.h>

namespace NKikimr {
namespace NTxMediator {

    TCoordinatorStep::TCoordinatorStep(const NKikimrTx::TEvCoordinatorStep &record)
        : Step(record.GetStep())
        , PrevStep(record.GetPrevStep())
    {
        const std::size_t txsize = record.TransactionsSize();

        // todo: save body as-is, without any processing
        // and defer parsing and per-tablet mapping for latter stage, when we could merge all selected steps
        // (for mediator exec queue, to split latency sensitive stuff from calculations)

        Transactions.reserve(txsize);
        TabletsToTransaction.reserve(record.GetTotalTxAffectedEntries());

        for (std::size_t i = 0; i != txsize; ++i) {
            const NKikimrTx::TCoordinatorTransaction &c = record.GetTransactions(i);

            Transactions.emplace_back(
                    c.HasModerator() ? c.GetModerator() : 0,
                    c.GetTxId()
                );

            for (ui32 tabi = 0, tabe = c.GetAffectedSet().size(); tabi != tabe; ++tabi)
                TabletsToTransaction.emplace_back(c.GetAffectedSet(tabi), i);
        }
    }

}

IActor* CreateTxMediator(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NTxMediator::TTxMediator(info, tablet);
}

}
