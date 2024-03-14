#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/scheme_board/populator.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxInitPopulator : public TTransactionBase<TSchemeShard> {
    using TDescription = NSchemeBoard::TTwoPartDescription;

    std::vector<std::pair<TPathId, TDescription>> Descriptions;
    TSideEffects::TPublications DelayedPublications;

    explicit TTxInitPopulator(TSelf *self, TSideEffects::TPublications&& publications)
        : TBase(self)
        , DelayedPublications(publications)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_INIT_POPULATOR; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(Self->IsSchemeShardConfigured());

        for (auto item: Self->PathsById) {
            TPathId pathId = item.first;
            TPathElement::TPtr pathEl = item.second;

            // KIKIMR-13173
            // planned to drop paths are added to populator
            // also such paths (like BSV PQ) can be considered as deleted by path description however they aren't dropped jet

            if (pathEl->Dropped())   {
                // migrated paths should be published directly
                // but once they published on the quorum replicas as deleted directly, further publishing is unnecessary
                // actually they are publishing strongly on the quorum as part of deleting operation
                // so it's ok to skip migrated paths is they has been marked as deleted
                continue;
            }

            if (!Self->PathIsActive(pathId)) {
                continue;
            }

            auto result = DescribePath(Self, ctx, pathId);
            TDescription description(std::move(result->PreSerializedData), std::move(result->Record));
            Descriptions.emplace_back(pathId, std::move(description));
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const ui64 tabletId = Self->TabletID();

        IActor* populator = CreateSchemeBoardPopulator(
            tabletId, Self->Generation(),
            std::move(Descriptions), Self->NextLocalPathId
        );

        Y_ABORT_UNLESS(!Self->SchemeBoardPopulator);
        Self->SchemeBoardPopulator = Self->Register(populator);

        Self->PublishToSchemeBoard(std::move(DelayedPublications), ctx);
        Self->SignalTabletActive(ctx);
    }

}; // TSchemeShard::TTxInitPopulator

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInitPopulator(TSideEffects::TPublications&& publications) {
    return new TTxInitPopulator(this, std::move(publications));
}

} // NSchemeShard
} // NKikimr
