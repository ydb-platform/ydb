#include "agent_impl.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvGetBlock>(std::unique_ptr<IEventHandle> ev) {
        class TGetBlockQuery : public TBlobStorageQuery<TEvBlobStorage::TEvGetBlock> {
        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                // TODO: implement GetBlock logic for BlobDepot
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                // TODO: implement GetBlock logic for BlobDepot
            }
        };
        return new TGetBlockQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
