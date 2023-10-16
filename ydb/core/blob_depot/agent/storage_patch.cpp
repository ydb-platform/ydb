#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvPatch>(std::unique_ptr<IEventHandle> ev) {
        class TPatchQuery : public TBlobStorageQuery<TEvBlobStorage::TEvPatch> {
        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                EndWithError(NKikimrProto::ERROR, "not implemented");
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse response) override {
                (void)response;
                Y_ABORT();
            }
        };

        return new TPatchQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
