#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvGet>(std::unique_ptr<IEventHandle> ev) {
        class TGetQuery : public TQuery {
        public:
            using TQuery::TQuery;

            void Initiate() override {
                EndWithError(NKikimrProto::ERROR, "not implemented");
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse response) override {
                (void)response;
                Y_FAIL();
            }
        };

        return new TGetQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
