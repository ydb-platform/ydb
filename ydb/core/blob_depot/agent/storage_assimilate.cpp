#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvAssimilate>(std::unique_ptr<IEventHandle> ev) {
        class TAssimilateQuery : public TBlobStorageQuery<TEvBlobStorage::TEvAssimilate> {
        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                Y_VERIFY(Agent.ProxyId);
                const bool sent = TActivationContext::Send(Event->Forward(Agent.ProxyId));
                Y_VERIFY(sent);
                delete this;
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse /*response*/) override {
                Y_FAIL();
            }
        };

        return new TAssimilateQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
