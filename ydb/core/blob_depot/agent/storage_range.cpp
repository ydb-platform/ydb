#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvRange>(std::unique_ptr<IEventHandle> ev) {
        class TRangeQuery : public TQuery {
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

        return new TRangeQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
