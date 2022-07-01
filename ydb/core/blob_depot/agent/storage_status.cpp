#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvStatus>(std::unique_ptr<IEventHandle> ev) {
        class TStatusQuery : public TQuery {
        public:
            using TQuery::TQuery;

            void Initiate() override {
                EndWithSuccess(std::make_unique<TEvBlobStorage::TEvStatusResult>(NKikimrProto::OK,
                    Agent.GetStorageStatusFlags()));
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse /*response*/) override {
                Y_FAIL();
            }
        };

        return new TStatusQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
