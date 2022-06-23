#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvPut>(std::unique_ptr<IEventHandle> ev) {
        class TPutQuery : public TQuery {
            ui32 BlockChecksRemain = 3;

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();

                // first step -- check blocks
                const auto status = Agent.CheckBlockForTablet(msg.Id.TabletID(), msg.Id.Generation(), this);
                if (status == NKikimrProto::OK) {
                    IssuePuts();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "block race detected");
                } else if (!--BlockChecksRemain) {
                    EndWithError(NKikimrProto::ERROR, "failed to acquire blocks");
                }
            }

            void IssuePuts() {
            }

            void OnUpdateBlock(bool success) override {
                if (success) {
                    Initiate(); // just restart request
                } else {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                }
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse response) override {
                (void)response;
                Y_FAIL();
            }
        };

        return new TPutQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
