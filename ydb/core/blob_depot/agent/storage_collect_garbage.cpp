#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvCollectGarbage>(std::unique_ptr<IEventHandle> ev) {
        class TCollectGarbageQuery : public TQuery {
            ui32 BlockChecksRemain = 3;

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvCollectGarbage>();

                const auto status = Agent.CheckBlockForTablet(msg.TabletId, msg.RecordGeneration, this);
                if (status == NKikimrProto::OK) {
                    IssueCollectGarbage();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "block race detected");
                } else if (!--BlockChecksRemain) {
                    EndWithError(NKikimrProto::ERROR, "failed to acquire blocks");
                }
            }

            void IssueCollectGarbage() {
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse response) override {
                (void)response;
                Y_FAIL();
            }
        };

        return new TCollectGarbageQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
