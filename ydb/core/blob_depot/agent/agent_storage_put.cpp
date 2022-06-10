#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TExecutingQuery *TBlobDepotAgent::CreateExecutingQuery<TEvBlobStorage::EvPut>(std::unique_ptr<IEventHandle> ev) {
        class TPutExecutingQuery : public TExecutingQuery {
            ui32 BlockChecksRemain = 3;

        public:
            using TExecutingQuery::TExecutingQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();

                // first step -- check blocks
                switch (Agent.CheckBlockForTablet(msg.Id.TabletID(), msg.Id.Generation(), this)) {
                    case NKikimrProto::OK:
                        return IssuePuts();

                    case NKikimrProto::RACE:
                        return EndWithError(NKikimrProto::RACE, "block race detected");

                    case NKikimrProto::NOTREADY:
                        if (!--BlockChecksRemain) {
                            EndWithError(NKikimrProto::ERROR, "failed to acquire blocks");
                        }
                        return;

                    default:
                        Y_FAIL();
                }
            }

            void IssuePuts() {
            }

            void OnUpdateBlock() override {
                Initiate(); // just restart request
            }
        };

        return new TPutExecutingQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
