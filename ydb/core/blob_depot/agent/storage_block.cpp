#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvBlock>(std::unique_ptr<IEventHandle> ev) {
        class TBlockQuery : public TQuery {
            struct TBlockContext : TRequestContext {
                TMonotonic Timestamp;

                TBlockContext(TMonotonic timestamp)
                    : Timestamp(timestamp)
                {}
            };

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvBlock>();

                // lookup existing blocks to try fail-fast
                const ui32 blockedGeneration = Agent.GetBlockForTablet(msg.TabletId);
                if (msg.Generation <= blockedGeneration) {
                    // we don't consider ExpirationTimestamp here because blocked generation may only increase
                    return EndWithError(NKikimrProto::RACE, "block race detected");
                }

                // issue request to the tablet
                NKikimrBlobDepot::TEvBlock block;
                block.SetTabletId(msg.TabletId);
                block.SetBlockedGeneration(msg.Generation);
                Agent.Issue(std::move(block), this, std::make_shared<TBlockContext>(TActivationContext::Monotonic()));
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvBlockResult*>(&response)) {
                    return HandleBlockResult(std::move(context), **p);
                } else {
                    Y_FAIL();
                }
            }

            void HandleBlockResult(TRequestContext::TPtr context, TEvBlobDepot::TEvBlockResult& msg) {
                if (!msg.Record.HasStatus()) {
                    EndWithError(NKikimrProto::ERROR, "incorrect TEvBlockResult response");
                } else if (const auto status = msg.Record.GetStatus(); status != NKikimrProto::OK) {
                    EndWithError(status, msg.Record.GetErrorReason());
                } else {
                    // update blocks cache
                    auto& blockContext = context->Obtain<TBlockContext>();
                    auto& query = *Event->Get<TEvBlobStorage::TEvBlock>();
                    Agent.SetBlockForTablet(query.TabletId, query.Generation, blockContext.Timestamp +
                        TDuration::MilliSeconds(msg.Record.GetTimeToLiveMs()));
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvBlockResult>(NKikimrProto::OK));
                }
            }
        };

        return new TBlockQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
