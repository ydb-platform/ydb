#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TExecutingQuery *TBlobDepotAgent::CreateExecutingQuery<TEvBlobStorage::EvBlock>(std::unique_ptr<IEventHandle> ev) {
        class TBlockExecutingQuery : public TExecutingQuery {
        public:
            using TExecutingQuery::TExecutingQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvBlock>();

                // lookup existing blocks to try fail-fast
                if (const auto it = Agent.Blocks.find(msg.TabletId); it != Agent.Blocks.end()) {
                    const TBlockInfo& block = it->second;
                    if (msg.Generation <= block.BlockedGeneration) {
                        // we don't consider ExpirationTimestamp here because blocked generation may only increase
                        return EndWithError(NKikimrProto::RACE, "block race detected");
                    }
                }

                // issue request to the tablet
                NKikimrBlobDepot::TEvBlock block;
                block.SetTabletId(msg.TabletId);
                block.SetBlockedGeneration(msg.Generation);
                Agent.Issue(std::move(block), this, std::bind(&TBlockExecutingQuery::HandleBlockResult, this,
                    std::placeholders::_1, TActivationContext::Monotonic()));
            }

            bool HandleBlockResult(IEventBase *event, TMonotonic issueTimestamp) {
                if (!event) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (const auto& msg = static_cast<TEvBlobDepot::TEvBlockResult&>(*event); !msg.Record.HasStatus()) {
                    EndWithError(NKikimrProto::ERROR, "incorrect TEvBlockResult response");
                } else if (const auto status = msg.Record.GetStatus(); status != NKikimrProto::OK) {
                    EndWithError(status, msg.Record.GetErrorReason());
                } else {
                    // update blocks cache
                    auto& query = *Event->Get<TEvBlobStorage::TEvBlock>();
                    auto& block = Agent.Blocks[query.TabletId];
                    Y_VERIFY(block.BlockedGeneration <= query.Generation); // TODO: QueryBlocks can race with Block?
                    block.BlockedGeneration = query.Generation;
                    block.ExpirationTimestamp = issueTimestamp + TDuration::MilliSeconds(msg.Record.GetTimeToLiveMs());

                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvBlockResult>(NKikimrProto::OK));
                }
                return true;
            }
        };

        return new TBlockExecutingQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
