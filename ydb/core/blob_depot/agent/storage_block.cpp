#include "agent_impl.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvBlock>(std::unique_ptr<IEventHandle> ev,
            TMonotonic received) {
        class TBlockQuery : public TBlobStorageQuery<TEvBlobStorage::TEvBlock> {
            struct TBlockContext : TRequestContext {
                TMonotonic Timestamp;

                TBlockContext(TMonotonic timestamp)
                    : Timestamp(timestamp)
                {}
            };

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                // issue request to the tablet
                NKikimrBlobDepot::TEvBlock block;
                block.SetTabletId(Request.TabletId);
                block.SetBlockedGeneration(Request.Generation);
                block.SetIssuerGuid(Request.IssuerGuid);
                if (Request.Version) {
                    block.SetVersion(*Request.Version);
                }
                block.SetWriteSourceOp(WriteSourceToProto(Request.WriteSource));
                if (ShouldSkipBridgeAccountingForSubrequests()) {
                    block.SetSkipBridgeAccountingForSubrequests(true);
                }
                Agent.Issue(std::move(block), this, std::make_shared<TBlockContext>(TActivationContext::Monotonic()));
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvBlockResult*>(&response)) {
                    return HandleBlockResult(std::move(context), **p);
                } else {
                    Y_ABORT();
                }
            }

            void HandleBlockResult(TRequestContext::TPtr context, TEvBlobDepot::TEvBlockResult& msg) {
                if (!msg.Record.HasStatus()) {
                    EndWithError(NKikimrProto::ERROR, "incorrect TEvBlockResult response");
                } else if (const auto status = msg.Record.GetStatus(); status != NKikimrProto::OK &&
                        !(Request.IsIdempotent && status == NKikimrProto::ALREADY)) {
                    EndWithError(status, msg.Record.GetErrorReason(),
                        msg.Record.GetIsTabletStorageInfoVersionObsolete(), msg.Record.GetActualGeneration());
                } else {
                    // update blocks cache
                    auto& blockContext = context->Obtain<TBlockContext>();
                    if (msg.Record.GetStatus() == NKikimrProto::OK &&
                            (Request.WriteSource != TWriteSource::SyncerMergeBlock || !(Request.TabletId >> 63))) {
                        // Only QueryBlocks registers a renewable read lease with the managed tablet.
                        const auto ttl = Agent.ManagedBridgeChild ? TDuration::Zero()
                            : TDuration::MilliSeconds(msg.Record.GetTimeToLiveMs());
                        Agent.BlocksManager.SetBlockForTablet(Request.TabletId, Request.Generation,
                            blockContext.Timestamp, ttl);
                    }
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvBlockResult>(NKikimrProto::OK));
                }
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };

        return new TBlockQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
