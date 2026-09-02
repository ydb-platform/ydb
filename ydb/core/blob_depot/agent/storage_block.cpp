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
                block.SetVersion(Request.Version);
                block.SetWriteSourceOp(WriteSourceToProto(Request.WriteSource));
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
                } else if (const auto status = msg.Record.GetStatus(); status != NKikimrProto::OK) {
                    EndWithError(status, msg.Record.GetErrorReason(),
                        msg.Record.GetIsTabletStorageInfoVersionObsolete());
                } else {
                    // update blocks cache
                    auto& blockContext = context->Obtain<TBlockContext>();
                    if (Request.WriteSource != TWriteSource::SyncerMergeBlock) {
                        Agent.BlocksManager.SetBlockForTablet(Request.TabletId, Request.Generation,
                            blockContext.Timestamp, TDuration::MilliSeconds(msg.Record.GetTimeToLiveMs()));
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
