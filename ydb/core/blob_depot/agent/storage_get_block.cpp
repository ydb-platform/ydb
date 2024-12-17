#include "agent_impl.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvGetBlock>(std::unique_ptr<IEventHandle> ev) {
        class TGetBlockQuery : public TBlobStorageQuery<TEvBlobStorage::TEvGetBlock> {
            ui32 BlockedGeneration = 0;
            ui32 RetriesRemain = 10;

        private:
            void TryGetBlock() {
                const auto status = Agent.BlocksManager.CheckBlockForTablet(Request.TabletId, std::nullopt, this, &BlockedGeneration);
                if (status == NKikimrProto::OK) {
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvGetBlockResult>(NKikimrProto::OK, Request.TabletId, BlockedGeneration));
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "BlobDepot tablet is unreachable");
                }
            }

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                TryGetBlock();
            }

            void OnUpdateBlock() override {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA52, "OnUpdateBlock", (AgentId, Agent.LogId),
                    (QueryId, GetQueryId()));

                if (!--RetriesRemain) {
                    EndWithError(NKikimrProto::ERROR, "too many retries to get blocked generation");
                    return;
                }
                TryGetBlock();
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse /*response*/) override {
                Y_DEBUG_ABORT("ProcessResponse must not be called for TGetBlockQuery");
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                BDEV_QUERY(BDEV23, "TEvGetBlock_end", (Status, status), (ErrorReason, errorReason));
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            void EndWithSuccess(std::unique_ptr<TEvBlobStorage::TEvGetBlockResult> result) {
                BDEV_QUERY(BDEV24, "TEvGetBlock_end", (Status, NKikimrProto::OK), (TabletId, Request.TabletId), (Generation, BlockedGeneration));
                TBlobStorageQuery::EndWithSuccess(std::move(result));
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };
        return new TGetBlockQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
