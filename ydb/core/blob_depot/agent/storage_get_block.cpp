#include "agent_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvGetBlock>(std::unique_ptr<IEventHandle> ev,
            TMonotonic received) {
        class TGetBlockQuery : public TBlobStorageQuery<TEvBlobStorage::TEvGetBlock> {
            ui32 BlockedGeneration = 0;

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                if (CheckBlockForTablet(Request.TabletId, std::nullopt, &BlockedGeneration) == NKikimrProto::OK) {
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvGetBlockResult>(NKikimrProto::OK,
                        Request.TabletId, BlockedGeneration));
                }
            }

            void OnUpdateBlock() override {
                YDB_LOG_DEBUG("OnUpdateBlock",
                    {"marker", "BDA52"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()});
                Initiate();
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr /*context*/, TResponse /*response*/) override {
                Y_DEBUG_ABORT("ProcessResponse must not be called for TGetBlockQuery");
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvGetBlock_end",
                    {"marker", "BDEV23"},
                    {"VG", Agent.VirtualGroupId},
                    {"BDT", Agent.TabletId},
                    {"G", Agent.BlobDepotGeneration},
                    {"Q", QueryId},
                    {"status", status},
                    {"errorReason", errorReason});
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            void EndWithSuccess(std::unique_ptr<TEvBlobStorage::TEvGetBlockResult> result) {
                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvGetBlock_end",
                    {"marker", "BDEV24"},
                    {"VG", Agent.VirtualGroupId},
                    {"BDT", Agent.TabletId},
                    {"G", Agent.BlobDepotGeneration},
                    {"Q", QueryId},
                    {"status", NKikimrProto::OK},
                    {"tabletId", Request.TabletId},
                    {"generation", BlockedGeneration});
                TBlobStorageQuery::EndWithSuccess(std::move(result));
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };
        return new TGetBlockQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
