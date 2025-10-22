#include "agent_impl.h"
#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvCheckIntegrity>(
            std::unique_ptr<IEventHandle> ev, TMonotonic received) {

        class TCheckIntegrityQuery : public TBlobStorageQuery<TEvBlobStorage::TEvCheckIntegrity> {
        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::BLOB_DEPOT_EVENTS)) {
                    BDEV_QUERY(BDEV25, "TEvCheckIntegrity_new", (U.BlobId, Request.Id));
                }

                TString blobId = Request.Id.AsBinaryString();

                if (const TResolvedValue *value = Agent.BlobMappingCache.ResolveKey(blobId, this,
                        std::make_shared<TRequestContext>(), false)) {
                    ProcessResolveResult(value);
                } else {
                    STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA58, "resolve pending", (AgentId, Agent.LogId),
                        (QueryId, GetQueryId()), (BlobId, Request.Id));
                }
            }

            void ProcessResolveResult(const TKeyResolved& result) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA59, "ProcessResolveResult", (AgentId, Agent.LogId),
                    (QueryId, GetQueryId()), (Result, result));

                if (result.Error()) {
                    EndWithError(NKikimrProto::ERROR, result.GetErrorReason());
                } else if (const TResolvedValue *value = result.GetResolvedValue(); !value) {
                    EndWithError(NKikimrProto::NODATA, "no data");
                } else {
                    TReadArg arg{
                        *value,
                        Request.GetHandleClass,
                        false,
                        0,
                        Request.Id.BlobSize(),
                        0,
                        std::nullopt,
                        Request.Id.AsBinaryString(),
                    };
                    IssueCheckIntegrity(std::move(arg));
                }
            }

            void OnCheckIntegrity(TCheckOutcome&& outcome) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA60, "OnCheckIntegrity", (AgentId, Agent.LogId), (QueryId, GetQueryId()),
                    (Outcome, outcome));
                TraceResponse(outcome.Result->Status);
                TBlobStorageQuery::EndWithSuccess(std::move(outcome.Result));
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                TraceResponse(status);
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            void TraceResponse(NKikimrProto::EReplyStatus status) {
                if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::BLOB_DEPOT_EVENTS)) {
                    BDEV_QUERY(BDEV26, "TEvCheckIntegrity_end", (BlobId, Request.Id), (Status, status));
                }
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TKeyResolved>(&response)) {
                    ProcessResolveResult(*p);
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvCheckIntegrityResult*>(&response)) {
                    TQuery::HandleCheckIntegrityResult(context, **p);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA61, "TTabletDisconnected",
                        (AgentId, Agent.LogId), (QueryId, GetQueryId()));
                    EndWithError(NKikimrProto::ERROR, "Tablet disconnected");
                } else {
                    Y_ABORT();
                }
            }

            ui64 GetTabletId() const override {
                return Request.Id.TabletID();
            }
        };

        return new TCheckIntegrityQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
