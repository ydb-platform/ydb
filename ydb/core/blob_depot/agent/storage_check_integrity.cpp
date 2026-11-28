#include "agent_impl.h"
#include "blob_mapping_cache.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvCheckIntegrity>(
            std::unique_ptr<IEventHandle> ev, TMonotonic received) {

        class TCheckIntegrityQuery : public TBlobStorageQuery<TEvBlobStorage::TEvCheckIntegrity> {
            struct TLookup : TRequestContext {};

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvCheckIntegrity_new",
                    {"marker", "BDEV25"},
                    {"VG", Agent.VirtualGroupId},
                    {"BDT", Agent.TabletId},
                    {"G", Agent.BlobDepotGeneration},
                    {"Q", QueryId},
                    {"U.BlobId", Request.Id});

                TString blobId = Request.Id.AsBinaryString();

                if (Agent.ManagedBridgeChild) {
                    NKikimrBlobDepot::TEvResolve resolve;
                    resolve.AddItems()->SetExactKey(blobId);
                    SetResolveParameters(resolve, Request.DataKind);
                    Agent.Issue(std::move(resolve), this, std::make_shared<TLookup>());
                } else if (const TResolvedValue *value = Agent.BlobMappingCache.ResolveKey(blobId, this,
                        std::make_shared<TRequestContext>(), false, Request.DataKind)) {
                    ProcessResolveResult(value);
                } else {
                    YDB_LOG_DEBUG("Resolve pending",
                        {"marker", "BDA58"},
                        {"agentId", Agent.LogId},
                        {"queryId", GetQueryId()},
                        {"blobId", Request.Id});
                }
            }

            void ProcessResolveResult(const TKeyResolved& result) {
                YDB_LOG_DEBUG("ProcessResolveResult",
                    {"marker", "BDA59"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()},
                    {"result", result});

                if (result.Error()) {
                    EndWithError(NKikimrProto::ERROR, result.GetErrorReason());
                } else if (const TResolvedValue *value = result.GetResolvedValue(); !value) {
                    if (Agent.ManagedBridgeChild) {
                        EndWithSuccess(Request.MakeErrorResponse(NKikimrProto::NODATA, "no data",
                            TGroupId::FromValue(Agent.VirtualGroupId)));
                    } else {
                        EndWithError(NKikimrProto::NODATA, "no data");
                    }
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
                        Request.DataKind,
                    };
                    IssueCheckIntegrity(std::move(arg));
                }
            }

            void OnCheckIntegrity(TCheckOutcome&& outcome) override {
                YDB_LOG_DEBUG("OnCheckIntegrity",
                    {"marker", "BDA60"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()},
                    {"outcome", outcome});
                TraceResponse(outcome.Result->Status);
                TBlobStorageQuery::EndWithSuccess(std::move(outcome.Result));
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                TraceResponse(status);
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            void TraceResponse(NKikimrProto::EReplyStatus status) {
                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvCheckIntegrity_end",
                    {"marker", "BDEV26"},
                    {"VG", Agent.VirtualGroupId},
                    {"BDT", Agent.TabletId},
                    {"G", Agent.BlobDepotGeneration},
                    {"Q", QueryId},
                    {"blobId", Request.Id},
                    {"status", status});
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TKeyResolved>(&response)) {
                    ProcessResolveResult(*p);
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvCheckIntegrityResult*>(&response)) {
                    TQuery::HandleCheckIntegrityResult(context, **p);
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
                    if (dynamic_cast<TLookup*>(context.get())) {
                        const auto& record = (*p)->Record;
                        if (record.GetStatus() != NKikimrProto::OK) {
                            return EndWithError(record.GetStatus(), record.GetErrorReason());
                        }
                        const auto *item = record.ResolvedKeysSize() ? &record.GetResolvedKeys(0) : nullptr;
                        if (item && item->HasErrorReason()) {
                            return EndWithError(NKikimrProto::ERROR, item->GetErrorReason());
                        }
                        if (item && item->ValueChainSize()) {
                            const TResolvedValue value(*item);
                            ProcessResolveResult(&value);
                        } else {
                            ProcessResolveResult(nullptr);
                        }
                    } else {
                        TQuery::HandleCheckIntegrityResolveResult(context, **p);
                    }
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    YDB_LOG_DEBUG("TTabletDisconnected",
                        {"marker", "BDA61"},
                        {"agentId", Agent.LogId},
                        {"queryId", GetQueryId()});
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
