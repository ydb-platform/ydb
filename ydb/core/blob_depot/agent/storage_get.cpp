#include "agent_impl.h"
#include "blob_mapping_cache.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvGet>(std::unique_ptr<IEventHandle> ev) {
        class TGetQuery : public TBlobStorageQuery<TEvBlobStorage::TEvGet> {
            std::unique_ptr<TEvBlobStorage::TEvGetResult> Response;
            ui32 AnswersRemain;

            struct TResolveKeyContext : TRequestContext {
                ui32 QueryIdx;

                TResolveKeyContext(ui32 queryIdx)
                    : QueryIdx(queryIdx)
                {}
            };

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::BLOB_DEPOT_EVENTS)) {
                    for (ui32 i = 0; i < Request.QuerySize; ++i) {
                        const auto& q = Request.Queries[i];
                        BDEV_QUERY(BDEV19, "TEvGet_new", (U.BlobId, q.Id), (U.Shift, q.Shift), (U.Size, q.Size),
                            (U.MustRestoreFirst, Request.MustRestoreFirst), (U.IsIndexOnly, Request.IsIndexOnly));
                    }
                }

                Response = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, Request.QuerySize,
                    TGroupId::FromValue(Agent.VirtualGroupId));
                AnswersRemain = Request.QuerySize;

                if (Request.ReaderTabletData) {
                    auto status = Agent.BlocksManager.CheckBlockForTablet(Request.ReaderTabletData->Id, Request.ReaderTabletData->Generation, this, nullptr);
                    if (status == NKikimrProto::BLOCKED) {
                        EndWithError(status, "Fail TEvGet due to BLOCKED tablet generation");
                        return;
                    }
                }

                for (ui32 i = 0; i < Request.QuerySize; ++i) {
                    auto& query = Request.Queries[i];

                    auto& response = Response->Responses[i];
                    response.Id = query.Id;
                    response.Shift = query.Shift;
                    response.RequestedSize = query.Size;

                    TString blobId = query.Id.AsBinaryString();
                    if (const TResolvedValue *value = Agent.BlobMappingCache.ResolveKey(blobId, this,
                            std::make_shared<TResolveKeyContext>(i), Request.MustRestoreFirst)) {
                        if (!ProcessSingleResult(i, value)) {
                            return; // error occured
                        }
                    } else {
                        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA29, "resolve pending", (AgentId, Agent.LogId),
                            (QueryId, GetQueryId()), (QueryIdx, i), (BlobId, query.Id));
                    }
                }

                CheckAndFinish();
            }

            bool ProcessSingleResult(ui32 queryIdx, const TKeyResolved& result) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA27, "ProcessSingleResult", (AgentId, Agent.LogId),
                    (QueryId, GetQueryId()), (QueryIdx, queryIdx), (Result, result));

                auto& r = Response->Responses[queryIdx];
                Y_ABORT_UNLESS(r.Status == NKikimrProto::UNKNOWN);
                if (result.Error()) {
                    r.Status = NKikimrProto::ERROR;
                    --AnswersRemain;
                } else if (const TResolvedValue *value = result.GetResolvedValue(); !value) {
                    r.Status = NKikimrProto::NODATA;
                    --AnswersRemain;
                } else if (Request.IsIndexOnly) {
                    r.Status = NKikimrProto::OK;
                    --AnswersRemain;
                } else {
                    Y_ABORT_UNLESS(Request.MustRestoreFirst <= value->ReliablyWritten);
                    TReadArg arg{
                        *value,
                        Request.GetHandleClass,
                        Request.MustRestoreFirst,
                        Request.Queries[queryIdx].Shift,
                        Request.Queries[queryIdx].Size,
                        queryIdx,
                        Request.ReaderTabletData,
                        Request.Queries[queryIdx].Id.AsBinaryString(),
                    };
                    TString error;
                    const bool success = IssueRead(std::move(arg), error);
                    if (!success) {
                        EndWithError(NKikimrProto::ERROR, std::move(error));
                        return false;
                    }
                }
                return true;
            }

            void OnRead(ui64 tag, TReadOutcome&& outcome) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA35, "OnRead", (AgentId, Agent.LogId), (QueryId, GetQueryId()),
                    (Tag, tag), (Outcome, outcome));

                auto& resp = Response->Responses[tag];
                Y_ABORT_UNLESS(resp.Status == NKikimrProto::UNKNOWN);
                std::visit(TOverloaded{
                    [&](TReadOutcome::TOk& ok) {
                        resp.Status = NKikimrProto::OK;
                        resp.Buffer = std::move(ok.Data);
                    },
                    [&](TReadOutcome::TNodata& /*nodata*/) {
                        resp.Status = NKikimrProto::NODATA;
                    },
                    [&](TReadOutcome::TError& error) {
                        resp.Status = error.Status;
                        if (Response->ErrorReason) {
                            Response->ErrorReason += ", ";
                        }
                        Response->ErrorReason += error.ErrorReason;
                    }
                }, outcome.Value);
                --AnswersRemain;
                CheckAndFinish();
            }

            void CheckAndFinish() {
                if (!AnswersRemain) {
                    if (!Request.IsIndexOnly) {
                        for (size_t i = 0, count = Response->ResponseSz; i < count; ++i) {
                            const auto& item = Response->Responses[i];
                            if (item.Status == NKikimrProto::OK) {
                                Y_VERIFY_S(item.Buffer.size() == item.RequestedSize ? Min(item.RequestedSize,
                                    item.Id.BlobSize() - Min(item.Id.BlobSize(), item.Shift)) : item.Id.BlobSize(),
                                    "Id# " << item.Id << " Shift# " << item.Shift << " RequestedSize# " << item.RequestedSize
                                    << " Buffer.size# " << item.Buffer.size());
                            }
                        }
                    }
                    EndWithSuccess();
                }
            }

            void EndWithSuccess() {
                TraceResponse(std::nullopt);
                TBlobStorageQuery::EndWithSuccess(std::move(Response));
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                TraceResponse(status);
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            void TraceResponse(std::optional<NKikimrProto::EReplyStatus> status) {
                if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::BLOB_DEPOT_EVENTS)) {
                    for (ui32 i = 0; i < Response->ResponseSz; ++i) {
                        const auto& r = Response->Responses[i];
                        BDEV_QUERY(BDEV20, "TEvGet_end", (BlobId, r.Id), (Shift, r.Shift),
                            (RequestedSize, r.RequestedSize), (Status, status.value_or(r.Status)),
                            (Buffer.size, r.Buffer.size()));
                    }
                }
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TKeyResolved>(&response)) {
                    ProcessSingleResult(context->Obtain<TResolveKeyContext>().QueryIdx, *p);
                    CheckAndFinish();
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    TQuery::HandleGetResult(context, **p);
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
                    TQuery::HandleResolveResult(context, **p);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    if (auto *resolveContext = dynamic_cast<TResolveKeyContext*>(context.get())) {
                        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA26, "TTabletDisconnected", (AgentId, Agent.LogId),
                            (QueryId, GetQueryId()), (QueryIdx, resolveContext->QueryIdx));
                        Response->Responses[resolveContext->QueryIdx].Status = NKikimrProto::ERROR;
                        --AnswersRemain;
                        CheckAndFinish();
                    }
                } else {
                    Y_ABORT();
                }
            }

            ui64 GetTabletId() const override {
                ui64 value = 0;
                for (ui32 i = 0; i < Request.QuerySize; ++i) {
                    auto& req = Request.Queries[i];
                    if (value && value != req.Id.TabletID()) {
                        return 0;
                    }
                    value = req.Id.TabletID();
                }
                return value;
            }
        };

        return new TGetQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
