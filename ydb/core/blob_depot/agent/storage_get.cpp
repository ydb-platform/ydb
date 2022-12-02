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
                if (Request.Decommission) {
                    // just forward this message to underlying proxy
                    Y_VERIFY(Agent.ProxyId);
                    const bool sent = TActivationContext::Send(Event->Forward(Agent.ProxyId));
                    Y_VERIFY(sent);
                    delete this;
                    return;
                }

                Response = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, Request.QuerySize,
                    Agent.VirtualGroupId);
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
                    if (const TResolvedValueChain *value = Agent.BlobMappingCache.ResolveKey(blobId, this,
                            std::make_shared<TResolveKeyContext>(i))) {
                        if (!ProcessSingleResult(i, value, std::nullopt)) {
                            return;
                        }
                    } else {
                        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA29, "resolve pending", (VirtualGroupId, Agent.VirtualGroupId),
                            (QueryId, GetQueryId()), (QueryIdx, i), (BlobId, query.Id));
                    }
                }

                CheckAndFinish();
            }

            bool ProcessSingleResult(ui32 queryIdx, const TResolvedValueChain *value, const std::optional<TString>& errorReason) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA27, "ProcessSingleResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, GetQueryId()), (QueryIdx, queryIdx), (Value, value), (ErrorReason, errorReason));

                auto& r = Response->Responses[queryIdx];
                Y_VERIFY(r.Status == NKikimrProto::UNKNOWN);
                if (errorReason) {
                    r.Status = NKikimrProto::ERROR;
                    --AnswersRemain;
                } else if (!value) {
                    r.Status = NKikimrProto::NODATA;
                    --AnswersRemain;
                } else if (Request.IsIndexOnly) {
                    r.Status = NKikimrProto::OK;
                    --AnswersRemain;
                } else if (value) {
                    TReadArg arg{
                        *value,
                        Request.GetHandleClass,
                        Request.MustRestoreFirst,
                        this,
                        Request.Queries[queryIdx].Shift,
                        Request.Queries[queryIdx].Size,
                        queryIdx,
                        Request.ReaderTabletData};
                    TString error;
                    const bool success = Agent.IssueRead(arg, error);
                    if (!success) {
                        EndWithError(NKikimrProto::ERROR, std::move(error));
                        return false;
                    }
                }
                return true;
            }

            void OnRead(ui64 tag, NKikimrProto::EReplyStatus status, TString buffer) override {
                auto& resp = Response->Responses[tag];
                Y_VERIFY(resp.Status == NKikimrProto::UNKNOWN);
                resp.Status = status;
                if (status == NKikimrProto::OK) {
                    resp.Buffer = std::move(buffer);
                }
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
                    EndWithSuccess(std::move(Response));
                }
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TKeyResolved>(&response)) {
                    ProcessSingleResult(context->Obtain<TResolveKeyContext>().QueryIdx, p->ValueChain, p->ErrorReason);
                    CheckAndFinish();
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    Agent.HandleGetResult(context, **p);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    if (auto *resolveContext = dynamic_cast<TResolveKeyContext*>(context.get())) {
                        Response->Responses[resolveContext->QueryIdx].Status = NKikimrProto::ERROR;
                        --AnswersRemain;
                        CheckAndFinish();
                    }
                } else {
                    Y_FAIL();
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
