#include "agent_impl.h"
#include "blob_mapping_cache.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvGet>(std::unique_ptr<IEventHandle> ev) {
        class TGetQuery : public TQuery {
            std::unique_ptr<TEvBlobStorage::TEvGetResult> Response;
            ui32 AnswersRemain;

            struct TResolveKeyContext : TRequestContext {
                ui32 QueryIdx;

                TResolveKeyContext(ui32 queryIdx)
                    : QueryIdx(queryIdx)
                {}
            };

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = GetQuery();

                if (msg.Decommission) {
                    // just forward this message to underlying proxy
                    Y_VERIFY(Agent.ProxyId);
                    const bool sent = TActivationContext::Send(Event->Forward(Agent.ProxyId));
                    Y_VERIFY(sent);
                    delete this;
                    return;
                }

                Response = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, msg.QuerySize,
                    Agent.VirtualGroupId);
                AnswersRemain = msg.QuerySize;

                if (msg.ReaderTabletData) {
                    auto status = Agent.BlocksManager.CheckBlockForTablet(msg.ReaderTabletData->Id, msg.ReaderTabletData->Generation, this, nullptr);
                    if (status == NKikimrProto::BLOCKED) {
                        EndWithError(status, "Fail TEvGet due to BLOCKED tablet generation");
                        return;
                    }
                }

                for (ui32 i = 0; i < msg.QuerySize; ++i) {
                    auto& query = msg.Queries[i];

                    auto& response = Response->Responses[i];
                    response.Id = query.Id;
                    response.Shift = query.Shift;
                    response.RequestedSize = query.Size;

                    TString blobId = query.Id.AsBinaryString();
                    if (const TResolvedValueChain *value = Agent.BlobMappingCache.ResolveKey(blobId, this,
                            std::make_shared<TResolveKeyContext>(i))) {
                        if (!ProcessSingleResult(i, value)) {
                            return;
                        }
                    }
                }
            }

            bool ProcessSingleResult(ui32 queryIdx, const TResolvedValueChain *value) {
                auto& msg = GetQuery();
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA27, "ProcessSingleResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, GetQueryId()), (QueryIdx, queryIdx), (Value, value));

                if (!value) {
                    Response->Responses[queryIdx].Status = NKikimrProto::NODATA;
                    --AnswersRemain;
                } else if (msg.IsIndexOnly) {
                    Response->Responses[queryIdx].Status = NKikimrProto::OK;
                    --AnswersRemain;
                } else if (value) {
                    TReadArg arg{
                        *value,
                        msg.GetHandleClass,
                        msg.MustRestoreFirst,
                        this,
                        msg.Queries[queryIdx].Shift,
                        msg.Queries[queryIdx].Size,
                        queryIdx,
                        msg.ReaderTabletData};
                    TString error;
                    const bool success = Agent.IssueRead(arg, error);
                    if (!success) {
                        EndWithError(NKikimrProto::ERROR, std::move(error));
                        return false;
                    }
                }
                if (!AnswersRemain) {
                    EndWithSuccess(std::move(Response));
                    return false;
                }
                return true;
            }

            void OnRead(ui64 tag, NKikimrProto::EReplyStatus status, TString buffer) override {
                auto& resp = Response->Responses[tag];
                resp.Status = status;
                if (status == NKikimrProto::OK) {
                    resp.Buffer = std::move(buffer);
                }
                if (!--AnswersRemain) {
                    EndWithSuccess(std::move(Response));
                }
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TKeyResolved>(&response)) {
                    ProcessSingleResult(context->Obtain<TResolveKeyContext>().QueryIdx, p->ValueChain);
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    Agent.HandleGetResult(context, **p);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    if (auto *resolveContext = dynamic_cast<TResolveKeyContext*>(context.get())) {
                        Response->Responses[resolveContext->QueryIdx].Status = NKikimrProto::ERROR;
                        if (!--AnswersRemain) {
                            EndWithSuccess(std::move(Response));
                        }
                    }
                } else {
                    Y_FAIL();
                }
            }

            TEvBlobStorage::TEvGet& GetQuery() const {
                return *Event->Get<TEvBlobStorage::TEvGet>();
            }
        };

        return new TGetQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
