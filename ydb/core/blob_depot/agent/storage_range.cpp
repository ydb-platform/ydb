#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvRange>(std::unique_ptr<IEventHandle> ev) {
        class TRangeQuery : public TQuery {
            std::unique_ptr<TEvBlobStorage::TEvRangeResult> Response;
            ui32 ReadsInFlight = 0;
            ui32 ResolvesInFlight = 0;

            struct TExtraResolveContext : TRequestContext {
                const size_t Index;

                TExtraResolveContext(size_t index)
                    : Index(index)
                {}
            };

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = GetQuery();

                if (msg.Decommission) {
                    Y_VERIFY(Agent.ProxyId);
                    STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA26, "forwarding TEvRange", (VirtualGroupId, Agent.VirtualGroupId),
                        (TabletId, Agent.TabletId), (Msg, msg), (ProxyId, Agent.ProxyId));
                    const bool sent = TActivationContext::Send(Event->Forward(Agent.ProxyId));
                    Y_VERIFY(sent);
                    delete this;
                    return;
                }

                Response = std::make_unique<TEvBlobStorage::TEvRangeResult>(NKikimrProto::OK, msg.From, msg.To,
                    Agent.VirtualGroupId);

                IssueResolve();
            }

            void IssueResolve() {
                auto& msg = GetQuery();

                TString from = msg.From.AsBinaryString();
                TString to = msg.To.AsBinaryString();
                const bool reverse = msg.To < msg.From;
                if (reverse) {
                    std::swap(from, to);
                }

                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                auto *range = item->MutableKeyRange();
                range->SetBeginningKey(from);
                range->SetIncludeBeginning(true);
                range->SetEndingKey(to);
                range->SetIncludeEnding(true);
                range->SetReverse(reverse);
                item->SetTabletId(msg.TabletId);
                item->SetMustRestoreFirst(msg.MustRestoreFirst);

                Agent.Issue(std::move(resolve), this, nullptr);
                ++ResolvesInFlight;
            }

            void IssueResolve(TLogoBlobID id, size_t index) {
                auto& msg = GetQuery();
                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                item->SetExactKey(id.AsBinaryString());
                item->SetTabletId(msg.TabletId);
                item->SetMustRestoreFirst(msg.MustRestoreFirst);

                Agent.Issue(std::move(resolve), this, std::make_shared<TExtraResolveContext>(index));
                ++ResolvesInFlight;
            }

            void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
                    HandleResolveResult(id, std::move(context), (*p)->Record);
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    Agent.HandleGetResult(context, **p);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else {
                    Y_FAIL();
                }
            }

            void HandleResolveResult(ui64 id, TRequestContext::TPtr context, NKikimrBlobDepot::TEvResolveResult& msg) {
                auto& query = GetQuery();

                --ResolvesInFlight;

                if (msg.GetStatus() != NKikimrProto::OK && msg.GetStatus() != NKikimrProto::OVERRUN) {
                    return EndWithError(msg.GetStatus(), msg.GetErrorReason());
                }

                for (const auto& key : msg.GetResolvedKeys()) {
                    const TString& blobId = key.GetKey();
                    auto id = TLogoBlobID::FromBinary(blobId);

                    const size_t index = context
                        ? context->Obtain<TExtraResolveContext>().Index
                        : Response->Responses.size();
                    if (!context) {
                        Response->Responses.emplace_back(id, TString());
                    }

                    if (!query.IsIndexOnly) {
                        TReadArg arg{
                            key.GetValueChain(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead,
                            query.MustRestoreFirst,
                            this,
                            0,
                            0,
                            index,
                            {},
                            {}};
                        TString error;
                        if (!Agent.IssueRead(arg, error)) {
                            return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "failed to read discovered blob: "
                                << error);
                        }
                        ++ReadsInFlight;
                    } else if (query.MustRestoreFirst) {
                        Y_FAIL("not implemented yet");
                    }
                }

                if (msg.GetStatus() == NKikimrProto::OVERRUN) {
                    Agent.RegisterRequest(id, this, std::move(context), {}, true);
                } else if (msg.GetStatus() == NKikimrProto::OK) {
                    if (!ReadsInFlight && !ResolvesInFlight) {
                        EndWithSuccess(std::move(Response));
                    }
                } else {
                    Y_UNREACHABLE();
                }
            }

            void OnRead(ui64 tag, NKikimrProto::EReplyStatus status, TString dataOrErrorReason) override {
                auto& item = Response->Responses[tag];
                --ReadsInFlight;

                switch (status) {
                    case NKikimrProto::OK:
                        item.Buffer = std::move(dataOrErrorReason);
                        break;

                    case NKikimrProto::NODATA:
                        IssueResolve(item.Id, tag);
                        break;

                    default:
                        return EndWithError(status, TStringBuilder() << "failed to retrieve BlobId# "
                            << item.Id << " Error# " << dataOrErrorReason);
                }

                if (!ReadsInFlight && !ResolvesInFlight) {
                    EndWithSuccess(std::move(Response));
                }
            }

            TEvBlobStorage::TEvRange& GetQuery() const {
                return *Event->Get<TEvBlobStorage::TEvRange>();
            }
        };

        return new TRangeQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
