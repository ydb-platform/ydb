#include "agent_impl.h"
#include "blocks.h"
#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvDiscover>(std::unique_ptr<IEventHandle> ev) {
        class TDiscoverQuery : public TQuery {
            ui64 TabletId = 0;
            bool ReadBody;
            ui32 MinGeneration = 0;

            bool DoneWithBlockedGeneration = false;
            bool DoneWithData = false;

            TLogoBlobID Id;
            TString Buffer;
            ui32 BlockedGeneration = 0;

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvDiscover>();

                TabletId = msg.TabletId;
                ReadBody = msg.ReadBody;
                MinGeneration = msg.MinGeneration;

                IssueResolve();

                if (msg.DiscoverBlockedGeneration) {
                    const auto status = Agent.BlocksManager.CheckBlockForTablet(TabletId, Max<ui32>(), this, &BlockedGeneration);
                    if (status == NKikimrProto::OK) {
                        DoneWithBlockedGeneration = true;
                    } else if (status != NKikimrProto::UNKNOWN) {
                        EndWithError(status, "tablet was deleted");
                    }
                } else {
                    DoneWithBlockedGeneration = true;
                }
            }

            void IssueResolve() {
                const ui8 channel = 0;
                const TLogoBlobID from(TabletId, MinGeneration, 0, channel, 0, 0);
                const TLogoBlobID to(TabletId, Max<ui32>(), Max<ui32>(), channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);

                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                item->SetBeginningKey(from.AsBinaryString());
                item->SetIncludeBeginning(true);
                item->SetEndingKey(to.AsBinaryString());
                item->SetIncludeEnding(true);
                item->SetMaxKeys(1);
                item->SetReverse(true);
                item->SetTabletId(TabletId);
                item->SetMustRestoreFirst(true);

                Agent.Issue(std::move(resolve), this, nullptr);
            }

            void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    return EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    Agent.HandleGetResult(context, **p);
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
                    HandleResolveResult(id, std::move(context), **p);
                } else {
                    Y_FAIL();
                }
            }

            void OnUpdateBlock(bool success) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA18, "OnUpdateBlock", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, QueryId), (Success, success));

                if (!success) {
                    return EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                }

                const auto status = Agent.BlocksManager.CheckBlockForTablet(TabletId, Max<ui32>(), this, &BlockedGeneration);
                if (status == NKikimrProto::OK) {
                    DoneWithBlockedGeneration = true;
                    CheckIfDone();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "tablet was deleted");
                } else {
                    Y_FAIL();
                }
            }

            void HandleResolveResult(ui64 id, TRequestContext::TPtr context, TEvBlobDepot::TEvResolveResult& msg) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA19, "HandleResolveResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, QueryId), (Msg, msg.Record));

                Agent.BlobMappingCache.HandleResolveResult(msg.Record, nullptr);

                const NKikimrProto::EReplyStatus status = msg.Record.GetStatus();
                if (status != NKikimrProto::OK && status != NKikimrProto::OVERRUN) {
                    return EndWithError(status, msg.Record.GetErrorReason());
                }

                if (status == NKikimrProto::OK) {
                    for (const auto& item : msg.Record.GetResolvedKeys()) {
                        Id = TLogoBlobID::FromBinary(item.GetKey());
                        Y_VERIFY(item.ValueChainSize() == 1);
                        if (ReadBody) {
                            TString error;
                            if (!Agent.IssueRead(item.GetValueChain(), 0, 0, NKikimrBlobStorage::Discover, true, this, 0,
                                    &error)) {
                                return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "failed to read discovered blob: "
                                    << error);
                            }
                        }
                    }

                    if (!ReadBody || !Id) {
                        DoneWithData = true;
                        return CheckIfDone();
                    }
                } else {
                    Y_FAIL(); // do not expect to return single key in few messages
                }

                if (status == NKikimrProto::OVERRUN) { // there will be extra message with data
                    Agent.RegisterRequest(id, this, std::move(context), {}, true);
                }
            }

            void OnRead(ui64 /*tag*/, NKikimrProto::EReplyStatus status, TString dataOrErrorReason) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA20, "OnRead", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, QueryId), (Status, status));

                if (status == NKikimrProto::OK) {
                    Buffer = std::move(dataOrErrorReason);
                    DoneWithData = true;
                    CheckIfDone();
                } else if (status == NKikimrProto::NODATA) {
                    // this may indicate a data race between locator and key value, we have to restart our resolution query
                    IssueResolve();
                    // FIXME: infinite cycle?
                } else {
                    EndWithError(status, dataOrErrorReason);
                }
            }

            void CheckIfDone() {
                if (DoneWithBlockedGeneration && DoneWithData) {
                    EndWithSuccess(Id
                        ? std::make_unique<TEvBlobStorage::TEvDiscoverResult>(Id, MinGeneration, Buffer, BlockedGeneration)
                        : std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::NODATA, MinGeneration, BlockedGeneration));
                }
            }
        };

        return new TDiscoverQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
