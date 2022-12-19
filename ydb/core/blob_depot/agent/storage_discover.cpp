#include "agent_impl.h"
#include "blocks.h"
#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvDiscover>(std::unique_ptr<IEventHandle> ev) {
        class TDiscoverQuery : public TBlobStorageQuery<TEvBlobStorage::TEvDiscover> {
            ui32 GetBlockedGenerationRetriesRemain = 10;

            bool DoneWithBlockedGeneration = false;
            bool DoneWithData = false;

            TLogoBlobID Id;
            TString Buffer;
            ui32 BlockedGeneration = 0;

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                BDEV_QUERY(BDEV16, "TEvDiscover_begin", (U.TabletId, Request.TabletId), (U.ReadBody, Request.ReadBody),
                    (U.MinGeneration, Request.MinGeneration));

                IssueResolve();

                if (Request.DiscoverBlockedGeneration) {
                    const auto status = Agent.BlocksManager.CheckBlockForTablet(Request.TabletId, Max<ui32>(), this, &BlockedGeneration);
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
                const TLogoBlobID from(Request.TabletId, Request.MinGeneration, 0, channel, 0, 0);
                const TLogoBlobID to(Request.TabletId, Max<ui32>(), Max<ui32>(), channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);

                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                auto *range = item->MutableKeyRange();
                range->SetBeginningKey(from.AsBinaryString());
                range->SetIncludeBeginning(true);
                range->SetEndingKey(to.AsBinaryString());
                range->SetIncludeEnding(true);
                range->SetMaxKeys(1);
                range->SetReverse(true);
                item->SetTabletId(Request.TabletId);
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

            void OnUpdateBlock() override {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA18, "OnUpdateBlock", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, GetQueryId()));

                const auto status = Agent.BlocksManager.CheckBlockForTablet(Request.TabletId, Max<ui32>(), this, &BlockedGeneration);
                if (status == NKikimrProto::OK) {
                    DoneWithBlockedGeneration = true;
                    CheckIfDone();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "tablet was deleted");
                } else if (!--GetBlockedGenerationRetriesRemain) {
                    EndWithError(status, "too many retries to obtain blocked generation");
                }
            }

            void HandleResolveResult(ui64 id, TRequestContext::TPtr context, TEvBlobDepot::TEvResolveResult& msg) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA19, "HandleResolveResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, GetQueryId()), (Msg, msg.Record));

                Agent.BlobMappingCache.HandleResolveResult(msg.Record, nullptr);

                const NKikimrProto::EReplyStatus status = msg.Record.GetStatus();
                if (status != NKikimrProto::OK && status != NKikimrProto::OVERRUN) {
                    return EndWithError(status, msg.Record.GetErrorReason());
                }

                if (status == NKikimrProto::OK) {
                    for (const auto& item : msg.Record.GetResolvedKeys()) {
                        Id = TLogoBlobID::FromBinary(item.GetKey());
                        if (item.HasErrorReason()) {
                            return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "failed to resolve blob# " << Id
                                << ": " << item.GetErrorReason());
                        }
                        Y_VERIFY(item.ValueChainSize() == 1);
                        if (Request.ReadBody) {
                            TReadArg arg{
                                item.GetValueChain(),
                                NKikimrBlobStorage::Discover,
                                true,
                                this,
                                0,
                                0,
                                0,
                                {}};
                            TString error;
                            if (!Agent.IssueRead(arg, error)) {
                                return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "failed to read discovered blob: "
                                    << error);
                            }
                        }
                    }

                    if (!Request.ReadBody || !Id) {
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
                    (QueryId, GetQueryId()), (Status, status));

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
                    Y_VERIFY_S(!Id || !Request.ReadBody || Buffer.size() == Id.BlobSize(), "Id# " << Id << " Buffer.size# " << Buffer.size());
                    BDEV_QUERY(BDEV17, "TEvDiscover_end", (Status, NKikimrProto::OK), (ErrorReason, ""), (Id, Id),
                        (Buffer.size, Buffer.size()), (BlockedGeneration, BlockedGeneration));
                    EndWithSuccess(Id
                        ? std::make_unique<TEvBlobStorage::TEvDiscoverResult>(Id, Request.MinGeneration, Buffer, BlockedGeneration)
                        : std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::NODATA, Request.MinGeneration, BlockedGeneration));
                }
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                BDEV_QUERY(BDEV18, "TEvDiscover_end", (Status, status), (ErrorReason, errorReason));
                TBlobStorageQuery::EndWithError(status, errorReason);
            }
            
            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };

        return new TDiscoverQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
