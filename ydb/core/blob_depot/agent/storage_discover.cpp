#include "agent_impl.h"
#include "blob_mapping_cache.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvDiscover>(std::unique_ptr<IEventHandle> ev,
            TMonotonic received) {
        class TDiscoverQuery : public TBlobStorageQuery<TEvBlobStorage::TEvDiscover> {
            bool DoneWithBlockedGeneration = false;
            bool DoneWithData = false;

            TLogoBlobID Id;
            TString Buffer;
            ui32 BlockedGeneration = 0;

            NKikimrBlobDepot::TEvResolve Resolve;

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvDiscover_begin",
                    {"marker", "BDEV16"},
                    {"VG", Agent.VirtualGroupId},
                    {"BDT", Agent.TabletId},
                    {"G", Agent.BlobDepotGeneration},
                    {"Q", QueryId},
                    {"U.TabletId", Request.TabletId},
                    {"U.ReadBody", Request.ReadBody},
                    {"U.MinGeneration", Request.MinGeneration});

                GenerateInitialResolve();
                IssueResolve();
                CheckBlockedGeneration();
            }

            void CheckBlockedGeneration() {
                if (!DoneWithBlockedGeneration) {
                    DoneWithBlockedGeneration = !Request.DiscoverBlockedGeneration ||
                        CheckBlockForTablet(Request.TabletId, std::nullopt, &BlockedGeneration) == NKikimrProto::OK;
                }
                CheckIfDone();
            }

            void GenerateInitialResolve() {
                const ui8 channel = 0;
                const TLogoBlobID from(Request.TabletId, Request.MinGeneration, 0, channel, 0, 0);
                const TLogoBlobID to(Request.TabletId, Max<ui32>(), Max<ui32>(), channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);

                auto *item = Resolve.AddItems();
                auto *range = item->MutableKeyRange();
                range->SetBeginningKey(from.AsBinaryString());
                range->SetIncludeBeginning(true);
                range->SetEndingKey(to.AsBinaryString());
                range->SetIncludeEnding(true);
                range->SetMaxKeys(1);
                range->SetReverse(true);
                item->SetTabletId(Request.TabletId);
                item->SetMustRestoreFirst(true);
            }

            void IssueResolve() {
                YDB_LOG_DEBUG("IssueResolve",
                    {"marker", "BDA49"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()});
                Agent.Issue(Resolve, this, nullptr);
            }

            void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    return EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    YDB_LOG_DEBUG("TEvGetResult",
                        {"marker", "BDA50"},
                        {"agentId", Agent.LogId},
                        {"queryId", GetQueryId()},
                        {"response", *p});
                    TQuery::HandleGetResult(context, **p);
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
                    YDB_LOG_DEBUG("TEvResolveResult",
                        {"marker", "BDA51"},
                        {"agentId", Agent.LogId},
                        {"queryId", GetQueryId()},
                        {"response", (*p)->Record});
                    if (context) {
                        TQuery::HandleResolveResult(std::move(context), **p);
                    } else {
                        HandleResolveResult(id, std::move(context), **p);
                    }
                } else {
                    Y_ABORT();
                }
            }

            void OnUpdateBlock() override {
                YDB_LOG_DEBUG("OnUpdateBlock",
                    {"marker", "BDA18"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()});
                CheckBlockedGeneration();
            }

            void HandleResolveResult(ui64 id, TRequestContext::TPtr context, TEvBlobDepot::TEvResolveResult& msg) {
                YDB_LOG_DEBUG("HandleResolveResult",
                    {"marker", "BDA19"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()},
                    {"msg", msg.Record});

                Agent.BlobMappingCache.HandleResolveResult(id, msg.Record, nullptr);

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
                        if (Request.ReadBody) {
                            if (!item.ValueChainSize()) {
                                // FIXME(alexvru): hypothetically this can be considered normal and we may continue scan
                                return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "empty ValueChain");
                            }
                            TReadArg arg{
                                item,
                                NKikimrBlobStorage::Discover,
                                true,
                                0,
                                0,
                                0,
                                {},
                                item.GetKey(),
                            };
                            TString error;
                            if (!IssueRead(std::move(arg), error)) {
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
                    Y_ABORT(); // do not expect to return single key in few messages
                }

                if (status == NKikimrProto::OVERRUN) { // there will be extra message with data
                    Agent.RegisterRequest(id, this, std::move(context), {}, true);
                }
            }

            void OnRead(ui64 /*tag*/, TReadOutcome&& outcome) override {
                YDB_LOG_DEBUG("OnRead",
                    {"marker", "BDA20"},
                    {"agentId", Agent.LogId},
                    {"queryId", GetQueryId()},
                    {"outcome", outcome});

                std::visit(TOverloaded{
                    [&](TReadOutcome::TOk& ok) {
                        Buffer = ok.Data.ConvertToString();
                        DoneWithData = true;
                        CheckIfDone();
                    },
                    [&](TReadOutcome::TNodata& /*nodata*/) {
                        // we are reading blob from the original group and it may be partially written -- it is totally
                        // okay to have some; we need to advance to the next readable blob
                        auto *range = Resolve.MutableItems(0)->MutableKeyRange();
                        range->SetEndingKey(Id.AsBinaryString());
                        range->ClearIncludeEnding();
                        IssueResolve();
                    },
                    [&](TReadOutcome::TError& error) {
                        EndWithError(error.Status, error.ErrorReason);
                    }
                }, outcome.Value);
            }

            void CheckIfDone() {
                if (DoneWithBlockedGeneration && DoneWithData) {
                    Y_VERIFY_S(!Id || !Request.ReadBody || Buffer.size() == Id.BlobSize(), "Id# " << Id << " Buffer.size# " << Buffer.size());
                    YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvDiscover_end",
                        {"marker", "BDEV17"},
                        {"VG", Agent.VirtualGroupId},
                        {"BDT", Agent.TabletId},
                        {"G", Agent.BlobDepotGeneration},
                        {"Q", QueryId},
                        {"status", NKikimrProto::OK},
                        {"errorReason", ""},
                        {"id", Id},
                        {"Buffer.size", Buffer.size()},
                        {"blockedGeneration", BlockedGeneration});
                    EndWithSuccess(Id
                        ? std::make_unique<TEvBlobStorage::TEvDiscoverResult>(Id, Request.MinGeneration, Buffer, BlockedGeneration)
                        : std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::NODATA, Request.MinGeneration, BlockedGeneration));
                }
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "TEvDiscover_end",
                    {"marker", "BDEV18"},
                    {"VG", Agent.VirtualGroupId},
                    {"BDT", Agent.TabletId},
                    {"G", Agent.BlobDepotGeneration},
                    {"Q", QueryId},
                    {"status", status},
                    {"errorReason", errorReason});
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };

        return new TDiscoverQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
