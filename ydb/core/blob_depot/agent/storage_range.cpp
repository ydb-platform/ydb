#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvRange>(std::unique_ptr<IEventHandle> ev) {
        class TRangeQuery : public TBlobStorageQuery<TEvBlobStorage::TEvRange> {
            struct TRead {
                TLogoBlobID Id;
            };

            std::unique_ptr<TEvBlobStorage::TEvRangeResult> Response;
            ui32 ReadsInFlight = 0;
            std::map<TLogoBlobID, TString> FoundBlobs;
            std::vector<TRead> Reads;
            bool Reverse = false;
            bool Finished = false;

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                BDEV_QUERY(BDEV21, "TEvRange_new", (U.TabletId, Request.TabletId), (U.From, Request.From), (U.To, Request.To),
                    (U.MustRestoreFirst, Request.MustRestoreFirst), (U.IndexOnly, Request.IsIndexOnly));

                Response = std::make_unique<TEvBlobStorage::TEvRangeResult>(NKikimrProto::OK, Request.From, Request.To,
                    TGroupId::FromValue(Agent.VirtualGroupId));

                // issue resolve query
                TString from = Request.From.AsBinaryString();
                TString to = Request.To.AsBinaryString();
                Reverse = Request.To < Request.From;
                if (Reverse) {
                    std::swap(from, to);
                }

                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                auto *range = item->MutableKeyRange();
                range->SetBeginningKey(from);
                range->SetIncludeBeginning(true);
                range->SetEndingKey(to);
                range->SetIncludeEnding(true);
                range->SetReverse(Reverse);
                item->SetTabletId(Request.TabletId);
                item->SetMustRestoreFirst(Request.MustRestoreFirst);

                Agent.Issue(std::move(resolve), this, nullptr);
            }

            void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
                    if (context) {
                        TQuery::HandleResolveResult(std::move(context), **p);
                    } else {
                        HandleResolveResult(id, std::move(context), (*p)->Record);
                    }
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    TQuery::HandleGetResult(context, **p);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else {
                    Y_ABORT();
                }
            }

            void HandleResolveResult(ui64 id, TRequestContext::TPtr context, NKikimrBlobDepot::TEvResolveResult& msg) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA47, "HandleResolveResult", (AgentId, Agent.LogId),
                    (QueryId, GetQueryId()), (Msg, msg));

                if (msg.GetStatus() != NKikimrProto::OK && msg.GetStatus() != NKikimrProto::OVERRUN) {
                    return EndWithError(msg.GetStatus(), msg.GetErrorReason());
                }

                for (const auto& key : msg.GetResolvedKeys()) {
                    const TString& blobId = key.GetKey();
                    auto id = TLogoBlobID::FromBinary(blobId);

                    if (key.HasErrorReason()) {
                        return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "failed to resolve blob# " << id
                            << ": " << key.GetErrorReason());
                    } else if (Request.IsIndexOnly) {
                        FoundBlobs.try_emplace(id);
                    } else if (key.ValueChainSize()) {
                        const ui64 tag = key.HasCookie() ? key.GetCookie() : Reads.size();
                        if (tag == Reads.size()) {
                            Reads.push_back(TRead{id});
                        } else {
                            Y_ABORT_UNLESS(Reads[tag].Id == id);
                        }
                        TReadArg arg{
                            key,
                            NKikimrBlobStorage::EGetHandleClass::FastRead,
                            Request.MustRestoreFirst,
                            0,
                            0,
                            tag,
                            {},
                            key.GetKey(),
                        };
                        ++ReadsInFlight;
                        TString error;
                        if (!IssueRead(std::move(arg), error)) {
                            return EndWithError(NKikimrProto::ERROR, TStringBuilder() << "failed to read discovered blob: "
                                << error);
                        }
                    }
                }

                if (msg.GetStatus() == NKikimrProto::OVERRUN) {
                    Agent.RegisterRequest(id, this, std::move(context), {}, true);
                } else {
                    CheckAndFinish();
                }
            }

            void OnRead(ui64 tag, TReadOutcome&& outcome) override {
                --ReadsInFlight;

                Y_ABORT_UNLESS(tag < Reads.size());
                TRead& read = Reads[tag];

                const bool success = std::visit(TOverloaded{
                    [&](TReadOutcome::TOk& ok) {
                        Y_ABORT_UNLESS(ok.Data.size() == read.Id.BlobSize());
                        const bool inserted = FoundBlobs.try_emplace(read.Id, ok.Data.ConvertToString()).second;
                        Y_VERIFY_S(inserted, "AgentId# " << Agent.LogId << " QueryId# " << GetQueryId()
                            << " duplicate BlobId# " << read.Id << " received");
                        return true;
                    },
                    [&](TReadOutcome::TNodata& /*nodata*/) {
                        // this blob has just vanished since we found it in index -- may be it was partially written and
                        // now gone; it's okay to have this situation, not a data loss
                        return true;
                    },
                    [&](TReadOutcome::TError& error) {
                        EndWithError(error.Status, TStringBuilder() << "failed to retrieve BlobId# "
                            << read.Id << " Error# " << error.ErrorReason);
                        return false;
                    }
                }, outcome.Value);

                if (success) {
                    CheckAndFinish();
                }
            }

            void CheckAndFinish() {
                if (!ReadsInFlight && !Finished) {
                    for (auto& [id, buffer] : FoundBlobs) {
                        Y_VERIFY_S(buffer.size() == Request.IsIndexOnly ? 0 : id.BlobSize(), "Id# " << id << " Buffer.size# " << buffer.size());
                        Response->Responses.emplace_back(id, std::move(buffer));
                    }
                    if (Reverse) {
                        std::reverse(Response->Responses.begin(), Response->Responses.end());
                    }
                    EndWithSuccess();
                    Finished = true;
                }
            }

            void EndWithSuccess() {
                if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::BLOB_DEPOT_EVENTS)) {
                    for (const auto& r : Response->Responses) {
                        BDEV_QUERY(BDEV22, "TEvRange_item", (BlobId, r.Id), (Buffer.size, r.Buffer.size()));
                    }
                    BDEV_QUERY(BDEV14, "TEvRange_end", (Status, NKikimrProto::OK), (ErrorReason, ""));
                }
                TBlobStorageQuery::EndWithSuccess(std::move(Response));
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                BDEV_QUERY(BDEV15, "TEvRange_end", (Status, status), (ErrorReason, errorReason));
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };

        return new TRangeQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
