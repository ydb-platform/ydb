#include "agent_impl.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvPut>(std::unique_ptr<IEventHandle> ev) {
        class TPutQuery : public TQuery {
            ui32 BlockChecksRemain = 3;
            ui32 PutsInFlight = 0;
            const bool SuppressFooter = true;
            NKikimrBlobDepot::TEvCommitBlobSeq CommitBlobSeq;
            TBlobSeqId BlobSeqId;

        public:
            using TQuery::TQuery;

            ~TPutQuery() {
                if (BlobSeqId != TBlobSeqId()) {
                    Agent.ChannelToKind[BlobSeqId.Channel]->WritesInFlight.erase(BlobSeqId);
                }
            }

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();
                if (msg.Buffer.size() > MaxBlobSize) {
                    return EndWithError(NKikimrProto::ERROR, "blob is way too big");
                }

                // zero step -- for decommission blobs just issue put immediately
                if (msg.Decommission) {
                    return IssuePuts();
                }

                // first step -- check blocks
                const auto status = Agent.BlocksManager.CheckBlockForTablet(msg.Id.TabletID(), msg.Id.Generation(), this, nullptr);
                if (status == NKikimrProto::OK) {
                    IssuePuts();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "block race detected");
                } else if (!--BlockChecksRemain) {
                    EndWithError(NKikimrProto::ERROR, "failed to acquire blocks");
                }
            }

            void IssuePuts() {
                auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();

                const auto it = Agent.ChannelKinds.find(NKikimrBlobDepot::TChannelKind::Data);
                if (it == Agent.ChannelKinds.end()) {
                    return EndWithError(NKikimrProto::ERROR, "no Data channels");
                }
                auto& kind = it->second;

                std::optional<TBlobSeqId> blobSeqId = kind.Allocate(Agent);
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA21, "allocated BlobSeqId", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, GetQueryId()), (BlobSeqId, blobSeqId));
                if (!blobSeqId) {
                    return kind.EnqueueQueryWaitingForId(this);
                }
                BlobSeqId = *blobSeqId;
                kind.WritesInFlight.insert(BlobSeqId);

                Y_VERIFY(CommitBlobSeq.ItemsSize() == 0);
                auto *commitItem = CommitBlobSeq.AddItems();
                commitItem->SetKey(msg.Id.AsBinaryString());
                auto *locator = commitItem->MutableBlobLocator();
                BlobSeqId.ToProto(locator->MutableBlobSeqId());
                //locator->SetChecksum(Crc32c(msg.Buffer.data(), msg.Buffer.size()));
                locator->SetTotalDataLen(msg.Buffer.size());
                if (!SuppressFooter) {
                    locator->SetFooterLen(sizeof(TVirtualGroupBlobFooter));
                }

                TVirtualGroupBlobFooter footer;
                memset(&footer, 0, sizeof(footer));
                footer.StoredBlobId = msg.Id;
                TStringBuf footerData(reinterpret_cast<const char*>(&footer), sizeof(footer));

                auto put = [&](EBlobType type, TRope&& buffer) {
                    const auto& [id, groupId] = kind.MakeBlobId(Agent, BlobSeqId, type, 0, buffer.size());
                    Y_VERIFY(!locator->HasGroupId() || locator->GetGroupId() == groupId);
                    locator->SetGroupId(groupId);
                    auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, std::move(buffer), msg.Deadline, msg.HandleClass, msg.Tactic);
                    ev->ExtraBlockChecks = msg.ExtraBlockChecks;
                    if (!msg.Decommission) { // do not check original blob against blocks when writing decommission copy
                        ev->ExtraBlockChecks.emplace_back(msg.Id.TabletID(), msg.Id.Generation());
                    }
                    Agent.SendToProxy(groupId, std::move(ev), this, nullptr);
                    ++PutsInFlight;
                };

                if (SuppressFooter) {
                    // write the blob as is, we don't need footer for this kind
                    put(EBlobType::VG_DATA_BLOB, TRope(msg.Buffer));
                } else if (msg.Buffer.size() + sizeof(TVirtualGroupBlobFooter) <= MaxBlobSize) {
                    // write single blob with footer
                    TRope buffer = msg.Buffer;
                    buffer.Insert(buffer.End(), TRope(TString(footerData))); //FIXME(innokentii)
                    buffer.Compact();
                    put(EBlobType::VG_COMPOSITE_BLOB, std::move(buffer));
                } else {
                    // write data blob and blob with footer
                    put(EBlobType::VG_DATA_BLOB, TRope(msg.Buffer));
                    put(EBlobType::VG_FOOTER_BLOB, TRope(TString(footerData)));
               }
            }

            void OnUpdateBlock(bool success) override {
                if (success) {
                    Initiate(); // just restart request
                } else {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                }
            }

            void OnIdAllocated() override {
                IssuePuts();
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (auto *p = std::get_if<TEvBlobStorage::TEvPutResult*>(&response)) {
                    HandlePutResult(std::move(context), **p);
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvCommitBlobSeqResult*>(&response)) {
                    HandleCommitBlobSeqResult(std::move(context), (*p)->Record);
                } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else {
                    Y_FAIL("unexpected response");
                }
            }

            void HandlePutResult(TRequestContext::TPtr /*context*/, TEvBlobStorage::TEvPutResult& msg) {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA22, "TEvPutResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, GetQueryId()), (Msg, msg));

                --PutsInFlight;
                if (msg.Status != NKikimrProto::OK) {
                    EndWithError(msg.Status, std::move(msg.ErrorReason));
                } else if (PutsInFlight) {
                    // wait for all puts to complete
                } else if (BlobSeqId.Generation != Agent.BlobDepotGeneration) {
                    // FIXME: although this is error now, we can handle this in the future, when BlobDepot picks records
                    // on restarts; it may have scanned written record and already updated it in its local database;
                    // however, if it did not, we can't try to commit this records as it may be already scheduled for
                    // garbage collection by the tablet
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet was restarting during write");
                } else {
                    // find and remove the write in flight record to ensure it won't be reported upon TEvPushNotify
                    // reception AND to check that it wasn't already trimmed by answering TEvPushNotifyResult
                    const auto it = Agent.ChannelKinds.find(NKikimrBlobDepot::TChannelKind::Data);
                    if (it == Agent.ChannelKinds.end()) {
                        return EndWithError(NKikimrProto::ERROR, "no Data channels");
                    }
                    auto& kind = it->second;
                    const size_t numErased = kind.WritesInFlight.erase(BlobSeqId);
                    Y_VERIFY(numErased);

                    Agent.Issue(std::move(CommitBlobSeq), this, nullptr);
                }
            }

            void HandleCommitBlobSeqResult(TRequestContext::TPtr /*context*/, NKikimrBlobDepot::TEvCommitBlobSeqResult& msg) {
                // FIXME: make this part optional

                Y_VERIFY(msg.ItemsSize() == 1);
                auto& item = msg.GetItems(0);
                if (item.GetStatus() != NKikimrProto::OK) {
                    EndWithError(item.GetStatus(), item.GetErrorReason());
                } else {
                    auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvPutResult>(NKikimrProto::OK, msg.Id,
                        Agent.GetStorageStatusFlags(), Agent.VirtualGroupId, Agent.GetApproximateFreeSpaceShare()));
                }
            }
        };

        return new TPutQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
