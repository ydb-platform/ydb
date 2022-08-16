#include "agent_impl.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvPut>(std::unique_ptr<IEventHandle> ev) {
        class TPutQuery : public TQuery {
            ui32 BlockChecksRemain = 3;
            ui32 PutsInFlight = 0;
            TBlobSeqId BlobSeqId;
            ui32 GroupId;
            ui64 TotalDataLen;

        public:
            using TQuery::TQuery;

            ~TPutQuery() {
                if (BlobSeqId != TBlobSeqId()) {
                    Agent.ChannelToKind[BlobSeqId.Channel]->WritesInFlight.erase(BlobSeqId);
                }
            }

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();

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

                auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();
                const ui32 size = msg.Id.BlobSize();
                TotalDataLen = size;

                TVirtualGroupBlobFooter footer;
                memset(&footer, 0, sizeof(footer));
                footer.StoredBlobId = msg.Id;

                TLogoBlobID id;
                TStringBuf footerData(reinterpret_cast<const char*>(&footer), sizeof(footer));

                auto sendPut = [&](TLogoBlobID id, const TString& buffer) {
                    auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, buffer, msg.Deadline, msg.HandleClass, msg.Tactic);
                    ev->ExtraBlockChecks = msg.ExtraBlockChecks;
                    if (!msg.Decommission) { // do not check original blob against blocks when writing decommission copy
                        ev->ExtraBlockChecks.emplace_back(msg.Id.TabletID(), msg.Id.Generation());
                    }
                    Agent.SendToProxy(GroupId, std::move(ev), this, nullptr);
                };

                if (size + sizeof(TVirtualGroupBlobFooter) <= MaxBlobSize) {
                    // write single blob with footer
                    TString buffer = msg.Buffer;
                    buffer.append(footerData);
                    std::tie(id, GroupId) = kind.MakeBlobId(Agent, BlobSeqId, EBlobType::VG_COMPOSITE_BLOB, 0, buffer.size());
                    sendPut(id, buffer);
                    ++PutsInFlight;
                } else {
                    // write data blob and blob with footer
                    std::tie(id, GroupId) = kind.MakeBlobId(Agent, BlobSeqId, EBlobType::VG_DATA_BLOB, 0, msg.Buffer.size());
                    sendPut(id, msg.Buffer);

                    std::tie(id, GroupId) = kind.MakeBlobId(Agent, BlobSeqId, EBlobType::VG_FOOTER_BLOB, 0, footerData.size());
                    sendPut(id, TString(footerData));

                    PutsInFlight += 2;
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

                    NKikimrBlobDepot::TEvCommitBlobSeq request;

                    auto& msg = *Event->Get<TEvBlobStorage::TEvPut>();

                    auto *item = request.AddItems();
                    item->SetKey(msg.Id.AsBinaryString());
                    auto *locator = item->MutableBlobLocator();
                    locator->SetGroupId(GroupId);
                    BlobSeqId.ToProto(locator->MutableBlobSeqId());
                    locator->SetChecksum(0);
                    locator->SetTotalDataLen(TotalDataLen);
                    locator->SetFooterLen(sizeof(TVirtualGroupBlobFooter));
                    Agent.Issue(std::move(request), this, nullptr);
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
