#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvPatch>(std::unique_ptr<IEventHandle> ev,
            TMonotonic received) {
        class TPatchQuery : public TBlobStorageQuery<TEvBlobStorage::TEvPatch> {
        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                if (!Agent.ManagedBridgeChild) {
                    return EndWithError(NKikimrProto::ERROR, "not implemented");
                }
                auto get = std::make_unique<TEvBlobStorage::TEvGet>(Request.OriginalId, 0, 0,
                    Request.Deadline, NKikimrBlobStorage::AsyncRead);
                if (Request.OriginalGroupId == Agent.VirtualGroupId) {
                    get->ForceGroupGeneration = Request.ForceGroupGeneration;
                }
                get->DataKind = Request.DataKind;
                Agent.SendToProxy(Request.OriginalGroupId, std::move(get), this, nullptr, Span.GetTraceId());
            }

            void ProcessResponse(ui64, TRequestContext::TPtr, TResponse response) override {
                if (auto *p = std::get_if<TEvBlobStorage::TEvGetResult*>(&response)) {
                    const auto& get = **p;
                    if (get.Status == NKikimrProto::RACE && Request.OriginalGroupId == Agent.VirtualGroupId) {
                        RacingGeneration = get.RacingGeneration;
                        return EndWithError(get.Status, get.ErrorReason);
                    }
                    if (get.Status != NKikimrProto::OK || get.ResponseSz != 1 ||
                            get.Responses[0].Status != NKikimrProto::OK ||
                            get.Responses[0].Buffer.size() != Request.OriginalId.BlobSize()) {
                        return EndWithError(NKikimrProto::ERROR, "cannot read the original blob");
                    }
                    if (Request.PatchedId.BlobSize() != Request.OriginalId.BlobSize()) {
                        return EndWithError(NKikimrProto::ERROR, "original and patched blob sizes differ");
                    }
                    TString data = get.Responses[0].Buffer.ConvertToString();
                    for (ui64 i = 0; i < Request.DiffCount; ++i) {
                        const auto& diff = Request.Diffs[i];
                        if (diff.Offset > data.size() || diff.Buffer.size() > data.size() - diff.Offset) {
                            return EndWithError(NKikimrProto::ERROR, "patch diff exceeds blob bounds");
                        }
                        memcpy(data.Detach() + diff.Offset, diff.Buffer.data(), diff.Buffer.size());
                    }
                    auto put = std::make_unique<TEvBlobStorage::TEvPut>(TEvBlobStorage::TEvPut::TParameters{
                        .BlobId = Request.PatchedId,
                        .Buffer = TRope(std::move(data)),
                        .Deadline = Request.Deadline,
                        .HandleClass = NKikimrBlobStorage::AsyncBlob,
                        .WriteSource = TWriteSource::DSProxyPatch,
                        .DataKind = Request.DataKind,
                    });
                    put->ForceGroupGeneration = Request.ForceGroupGeneration;
                    Agent.SendToProxy(Agent.VirtualGroupId, std::move(put), this, nullptr, Span.GetTraceId());
                } else if (auto *p = std::get_if<TEvBlobStorage::TEvPutResult*>(&response)) {
                    const auto& put = **p;
                    auto result = std::make_unique<TEvBlobStorage::TEvPatchResult>(put.Status, Request.PatchedId,
                        put.StatusFlags, TGroupId::FromValue(Agent.VirtualGroupId), put.ApproximateFreeSpaceShare);
                    result->RacingGeneration = put.RacingGeneration;
                    result->ErrorReason = put.ErrorReason;
                    EndWithSuccess(std::move(result));
                } else {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                }
            }
        };

        return new TPatchQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
