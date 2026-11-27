#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvAssimilate>(
            std::unique_ptr<IEventHandle> ev, TMonotonic received) {
        class TAssimilateQuery : public TBlobStorageQuery<TEvBlobStorage::TEvAssimilate> {
        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                if (!Agent.ManagedBridgeChild || Request.IncludePartPlacement || !Request.IgnoreDecommitState) {
                    return EndWithError(NKikimrProto::ERROR, "no physical group to assimilate");
                }
                NKikimrBlobDepot::TEvBridgeScan scan;
                scan.SetReverse(Request.Reverse);
                if (Request.SkipBlocksUpTo) {
                    scan.SetSkipBlocksUpTo(*Request.SkipBlocksUpTo);
                }
                if (Request.SkipBarriersUpTo) {
                    const auto& [tabletId, channel] = *Request.SkipBarriersUpTo;
                    scan.SetSkipBarrierTabletId(tabletId);
                    scan.SetSkipBarrierChannel(channel);
                }
                if (Request.SkipBlobsUpTo) {
                    LogoBlobIDFromLogoBlobID(*Request.SkipBlobsUpTo, scan.MutableSkipBlobsUpTo());
                }
                Agent.Issue(std::move(scan), this, nullptr);
            }

            void ProcessResponse(ui64, TRequestContext::TPtr, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    return EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                }
                const auto& record = std::get<TEvBlobDepot::TEvBridgeScanResult*>(response)->Record;
                if (record.GetStatus() != NKikimrProto::OK) {
                    return EndWithError(record.GetStatus(), "BlobDepot inventory unavailable");
                }
                using TResult = TEvBlobStorage::TEvAssimilateResult;
                auto result = std::make_unique<TResult>(NKikimrProto::OK);
                result->ReplyBytes = record.ByteSizeLong();
                for (const auto& row : record.GetBlocks()) {
                    result->Blocks.push_back({row.GetTabletId(), row.GetGeneration()});
                }
                for (const auto& row : record.GetBarriers()) {
                    auto value = [](TGenStep genCtr, TGenStep barrier) -> TResult::TBarrier::TValue {
                        return {genCtr.Generation(), genCtr.Step(), barrier.Generation(), barrier.Step()};
                    };
                    result->Barriers.push_back({row.GetTabletId(), ui8(row.GetChannel()),
                        value(TGenStep(row.GetSoftGenCtr()), TGenStep(row.GetSoft())),
                        value(TGenStep(row.GetHardGenCtr()), TGenStep(row.GetHard()))});
                }
                for (const auto& row : record.GetBlobs()) {
                    result->Blobs.push_back({LogoBlobIDFromLogoBlobID(row.GetId()),
                        row.GetKeepState() == NKikimrBlobDepot::Keep,
                        row.GetKeepState() == NKikimrBlobDepot::DoNotKeep, row.GetHasData()});
                }
                EndWithSuccess(std::move(result));
            }
        };
        return new TAssimilateQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
