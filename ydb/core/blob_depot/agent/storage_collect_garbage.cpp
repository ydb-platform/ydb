#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvCollectGarbage>(std::unique_ptr<IEventHandle> ev,
            TMonotonic received) {
        class TCollectGarbageQuery : public TBlobStorageQuery<TEvBlobStorage::TEvCollectGarbage> {
            ui32 KeepIndex = 0;
            ui32 NumKeep;
            ui32 DoNotKeepIndex = 0;
            ui32 NumDoNotKeep;
            bool IsLast;
            bool QueryInFlight = false;

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                NumKeep = Request.Keep ? Request.Keep->size() : 0;
                NumDoNotKeep = Request.DoNotKeep ? Request.DoNotKeep->size() : 0;

                if (Request.IgnoreBlock || CheckBlockForTablet(Request.TabletId,
                        Request.RecordGeneration) == NKikimrProto::OK) {
                    IssueCollectGarbage();
                }
            }

            void IssueCollectGarbage() {
                NKikimrBlobDepot::TEvCollectGarbage record;

                ui32 numItemsIssued = 0;

                for (; KeepIndex < NumKeep && numItemsIssued < MaxCollectGarbageFlagsPerMessage; ++KeepIndex) {
                    LogoBlobIDFromLogoBlobID((*Request.Keep)[KeepIndex], record.AddKeep());
                    BDEV_QUERY(BDEV05, "TEvCollectGarbage_keep", (U.BlobId, (*Request.Keep)[KeepIndex]));
                    ++numItemsIssued;
                }
                for (; DoNotKeepIndex < NumDoNotKeep && numItemsIssued < MaxCollectGarbageFlagsPerMessage; ++DoNotKeepIndex) {
                    LogoBlobIDFromLogoBlobID((*Request.DoNotKeep)[DoNotKeepIndex], record.AddDoNotKeep());
                    BDEV_QUERY(BDEV06, "TEvCollectGarbage_doNotKeep", (U.BlobId, (*Request.DoNotKeep)[DoNotKeepIndex]));
                    ++numItemsIssued;
                }

                IsLast = KeepIndex == NumKeep && DoNotKeepIndex == NumDoNotKeep;

                record.SetTabletId(Request.TabletId);
                record.SetGeneration(Request.RecordGeneration);

                if (Request.Collect && IsLast) {
                    record.SetPerGenerationCounter(Request.PerGenerationCounter);
                    record.SetChannel(Request.Channel);
                    record.SetHard(Request.Hard);
                    record.SetCollectGeneration(Request.CollectGeneration);
                    record.SetCollectStep(Request.CollectStep);
                    if (Request.IgnoreBlock) {
                        record.SetIgnoreBlock(true);
                    }

                    BDEV_QUERY(BDEV04, "TEvCollectGarbage_barrier", (U.TabletId, Request.TabletId),
                        (U.Generation, Request.RecordGeneration), (U.PerGenerationCounter, Request.PerGenerationCounter),
                        (U.Channel, Request.Channel), (U.Hard, Request.Hard),
                        (U.CollectGeneration, Request.CollectGeneration), (U.CollectStep, Request.CollectStep));
                }

                Agent.Issue(std::move(record), this, nullptr);

                Y_ABORT_UNLESS(!QueryInFlight);
                QueryInFlight = true;
            }

            void OnUpdateBlock() override {
                Initiate();
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvCollectGarbageResult*>(&response)) {
                    HandleCollectGarbageResult(std::move(context), (*p)->Record);
                } else {
                    Y_ABORT();
                }
            }

            void HandleCollectGarbageResult(TRequestContext::TPtr /*context*/, NKikimrBlobDepot::TEvCollectGarbageResult& msg) {
                Y_ABORT_UNLESS(QueryInFlight);
                QueryInFlight = false;

                if (!msg.HasStatus()) {
                    EndWithError(NKikimrProto::ERROR, "incorrect TEvCollectGarbageResult protobuf");
                } else if (const auto status = msg.GetStatus(); status != NKikimrProto::OK) {
                    EndWithError(status, msg.GetErrorReason());
                } else if (IsLast) {
                    EndWithSuccess();
                } else {
                    IssueCollectGarbage();
                }
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason) {
                BDEV_QUERY(BDEV07, "TEvCollectGarbage_end", (Status, status), (ErrorReason, errorReason));
                TBlobStorageQuery::EndWithError(status, errorReason);
            }

            void EndWithSuccess() {
                BDEV_QUERY(BDEV08, "TEvCollectGarbage_end", (Status, NKikimrProto::OK));
                TBlobStorageQuery::EndWithSuccess(std::make_unique<TEvBlobStorage::TEvCollectGarbageResult>(
                    NKikimrProto::OK, Request.TabletId, Request.RecordGeneration, Request.PerGenerationCounter,
                    Request.Channel));
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };

        return new TCollectGarbageQuery(*this, std::move(ev), received);
    }

} // NKikimr::NBlobDepot
