#include "agent_impl.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvCollectGarbage>(std::unique_ptr<IEventHandle> ev) {
        class TCollectGarbageQuery : public TBlobStorageQuery<TEvBlobStorage::TEvCollectGarbage> {
            ui32 BlockChecksRemain = 3;
            ui32 KeepIndex = 0;
            ui32 NumKeep;
            ui32 DoNotKeepIndex = 0;
            ui32 NumDoNotKeep;
            ui32 CounterShift = 0;
            bool IsLast;
            bool QueryInFlight = false;

        public:
            using TBlobStorageQuery::TBlobStorageQuery;

            void Initiate() override {
                NumKeep = Request.Keep ? Request.Keep->size() : 0;
                NumDoNotKeep = Request.DoNotKeep ? Request.DoNotKeep->size() : 0;

                const auto status = Agent.BlocksManager.CheckBlockForTablet(Request.TabletId, Request.RecordGeneration, this, nullptr);
                if (status == NKikimrProto::OK) {
                    IssueCollectGarbage();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "block race detected");
                } else if (!--BlockChecksRemain) {
                    EndWithError(NKikimrProto::ERROR, "failed to acquire blocks");
                }
            }

            void IssueCollectGarbage() {
                NKikimrBlobDepot::TEvCollectGarbage record;

                ui32 numItemsIssued = 0;

                for (; KeepIndex < NumKeep && numItemsIssued < MaxCollectGarbageFlagsPerMessage; ++KeepIndex) {
                    LogoBlobIDFromLogoBlobID((*Request.Keep)[KeepIndex], record.AddKeep());
                    ++numItemsIssued;
                }
                for (; DoNotKeepIndex < NumDoNotKeep && numItemsIssued < MaxCollectGarbageFlagsPerMessage; ++DoNotKeepIndex) {
                    LogoBlobIDFromLogoBlobID((*Request.DoNotKeep)[DoNotKeepIndex], record.AddDoNotKeep());
                    ++numItemsIssued;
                }

                IsLast = KeepIndex == NumKeep && DoNotKeepIndex == NumDoNotKeep;

                record.SetTabletId(Request.TabletId);
                record.SetGeneration(Request.RecordGeneration);
                record.SetPerGenerationCounter(Request.PerGenerationCounter + CounterShift);
                record.SetChannel(Request.Channel);

                if (Request.Collect && IsLast) {
                    record.SetHard(Request.Hard);
                    record.SetCollectGeneration(Request.CollectGeneration);
                    record.SetCollectStep(Request.CollectStep);
                }

                Agent.Issue(std::move(record), this, nullptr);

                Y_VERIFY(!QueryInFlight);
                QueryInFlight = true;

                ++CounterShift;
            }

            void OnUpdateBlock(bool success) override {
                if (success) {
                    Initiate();
                } else {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                }
            }

            void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
                if (std::holds_alternative<TTabletDisconnected>(response)) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                } else if (auto *p = std::get_if<TEvBlobDepot::TEvCollectGarbageResult*>(&response)) {
                    HandleCollectGarbageResult(std::move(context), (*p)->Record);
                } else {
                    Y_FAIL();
                }
            }

            void HandleCollectGarbageResult(TRequestContext::TPtr /*context*/, NKikimrBlobDepot::TEvCollectGarbageResult& msg) {
                Y_VERIFY(QueryInFlight);
                QueryInFlight = false;

                if (!msg.HasStatus()) {
                    EndWithError(NKikimrProto::ERROR, "incorrect TEvCollectGarbageResult protobuf");
                } else if (const auto status = msg.GetStatus(); status != NKikimrProto::OK) {
                    EndWithError(status, msg.GetErrorReason());
                } else if (IsLast) {
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvCollectGarbageResult>(NKikimrProto::OK,
                        Request.TabletId, Request.RecordGeneration, Request.PerGenerationCounter, Request.Channel));
                } else {
                    IssueCollectGarbage();
                }
            }

            ui64 GetTabletId() const override {
                return Request.TabletId;
            }
        };

        return new TCollectGarbageQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
