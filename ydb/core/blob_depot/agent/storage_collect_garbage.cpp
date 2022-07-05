#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TQuery *TBlobDepotAgent::CreateQuery<TEvBlobStorage::EvCollectGarbage>(std::unique_ptr<IEventHandle> ev) {
        class TCollectGarbageQuery : public TQuery {
            ui32 BlockChecksRemain = 3;
            ui32 KeepIndex = 0;
            ui32 NumKeep;
            ui32 DoNotKeepIndex = 0;
            ui32 NumDoNotKeep;
            ui32 CounterShift = 0;
            bool IsLast;

        public:
            using TQuery::TQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvCollectGarbage>();

                NumKeep = msg.Keep ? msg.Keep->size() : 0;
                NumDoNotKeep = msg.DoNotKeep ? msg.DoNotKeep->size() : 0;

                const auto status = Agent.CheckBlockForTablet(msg.TabletId, msg.RecordGeneration, this);
                if (status == NKikimrProto::OK) {
                    IssueCollectGarbage();
                } else if (status != NKikimrProto::UNKNOWN) {
                    EndWithError(status, "block race detected");
                } else if (!--BlockChecksRemain) {
                    EndWithError(NKikimrProto::ERROR, "failed to acquire blocks");
                }
            }

            void IssueCollectGarbage() {
                auto& msg = *Event->Get<TEvBlobStorage::TEvCollectGarbage>();
                NKikimrBlobDepot::TEvCollectGarbage record;

                ui32 numItemsIssued = 0;

                for (; KeepIndex < NumKeep && numItemsIssued < MaxCollectGarbageFlagsPerMessage; ++KeepIndex) {
                    LogoBlobIDFromLogoBlobID((*msg.Keep)[KeepIndex], record.AddKeep());
                    ++numItemsIssued;
                }
                for (; DoNotKeepIndex < NumDoNotKeep && numItemsIssued < MaxCollectGarbageFlagsPerMessage; ++DoNotKeepIndex) {
                    LogoBlobIDFromLogoBlobID((*msg.DoNotKeep)[DoNotKeepIndex], record.AddDoNotKeep());
                    ++numItemsIssued;
                }

                IsLast = KeepIndex == NumKeep && DoNotKeepIndex == NumDoNotKeep;

                record.SetTabletId(msg.TabletId);
                record.SetGeneration(msg.RecordGeneration);
                record.SetPerGenerationCounter(msg.PerGenerationCounter + CounterShift);
                record.SetChannel(msg.Channel);

                if (msg.Collect && IsLast) {
                    record.SetHard(msg.Hard);
                    record.SetCollectGeneration(msg.CollectGeneration);
                    record.SetCollectStep(msg.CollectStep);
                }

                Agent.Issue(std::move(record), this, nullptr);

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
                if (!msg.HasStatus()) {
                    EndWithError(NKikimrProto::ERROR, "incorrect TEvCollectGarbageResult protobuf");
                } else if (const auto status = msg.GetStatus(); status != NKikimrProto::OK) {
                    EndWithError(status, msg.GetErrorReason());
                } else if (IsLast) {
                    auto& msg = *Event->Get<TEvBlobStorage::TEvCollectGarbage>();
                    EndWithSuccess(std::make_unique<TEvBlobStorage::TEvCollectGarbageResult>(NKikimrProto::OK,
                        msg.TabletId, msg.RecordGeneration, msg.PerGenerationCounter, msg.Channel));
                } else {
                    IssueCollectGarbage();
                }
            }
        };

        return new TCollectGarbageQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
