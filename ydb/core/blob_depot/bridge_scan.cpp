#include "blob_depot_tablet.h"
#include "blocks.h"
#include "data.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvBridgeScan::TPtr ev) {
        class TTxBridgeScan : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TEvBlobDepot::TEvBridgeScan::TPtr Request;
            std::unique_ptr<IEventHandle> Response;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_BRIDGE_SCAN; }

            TTxBridgeScan(TBlobDepot *self, TEvBlobDepot::TEvBridgeScan::TPtr request)
                : TTransactionBase(self)
                , Request(std::move(request))
            {}

            bool Execute(TTransactionContext&, const TActorContext&) override {
                const auto& request = Request->Get()->Record;
                auto [response, result] = TEvBlobDepot::MakeResponseFor(*Request);
                if (!Self->Config.HasBridgeGroupGeneration()) {
                    result->SetStatus(NKikimrProto::ERROR);
                } else if (!Self->CheckBridgeGeneration(request.GetBridgeGroupGeneration())) {
                    result->SetStatus(NKikimrProto::RACE);
                    result->SetBridgeGroupGeneration(Self->Config.GetBridgeGroupGeneration());
                } else if (request.GetCheckGenerationOnly()) {
                    result->SetStatus(NKikimrProto::OK);
                } else if (!Self->Data->IsLoaded()) {
                    // Reporting the end of an incomplete in-memory index would finish synchronization prematurely.
                    result->SetStatus(NKikimrProto::NOTREADY);
                } else {
                    result->SetStatus(NKikimrProto::OK);
                    size_t left = 1024;
                    auto scan = [&]<class TMap>(const TMap& rows, std::optional<typename TMap::key_type> cursor, auto append) {
                        if (request.GetReverse()) {
                            auto it = cursor ? rows.lower_bound(*cursor) : rows.end();
                            while (left && it != rows.begin()) {
                                append(*--it);
                                --left;
                            }
                        } else {
                            auto it = cursor ? rows.upper_bound(*cursor) : rows.begin();
                            while (left && it != rows.end()) {
                                append(*it++);
                                --left;
                            }
                        }
                    };
                    std::optional<ui64> block;
                    if (request.HasSkipBlocksUpTo()) {
                        block = request.GetSkipBlocksUpTo();
                    }
                    scan(Self->BlocksManager->BridgeBlocks, block, [&](const auto& row) {
                        auto *out = result->AddBlocks();
                        out->SetTabletId(row.first);
                        out->SetGeneration(row.second);
                    });
                    std::optional<std::tuple<ui64, ui8>> barrier;
                    if (request.HasSkipBarrierTabletId()) {
                        barrier.emplace(request.GetSkipBarrierTabletId(), request.GetSkipBarrierChannel());
                    }
                    scan(Self->BarrierServer->Barriers, barrier, [&](const auto& row) {
                        auto *out = result->AddBarriers();
                        out->SetTabletId(std::get<0>(row.first));
                        out->SetChannel(std::get<1>(row.first));
                        out->SetSoftGenCtr(ui64(row.second.SoftGenCtr));
                        out->SetSoft(ui64(row.second.Soft));
                        out->SetHardGenCtr(ui64(row.second.HardGenCtr));
                        out->SetHard(ui64(row.second.Hard));
                    });
                    std::optional<TData::TKey> blob;
                    if (request.HasSkipBlobsUpTo()) {
                        blob.emplace(LogoBlobIDFromLogoBlobID(request.GetSkipBlobsUpTo()));
                    }
                    scan(Self->Data->Data, blob, [&](const auto& row) {
                        auto *out = result->AddBlobs();
                        LogoBlobIDFromLogoBlobID(row.first.GetBlobId(), out->MutableId());
                        out->SetKeepState(row.second.KeepState);
                        out->SetHasData(!row.second.ValueChain.empty() && !row.second.IsWrittenUncertainly());
                    });
                }
                Response = std::move(response);
                return true;
            }

            void Complete(const TActorContext&) override {
                TActivationContext::Send(Response.release());
            }
        };

        Execute(std::make_unique<TTxBridgeScan>(this, std::move(ev)));
    }

} // NKikimr::NBlobDepot
