#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilatorFetchMachine {
        TActorIdentity Self;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TActorId BlobDepotId;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Data processing
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TPerDiskState {
            std::deque<TBlock> Blocks;
            std::optional<ui64> LastBlock;
            std::deque<TBarrier> Barriers;
            std::optional<std::tuple<ui64, ui8>> LastBarrier;
            std::deque<TBlob> Blobs;
            std::optional<TLogoBlobID> LastBlob;
            bool Finished = false;
            bool RequestInFlight = false;

            ui64 FirstBlock() const {
                Y_VERIFY_DEBUG(!Blocks.empty());
                return Blocks.front().TabletId;
            }

            std::tuple<ui64, ui8> FirstBarrier() const {
                Y_VERIFY_DEBUG(!Barriers.empty());
                const auto& barrier = Barriers.front();
                return {barrier.TabletId, barrier.Channel};
            }

            TLogoBlobID FirstBlob() const {
                Y_VERIFY_DEBUG(!Blobs.empty());
                return Blobs.front().Id;
            }

            std::variant<ui64, std::tuple<ui64, ui8>, TLogoBlobID> FirstKey() const {
                if (!Blocks.empty()) {
                    return FirstBlock();
                } else if (!Barriers.empty()) {
                    return FirstBarrier();
                } else if (!Blobs.empty()) {
                    return FirstBlob();
                } else {
                    Y_FAIL();
                }
            }

            template<typename T>
            void PopFirstItem(T&& callback) {
                if (!Blocks.empty()) {
                    callback(Blocks.front());
                    Blocks.pop_front();
                } else if (!Barriers.empty()) {
                    callback(Barriers.front());
                    Barriers.pop_front();
                } else if (!Blobs.empty()) {
                    callback(Blobs.front());
                    Blobs.pop_front();
                } else {
                    Y_FAIL();
                }
            }

            struct THeapCompare {
                bool operator ()(const TPerDiskState *x, const TPerDiskState *y) const {
                    if (!x->Blocks.empty() && !y->Blocks.empty()) {
                        return x->FirstBlock() > y->FirstBlock();
                    } else if (x->Blocks.empty() != y->Blocks.empty()) {
                        return x->Blocks.empty() > y->Blocks.empty();
                    } else if (!x->Barriers.empty() && !y->Barriers.empty()) {
                        return x->FirstBarrier() > y->FirstBarrier();
                    } else if (x->Barriers.empty() != y->Barriers.empty()) {
                        return x->Barriers.empty() > y->Barriers.empty();
                    } else if (!x->Blobs.empty() && !y->Blobs.empty()) {
                        return x->FirstBlob() > y->FirstBlob();
                    } else {
                        return x->Barriers.empty() > y->Barriers.empty();
                    }
                }
            };

            bool Exhausted() const {
                return Blocks.empty() && Barriers.empty() && Blobs.empty();
            }
        };

        std::vector<TPerDiskState> PerDiskState;
        std::vector<TPerDiskState*> Heap;
        std::optional<ui64> LastProcessedBlock;
        std::optional<std::tuple<ui64, ui8>> LastProcessedBarrier;
        std::optional<TLogoBlobID> LastProcessedBlob;

        struct TRequestInFlight {
            ui32 OrderNumber;
        };

        using TRequestsInFlight = THashMap<ui64, TRequestInFlight>;

        struct TNodeInfo {
            std::vector<ui32> OrderNumbers;
            THashSet<TRequestsInFlight::value_type*> RequestsInFlight;
        };

        ui32 LastRequestId = 0;
        TRequestsInFlight RequestsInFlight;
        THashMap<ui32, TNodeInfo> Nodes;

        bool AssimilateDataInFlight = false;

    public:
        TGroupAssimilatorFetchMachine(TActorIdentity self, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TActorId blobDepotId, const std::optional<TString>& assimilatorState);
        void Handle(TAutoPtr<IEventHandle>& ev);
        void OnPassAway();

    private:
        void IssueAssimilateCmdToVDisk(ui32 orderNumber);
        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        ui32 EndRequest(ui64 id);
        void Handle(TEvBlobStorage::TEvVAssimilateResult::TPtr ev);
        void HandleAssimilateDataConfirm();
        void Merge();
        void MergeDone();
    };

} // NKikimr::NBlobDepot
