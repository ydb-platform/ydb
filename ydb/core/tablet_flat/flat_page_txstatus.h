#pragma once

#include "flat_page_label.h"
#include "flat_table_savepoints.h"
#include "flat_util_binary.h"

#include <ydb/core/base/row_version.h>

#include <util/generic/hash_set.h>

#include <vector>
#include <unordered_map>

namespace NKikimr {
namespace NTable {
namespace NPage {

    class TTxStatusPage : public TThrRefBase {
    public:
        struct THeader {
            ui64 CommittedCount;
            ui64 RemovedCount;
        } Y_PACKED;

        struct TCommittedItem {
            ui64 TxId_;
            ui64 RowVersionStep_;
            ui64 RowVersionTxId_;

            ui64 GetTxId() const {
                return TxId_;
            }

            TRowVersion GetRowVersion() const {
                return TRowVersion(RowVersionStep_, RowVersionTxId_);
            }
        } Y_PACKED;

        struct TRemovedItem {
            ui64 TxId_;

            ui64 GetTxId() const {
                return TxId_;
            }
        } Y_PACKED;

        // Version 1: follows removed items when the page has removed operations (savepoint seq nums)
        struct TRemovedOpsHeader {
            ui64 RemovedOpsCount;
        } Y_PACKED;

        // A removed closed range [From, To] of savepoint seq nums of a transaction,
        // 0 < From <= To. Items are stored sorted by (TxId, From), so ranges of a
        // transaction are contiguous, and ranges of a transaction neither overlap
        // nor touch each other; this is validated on load and relied upon by readers.
        struct TRemovedOpsItem {
            ui64 TxId_;
            ui32 From_;
            ui32 To_;

            ui64 GetTxId() const {
                return TxId_;
            }

            ui32 GetFrom() const {
                return From_;
            }

            ui32 GetTo() const {
                return To_;
            }
        } Y_PACKED;

        static_assert(sizeof(THeader) == 16, "Invalid THeader size");
        static_assert(sizeof(TCommittedItem) == 24, "Invalid TCommittedItem size");
        static_assert(sizeof(TRemovedItem) == 8, "Invalid TRemovedItem size");
        static_assert(sizeof(TRemovedOpsHeader) == 8, "Invalid TRemovedOpsHeader size");
        static_assert(sizeof(TRemovedOpsItem) == 16, "Invalid TRemovedOpsItem size");

    public:
        TTxStatusPage(TSharedData page)
            : Raw(std::move(page))
        {
            const auto got = NPage::TLabelWrapper().Read(Raw, EPage::TxStatus);

            Y_ENSURE(got == ECodec::Plain, "Unexpected EPage::TxStatus codec");
            Y_ENSURE(got.Version == 0 || got.Version == 1, "Unknown EPage::TxStatus version " << got.Version);

            Y_ENSURE(sizeof(THeader) <= got.Page.size(),
                    "NPage::TTxStatusPage header is out of page bounds");

            const THeader* header = TDeref<THeader>::At(got.Page.data(), 0);

            size_t expectedSize = (
                    sizeof(THeader) +
                    sizeof(TCommittedItem) * header->CommittedCount +
                    sizeof(TRemovedItem) * header->RemovedCount);

            Y_ENSURE(expectedSize <= got.Page.size(),
                    "NPage::TTxStatusPage items are out of page bounds");

            const TCommittedItem* ptrCommitted = TDeref<TCommittedItem>::At(header + 1, 0);

            CommittedItems = { ptrCommitted, ptrCommitted + header->CommittedCount };

            const TRemovedItem* ptrRemoved = TDeref<TRemovedItem>::At(ptrCommitted + header->CommittedCount, 0);

            RemovedItems = {ptrRemoved, ptrRemoved + header->RemovedCount };

            if (got.Version >= 1) {
                Y_ENSURE(expectedSize + sizeof(TRemovedOpsHeader) <= got.Page.size(),
                        "NPage::TTxStatusPage removed ops header is out of page bounds");

                const TRemovedOpsHeader* removedOpsHeader = TDeref<TRemovedOpsHeader>::At(ptrRemoved + header->RemovedCount, 0);

                expectedSize += sizeof(TRemovedOpsHeader) + sizeof(TRemovedOpsItem) * removedOpsHeader->RemovedOpsCount;

                Y_ENSURE(expectedSize <= got.Page.size(),
                        "NPage::TTxStatusPage removed ops items are out of page bounds");

                const TRemovedOpsItem* ptrRemovedOps = TDeref<TRemovedOpsItem>::At(removedOpsHeader + 1, 0);

                RemovedOpsItems = { ptrRemovedOps, ptrRemovedOps + removedOpsHeader->RemovedOpsCount };

                for (size_t index = 0; index < RemovedOpsItems.size(); ++index) {
                    const auto& item = RemovedOpsItems[index];
                    Y_ENSURE(0 < item.GetFrom() && item.GetFrom() <= item.GetTo(),
                            "NPage::TTxStatusPage removed ops item of tx " << item.GetTxId()
                            << " has invalid range [" << item.GetFrom() << ", " << item.GetTo() << "]");
                    if (index > 0) {
                        const auto& prev = RemovedOpsItems[index - 1];
                        Y_ENSURE(std::make_pair(prev.GetTxId(), prev.GetFrom()) < std::make_pair(item.GetTxId(), item.GetFrom()),
                                "NPage::TTxStatusPage removed ops items are not sorted by (TxId, From)");
                        Y_ENSURE(prev.GetTxId() != item.GetTxId() || ui64(prev.GetTo()) + 1 < item.GetFrom(),
                                "NPage::TTxStatusPage removed ops items of tx " << item.GetTxId()
                                << " overlap or are adjacent: [" << prev.GetFrom() << ", " << prev.GetTo()
                                << "] and [" << item.GetFrom() << ", " << item.GetTo() << "]");
                    }
                }
            }
        }

        TArrayRef<const TCommittedItem> GetCommittedItems() const {
            return CommittedItems;
        }

        TArrayRef<const TRemovedItem> GetRemovedItems() const {
            return RemovedItems;
        }

        TArrayRef<const TRemovedOpsItem> GetRemovedOpsItems() const {
            return RemovedOpsItems;
        }

        const TSharedData& GetRaw() const {
            return Raw;
        }

    private:
        TSharedData Raw;
        TArrayRef<const TCommittedItem> CommittedItems;
        TArrayRef<const TRemovedItem> RemovedItems;
        TArrayRef<const TRemovedOpsItem> RemovedOpsItems;
    };

    class TTxStatusBuilder {
        using THeader = TTxStatusPage::THeader;
        using TCommittedItem = TTxStatusPage::TCommittedItem;
        using TRemovedItem = TTxStatusPage::TRemovedItem;
        using TRemovedOpsHeader = TTxStatusPage::TRemovedOpsHeader;
        using TRemovedOpsItem = TTxStatusPage::TRemovedOpsItem;

    public:
        TTxStatusBuilder() = default;

        explicit operator bool() const {
            return !CommittedItems.empty() || !RemovedItems.empty() || !RemovedOpsItems.empty();
        }

        void AddCommitted(ui64 txId, TRowVersion rowVersion) {
            auto it = CommittedMap.find(txId);
            Y_DEBUG_ABORT_UNLESS(it == CommittedMap.end());
            TTxStatusPage::TCommittedItem* item;
            if (it == CommittedMap.end()) {
                size_t index = CommittedItems.size();
                item = &CommittedItems.emplace_back();
                item->TxId_ = txId;
                CommittedMap[txId] = index;
            } else {
                size_t index = it->second;
                item = &CommittedItems[index];
            }
            item->RowVersionStep_ = rowVersion.Step;
            item->RowVersionTxId_ = rowVersion.TxId;
        }

        void AddRemoved(ui64 txId) {
            auto it = RemovedMap.find(txId);
            Y_DEBUG_ABORT_UNLESS(it == RemovedMap.end());
            if (it == RemovedMap.end()) {
                size_t index = RemovedItems.size();
                auto& item = RemovedItems.emplace_back();
                item.TxId_ = txId;
                RemovedMap[txId] = index;
            }
        }

        // Ranges are already merged, so each transaction must be added at most once
        void AddRemovedOps(ui64 txId, const TSavepointSeqNumRanges& ranges) {
            Y_ENSURE(RemovedOpsTxIds.insert(txId).second, "Duplicate removed ops of tx " << txId);
            for (const auto& range : ranges.GetRanges()) {
                auto& item = RemovedOpsItems.emplace_back();
                item.TxId_ = txId;
                item.From_ = range.From;
                item.To_ = range.To;
            }
        }

        TSharedData Finish() {
            if (CommittedItems.empty() && RemovedItems.empty() && RemovedOpsItems.empty()) {
                return { };
            }

            std::sort(CommittedItems.begin(), CommittedItems.end(),
                [](const TCommittedItem& a, const TCommittedItem& b) -> bool {
                    return a.GetTxId() < b.GetTxId();
                });
            std::sort(RemovedItems.begin(), RemovedItems.end(),
                [](const TRemovedItem& a, const TRemovedItem& b) -> bool {
                    return a.GetTxId() < b.GetTxId();
                });
            std::sort(RemovedOpsItems.begin(), RemovedOpsItems.end(),
                [](const TRemovedOpsItem& a, const TRemovedOpsItem& b) -> bool {
                    return std::make_pair(a.GetTxId(), a.GetFrom()) < std::make_pair(b.GetTxId(), b.GetFrom());
                });

            // Version 1 is only written when there are removed operations
            const bool hasRemovedOps = !RemovedOpsItems.empty();

            size_t pageSize = (
                    sizeof(TLabel) +
                    sizeof(THeader) +
                    NUtil::NBin::SizeOf(CommittedItems) +
                    NUtil::NBin::SizeOf(RemovedItems));
            if (hasRemovedOps) {
                pageSize += sizeof(TRemovedOpsHeader) + NUtil::NBin::SizeOf(RemovedOpsItems);
            }

            TSharedData buf = TSharedData::Uninitialized(pageSize);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::TxStatus, hasRemovedOps ? 1 : 0, pageSize));

            if (auto* header = out.Skip<THeader>()) {
                header->CommittedCount = CommittedItems.size();
                header->RemovedCount = RemovedItems.size();
            }

            out.Put(CommittedItems);
            out.Put(RemovedItems);

            if (hasRemovedOps) {
                if (auto* header = out.Skip<TRemovedOpsHeader>()) {
                    header->RemovedOpsCount = RemovedOpsItems.size();
                }

                out.Put(RemovedOpsItems);
            }

            Y_ENSURE(*out == buf.mutable_end());
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            CommittedItems.clear();
            RemovedItems.clear();
            RemovedOpsItems.clear();
            RemovedOpsTxIds.clear();
            CommittedMap.clear();
            RemovedMap.clear();
            return buf;
        }

    private:
        TVector<TCommittedItem> CommittedItems;
        TVector<TRemovedItem> RemovedItems;
        TVector<TRemovedOpsItem> RemovedOpsItems;
        THashMap<ui64, size_t> CommittedMap;
        THashMap<ui64, size_t> RemovedMap;
        THashSet<ui64> RemovedOpsTxIds;
    };

}   // namespace NPage
}   // namespace NTable
}   // namespace NKikimr
