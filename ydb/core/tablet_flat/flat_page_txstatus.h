#pragma once

#include "flat_page_label.h"
#include "flat_util_binary.h"

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

        static_assert(sizeof(THeader) == 16, "Invalid THeader size");
        static_assert(sizeof(TCommittedItem) == 24, "Invalid TCommittedItem size");
        static_assert(sizeof(TRemovedItem) == 8, "Invalid TRemovedItem size");

    public:
        TTxStatusPage(TSharedData page)
            : Raw(std::move(page))
        {
            const auto got = NPage::TLabelWrapper().Read(Raw, EPage::TxStatus);

            Y_ABORT_UNLESS(got == ECodec::Plain && got.Version == 0);

            Y_ABORT_UNLESS(sizeof(THeader) <= got.Page.size(),
                    "NPage::TTxStatusPage header is out of page bounds");

            const THeader* header = TDeref<THeader>::At(got.Page.data(), 0);

            size_t expectedSize = (
                    sizeof(THeader) +
                    sizeof(TCommittedItem) * header->CommittedCount +
                    sizeof(TRemovedItem) * header->RemovedCount);

            Y_ABORT_UNLESS(expectedSize <= got.Page.size(),
                    "NPage::TTxStatusPage items are out of page bounds");

            const TCommittedItem* ptrCommitted = TDeref<TCommittedItem>::At(header + 1, 0);

            CommittedItems = { ptrCommitted, ptrCommitted + header->CommittedCount };

            const TRemovedItem* ptrRemoved = TDeref<TRemovedItem>::At(ptrCommitted + header->CommittedCount, 0);

            RemovedItems = {ptrRemoved, ptrRemoved + header->RemovedCount };
        }

        TArrayRef<const TCommittedItem> GetCommittedItems() const {
            return CommittedItems;
        }

        TArrayRef<const TRemovedItem> GetRemovedItems() const {
            return RemovedItems;
        }

        const TSharedData& GetRaw() const {
            return Raw;
        }

    private:
        TSharedData Raw;
        TArrayRef<const TCommittedItem> CommittedItems;
        TArrayRef<const TRemovedItem> RemovedItems;
    };

    class TTxStatusBuilder {
        using THeader = TTxStatusPage::THeader;
        using TCommittedItem = TTxStatusPage::TCommittedItem;
        using TRemovedItem = TTxStatusPage::TRemovedItem;

    public:
        TTxStatusBuilder() = default;

        explicit operator bool() const {
            return !CommittedItems.empty() || !RemovedItems.empty();
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

        TSharedData Finish() noexcept {
            if (CommittedItems.empty() && RemovedItems.empty()) {
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

            size_t pageSize = (
                    sizeof(TLabel) +
                    sizeof(THeader) +
                    NUtil::NBin::SizeOf(CommittedItems) +
                    NUtil::NBin::SizeOf(RemovedItems));

            TSharedData buf = TSharedData::Uninitialized(pageSize);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::TxStatus, 0, pageSize));

            if (auto* header = out.Skip<THeader>()) {
                header->CommittedCount = CommittedItems.size();
                header->RemovedCount = RemovedItems.size();
            }

            out.Put(CommittedItems);
            out.Put(RemovedItems);

            Y_ABORT_UNLESS(*out == buf.mutable_end());
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            CommittedItems.clear();
            RemovedItems.clear();
            CommittedMap.clear();
            RemovedMap.clear();
            return buf;
        }

    private:
        TVector<TCommittedItem> CommittedItems;
        TVector<TRemovedItem> RemovedItems;
        THashMap<ui64, size_t> CommittedMap;
        THashMap<ui64, size_t> RemovedMap;
    };

}   // namespace NPage
}   // namespace NTable
}   // namespace NKikimr
