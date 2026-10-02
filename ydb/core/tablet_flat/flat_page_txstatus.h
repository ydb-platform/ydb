#pragma once

#include "flat_page_label.h"
#include "flat_table_savepoints.h"
#include "flat_util_binary.h"

#include <ydb/core/base/row_version.h>

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

        // Version 1: follows removed items when the page has rolled back savepoint seq nums
        struct TRolledBackHeader {
            ui64 RolledBackCount;
        } Y_PACKED;

        // A rolled back closed range [From, To] of savepoint seq nums of a transaction
        struct TRolledBackItem {
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
        static_assert(sizeof(TRolledBackHeader) == 8, "Invalid TRolledBackHeader size");
        static_assert(sizeof(TRolledBackItem) == 16, "Invalid TRolledBackItem size");

    public:
        TTxStatusPage(TSharedData page)
            : Raw(std::move(page))
        {
            const auto got = NPage::TLabelWrapper().Read(Raw, EPage::TxStatus);

            Y_ENSURE(got == ECodec::Plain && (got.Version == 0 || got.Version == 1));

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
                Y_ENSURE(expectedSize + sizeof(TRolledBackHeader) <= got.Page.size(),
                        "NPage::TTxStatusPage rolled back header is out of page bounds");

                const TRolledBackHeader* rolledBackHeader = TDeref<TRolledBackHeader>::At(ptrRemoved + header->RemovedCount, 0);

                expectedSize += sizeof(TRolledBackHeader) + sizeof(TRolledBackItem) * rolledBackHeader->RolledBackCount;

                Y_ENSURE(expectedSize <= got.Page.size(),
                        "NPage::TTxStatusPage rolled back items are out of page bounds");

                const TRolledBackItem* ptrRolledBack = TDeref<TRolledBackItem>::At(rolledBackHeader + 1, 0);

                RolledBackItems = { ptrRolledBack, ptrRolledBack + rolledBackHeader->RolledBackCount };
            }
        }

        TArrayRef<const TCommittedItem> GetCommittedItems() const {
            return CommittedItems;
        }

        TArrayRef<const TRemovedItem> GetRemovedItems() const {
            return RemovedItems;
        }

        TArrayRef<const TRolledBackItem> GetRolledBackItems() const {
            return RolledBackItems;
        }

        const TSharedData& GetRaw() const {
            return Raw;
        }

    private:
        TSharedData Raw;
        TArrayRef<const TCommittedItem> CommittedItems;
        TArrayRef<const TRemovedItem> RemovedItems;
        TArrayRef<const TRolledBackItem> RolledBackItems;
    };

    class TTxStatusBuilder {
        using THeader = TTxStatusPage::THeader;
        using TCommittedItem = TTxStatusPage::TCommittedItem;
        using TRemovedItem = TTxStatusPage::TRemovedItem;
        using TRolledBackHeader = TTxStatusPage::TRolledBackHeader;
        using TRolledBackItem = TTxStatusPage::TRolledBackItem;

    public:
        TTxStatusBuilder() = default;

        explicit operator bool() const {
            return !CommittedItems.empty() || !RemovedItems.empty() || !RolledBackItems.empty();
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

        void AddRolledBack(ui64 txId, const TSavepointSeqNumRanges& ranges) {
            for (const auto& range : ranges.GetRanges()) {
                auto& item = RolledBackItems.emplace_back();
                item.TxId_ = txId;
                item.From_ = range.From;
                item.To_ = range.To;
            }
        }

        TSharedData Finish() {
            if (CommittedItems.empty() && RemovedItems.empty() && RolledBackItems.empty()) {
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
            std::sort(RolledBackItems.begin(), RolledBackItems.end(),
                [](const TRolledBackItem& a, const TRolledBackItem& b) -> bool {
                    return std::make_pair(a.GetTxId(), a.GetFrom()) < std::make_pair(b.GetTxId(), b.GetFrom());
                });

            // Version 1 is only written when there are rolled back savepoint seq nums
            const bool hasRolledBack = !RolledBackItems.empty();

            size_t pageSize = (
                    sizeof(TLabel) +
                    sizeof(THeader) +
                    NUtil::NBin::SizeOf(CommittedItems) +
                    NUtil::NBin::SizeOf(RemovedItems));
            if (hasRolledBack) {
                pageSize += sizeof(TRolledBackHeader) + NUtil::NBin::SizeOf(RolledBackItems);
            }

            TSharedData buf = TSharedData::Uninitialized(pageSize);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::TxStatus, hasRolledBack ? 1 : 0, pageSize));

            if (auto* header = out.Skip<THeader>()) {
                header->CommittedCount = CommittedItems.size();
                header->RemovedCount = RemovedItems.size();
            }

            out.Put(CommittedItems);
            out.Put(RemovedItems);

            if (hasRolledBack) {
                if (auto* header = out.Skip<TRolledBackHeader>()) {
                    header->RolledBackCount = RolledBackItems.size();
                }

                out.Put(RolledBackItems);
            }

            Y_ENSURE(*out == buf.mutable_end());
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            CommittedItems.clear();
            RemovedItems.clear();
            RolledBackItems.clear();
            CommittedMap.clear();
            RemovedMap.clear();
            return buf;
        }

    private:
        TVector<TCommittedItem> CommittedItems;
        TVector<TRemovedItem> RemovedItems;
        TVector<TRolledBackItem> RolledBackItems;
        THashMap<ui64, size_t> CommittedMap;
        THashMap<ui64, size_t> RemovedMap;
    };

}   // namespace NPage
}   // namespace NTable
}   // namespace NKikimr
