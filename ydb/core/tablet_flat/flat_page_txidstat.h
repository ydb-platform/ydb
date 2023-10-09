#pragma once

#include "flat_page_label.h"
#include "flat_util_binary.h"

#include <vector>
#include <unordered_map>

namespace NKikimr {
namespace NTable {
namespace NPage {

    class TTxIdStatsPage : public TThrRefBase {
    public:
        struct THeader {
            ui64 ItemCount;
        } Y_PACKED;

        struct TItem {
            ui64 TxId_;
            ui64 Rows_;
            ui64 Bytes_;

            ui64 GetTxId() const {
                return TxId_;
            }

            ui64 GetRows() const {
                return Rows_;
            }

            ui64 GetBytes() const {
                return Bytes_;
            }
        } Y_PACKED;

        static_assert(sizeof(THeader) == 8, "Invalid THeader size");
        static_assert(sizeof(TItem) == 24, "Invalid TItem size");

    public:
        TTxIdStatsPage(TSharedData page)
            : Raw(std::move(page))
        {
            const auto got = NPage::TLabelWrapper().Read(Raw, EPage::TxIdStats);

            Y_ABORT_UNLESS(got == ECodec::Plain && got.Version == 0);

            Y_ABORT_UNLESS(sizeof(THeader) <= got.Page.size(),
                    "NPage::TTxIdStatsPage header is out of page bounds");

            auto* header = TDeref<THeader>::At(got.Page.data(), 0);

            Y_ABORT_UNLESS(sizeof(THeader) + header->ItemCount * sizeof(TItem) <= got.Page.size(),
                    "NPage::TTxIdStatsPage items are out of page bounds");

            auto* ptr = TDeref<TItem>::At(got.Page.data(), sizeof(THeader));

            Items = { ptr, ptr + header->ItemCount };
        }

        ui64 Count() const {
            return Items.size();
        }

        TArrayRef<const TItem> GetItems() const {
            return Items;
        }

    private:
        TSharedData Raw;
        TArrayRef<const TItem> Items;
    };

    class TTxIdStatsBuilder {
        struct TStats {
            ui64 Rows = 0;
            ui64 Bytes = 0;
        };

    public:
        TTxIdStatsBuilder() = default;

        void AddRow(ui64 txId, ui64 bytes) {
            auto& stats = Stats[txId];
            stats.Rows++;
            stats.Bytes += bytes;
        }

        explicit operator bool() const {
            return !Stats.empty();
        }

        TSharedData Finish() const noexcept {
            if (Stats.empty()) {
                return { };
            }

            using THeader = TTxIdStatsPage::THeader;
            using TItem = TTxIdStatsPage::TItem;

            std::vector<ui64> txIdList;
            txIdList.reserve(Stats.size());
            for (const auto& pr : Stats) {
                txIdList.push_back(pr.first);
            }
            std::sort(txIdList.begin(), txIdList.end());

            size_t pageSize = (
                    sizeof(TLabel) +
                    sizeof(THeader) +
                    sizeof(TItem) * txIdList.size());

            TSharedData buf = TSharedData::Uninitialized(pageSize);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::TxIdStats, 0, pageSize));

            if (auto* header = out.Skip<THeader>()) {
                header->ItemCount = txIdList.size();
            }

            for (ui64 txId : txIdList) {
                const auto& stats = Stats.at(txId);
                auto* item = out.Skip<TItem>();
                item->TxId_ = txId;
                item->Rows_ = stats.Rows;
                item->Bytes_ = stats.Bytes;
            }

            Y_ABORT_UNLESS(*out == buf.mutable_end());
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            return buf;
        }

    private:
        std::unordered_map<ui64, TStats> Stats;
    };

}   // namespace NPage
}   // namespace NTable
}   // namespace NKikimr
