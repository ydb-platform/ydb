#pragma once
#include "flat_page_data.h"
#include "flat_part_iface.h"
#include "flat_part_index_iter_iface.h"
#include "flat_part_slice.h"

namespace NKikimr {
namespace NTable {

    class TKeysLoader {
    public:
        explicit TKeysLoader(const TPart* part, IPages* env)
            : Part(part)
            , Env(env)
            , Index(CreateIndexIter(part, env, {}))
        {
        }

        TIntrusivePtr<TSlices> Do(TIntrusiveConstPtr<TScreen> screen)
        {
            if (!screen) {
                // Use a full screen to simplify logic below
                screen = new TScreen({ TScreen::THole(true) });
            }

            TIntrusivePtr<TSlices> run = new TSlices;

            bool ok = SeekLastRow();

            for (const auto& hole : *screen) {
                TSlice slice;

                if (ok &= SeekRow(hole.Begin)) {
                    if (GetRowId() == Max<TRowId>()) {
                        // all subsequent holes are out of range
                        break;
                    }
                    slice.FirstRowId = GetRowId();
                    slice.FirstKey = GetKey();
                    slice.FirstInclusive = true;
                }
                if (ok &= SeekRow(hole.End)) {
                    if (GetRowId() == Max<TRowId>()) {
                        // Prefer a true inclusive last key
                        if (ok &= SeekLastRow()) {
                            slice.LastRowId = GetRowId();
                            slice.LastKey = GetKey();
                            slice.LastInclusive = true;
                        }
                    } else {
                        slice.LastRowId = GetRowId();
                        slice.LastKey = GetKey();
                        slice.LastInclusive = false;
                    }
                }
                if (ok && slice.Rows() > 0) {
                    run->push_back(std::move(slice));
                }
            }

            return ok ? std::move(run) : nullptr;
        }

    private:
        TRowId GetRowId() const noexcept
        {
            return RowId;
        }

        TSerializedCellVec GetKey() const noexcept
        {
            return TSerializedCellVec(Key);
        }

        bool SeekRow(TRowId rowId) noexcept
        {
            if (RowId != rowId) {
                if (rowId == Max<TRowId>()) {
                    RowId = rowId;
                    Key = { };
                    return true;
                }

                auto ready = Index->Seek(rowId);
                if (ready == EReady::Page) {
                    return false;
                } else if (ready == EReady::Gone) {
                    // Row is out of range for this part
                    RowId = Max<TRowId>();
                    Key = { };
                    return true;
                }

                Y_ABORT_UNLESS(Index->GetRowId() <= rowId, "SeekIndex invariant failure");
                if (!LoadPage(Index->GetPageId())) {
                    return false;
                }
                Y_ABORT_UNLESS(Page.BaseRow() == Index->GetRowId(), "Index and data are out of sync");
                auto lastRowId = Page.BaseRow() + (Page->Count - 1);
                if (lastRowId < rowId) {
                    // Row is out of range for this page
                    RowId = Max<TRowId>();
                    Key = { };
                    return true;
                }
                LoadRow(rowId);
            }
            return true;
        }

        bool SeekLastRow() noexcept
        {
            auto hasLast = Index->SeekLast();
            if (hasLast == EReady::Page) {
                return false;
            }
            Y_ABORT_UNLESS(hasLast != EReady::Gone, "Unexpected failure to find the last index record");

            if (!LoadPage(Index->GetPageId())) {
                return false;
            }
            Y_ABORT_UNLESS(Page.BaseRow() == Index->GetRowId(), "Index and data are out of sync");
            auto lastRowId = Page.BaseRow() + (Page->Count - 1);
            LoadRow(lastRowId);
            return true;
        }

        bool LoadPage(TPageId pageId) noexcept
        {
            Y_ABORT_UNLESS(pageId != Max<TPageId>(), "Unexpected seek to an invalid page id");
            if (PageId != pageId) {
                if (auto* data = Env->TryGetPage(Part, pageId, {})) {
                    Y_ABORT_UNLESS(Page.Set(data), "Unexpected failure to load data page");
                    PageId = pageId;
                } else {
                    return false;
                }
            }
            return true;
        }

        void LoadRow(TRowId rowId) noexcept
        {
            if (RowId != rowId) {
                auto it = Page->Begin() + (rowId - Page.BaseRow());
                Y_ABORT_UNLESS(it, "Unexpected failure to find row on the data page");
                Key.clear();
                for (const auto& info : Part->Scheme->Groups[0].ColsKeyData) {
                    Key.push_back(it->Cell(info));
                }
                RowId = rowId;
            }
        }

    private:
        const TPart* Part;
        IPages* Env;
        TRowId RowId = Max<TRowId>();
        TPageId PageId = Max<TPageId>();
        THolder<IPartGroupIndexIter> Index;
        NPage::TDataPage Page;
        TSmallVec<TCell> Key;
    };

}
}
