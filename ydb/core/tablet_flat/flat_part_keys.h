#pragma once
#include "flat_page_data.h"
#include "flat_part_iface.h"
#include "flat_part_index_iter_iface.h"
#include "flat_part_slice.h"
#include "flat_sausage_fetch.h"
#include "flat_sausagecache.h"

namespace NKikimr {
namespace NTable {

    class TKeysEnv : public IPages {
    public:
        using TCache = NTabletFlatExecutor::TPrivatePageCache::TInfo;

        TKeysEnv(const TPart *part, TIntrusivePtr<TCache> cache)
            : Part(part)
            , Cache(std::move(cache))
        {
        }

        TResult Locate(const TMemTable*, ui64, ui32) noexcept override
        {
            Y_ABORT("IPages::Locate(TMemTable*, ...) shouldn't be used here");
        }

        TResult Locate(const TPart*, ui64, ELargeObj) noexcept override
        {
            Y_ABORT("IPages::Locate(TPart*, ...) shouldn't be used here");
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            Y_ABORT_UNLESS(part == Part, "Unsupported part");
            Y_ABORT_UNLESS(groupId.IsMain(), "Unsupported column group");

            if (auto* extra = ExtraPages.FindPtr(pageId)) {
                return extra;
            } else if (auto* cached = Cache->Lookup(pageId)) {
                // Save page in case it's evicted on the next iteration
                ExtraPages[pageId] = *cached;
                return cached;
            } else {
                NeedPages.insert(pageId);
                return nullptr;
            }
        }

        void Check(bool has) const noexcept
        {
            Y_ABORT_UNLESS(bool(NeedPages) == has, "Loader does not have some ne");
        }

        TAutoPtr<NPageCollection::TFetch> GetFetches()
        {
            if (NeedPages) {
                TVector<TPageId> pages(NeedPages.begin(), NeedPages.end());
                std::sort(pages.begin(), pages.end());
                return new NPageCollection::TFetch{ 0, Cache->PageCollection, std::move(pages) };
            } else {
                return nullptr;
            }
        }

        void Save(ui32 cookie, NSharedCache::TEvResult::TLoaded&& loaded) noexcept
        {
            if (cookie == 0 && NeedPages.erase(loaded.PageId)) {
                ExtraPages[loaded.PageId] = TPinnedPageRef(loaded.Page).GetData();
                Cache->Fill(std::move(loaded));
            }
        }

    private:
        const TPart* Part;
        TIntrusivePtr<TCache> Cache;
        THashMap<TPageId, TSharedData> ExtraPages;
        THashSet<TPageId> NeedPages;
    };

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
