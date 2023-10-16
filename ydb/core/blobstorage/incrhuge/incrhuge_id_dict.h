#pragma once

#include "defs.h"

#include <library/cpp/containers/intrusive_rb_tree/rb_tree.h>
#include <util/generic/intrlist.h>
#include <util/generic/bitmap.h>

namespace NKikimr {
    namespace NIncrHuge {

        template<typename TValue, typename TPageSize, TPageSize PageSize>
        class TIdLookupTable {
            template<typename TPage>
            struct TComparePageId {
                static bool Compare(const TPage& left, const TPage& right) {
                    return left.PageId < right.PageId;
                }
                static bool Compare(const TPage& left, ui64 right) {
                    return left.PageId < right;
                }
                static bool Compare(ui64 left, const TPage& right) {
                    return left < right.PageId;
                }
            };

            struct TPage
                : public TRbTreeItem<TPage, TComparePageId<TPage>>
                , public TIntrusiveListItem<TPage>
            {
                union TItem {
                    TValue Value;
                    TPageSize NextFreeItem;
                };

                TItem             Items[PageSize];
                TBitMap<PageSize> UsedItemsMap;
                TPageSize         FirstFreeItem;
                TPageSize         NumUsedItems;
                ui64              PageId;

                TPage()
                    : FirstFreeItem(0)
                    , NumUsedItems(0)
                    , PageId(0)
                {
                    for (TPageSize i = 0; i < PageSize; ++i) {
                        Items[i].NextFreeItem = i + 1;
                    }
                }
            };

            TRbTree<TPage, TComparePageId<TPage>> Pages;

            // list of pages with free items; most used pages come first, that is list is ordered in descending
            // NumUsedItems order
            TIntrusiveList<TPage> PagesWithFreeItems;

        public:
            ~TIdLookupTable() {
                auto it = Pages.Begin();
                while (it != Pages.End()) {
                    std::unique_ptr<TPage> page(&*it);
                    Pages.Erase(it++);
                }
            }

            TIncrHugeBlobId Create(TValue&& value) {
                // get a page to write into
                TPage *page;
                if (PagesWithFreeItems) {
                    // there is a page with free items, reuse it
                    page = PagesWithFreeItems.Front();
                    if (++page->NumUsedItems == PageSize) {
                        // this page is filled up to maximum capacity, remove it from this list
                        PagesWithFreeItems.PopFront();
                    }
                } else {
                    // all pages are filled up to maximum; allocate new page
                    auto newPage = std::make_unique<TPage>();

                    // create unique page id
                    if (Pages) {
                        auto lastPage = Pages.End();
                        --lastPage;
                        newPage->PageId = lastPage->PageId + 1;
                    }

                    // insert page into tree
                    Pages.Insert(page = newPage.release());

                    // and then into pages with free items list
                    static_assert(PageSize != 1, "page size can't be 1");
                    page->NumUsedItems = 1;
                    PagesWithFreeItems.PushBack(page);
                }

                TPageSize index = page->FirstFreeItem;
                page->FirstFreeItem = page->Items[index].NextFreeItem;
                page->UsedItemsMap[index] = true;
                try {
                    new(&page->Items[index].Value) TValue(std::move(value));
                } catch (...) {
                    Y_ABORT("TValue ctor should not throw");
                }

                return ComposeId(page->PageId, index);
            }

            TValue *Insert(TIncrHugeBlobId id, TValue&& value, bool findDup = false) {
                ui64 pageId;
                TPageSize index;
                DecomposeId(id, &pageId, &index);

                // find page or create new one if this does not exist
                TPage *page = Pages.Find(pageId);
                if (!page) {
                    auto newPage = std::make_unique<TPage>();
                    newPage->PageId = pageId;
                    Pages.Insert(page = newPage.release());
                    PagesWithFreeItems.PushBack(page);
                }

                // if this item is used, return duplicate entry
                if (page->UsedItemsMap[index] && findDup) {
                    return &page->Items[index].Value;
                }

                // ensure that this item isn't used and set up usage flag
                Y_ABORT_UNLESS(!page->UsedItemsMap[index]);
                page->UsedItemsMap[index] = true;

                // find item referring to this one
                TPageSize *p;
                for (p = &page->FirstFreeItem; *p != index; p = &page->Items[*p].NextFreeItem) {
                    Y_ABORT_UNLESS(!page->UsedItemsMap[*p]);
                }

                // fill it with correct value
                *p = page->Items[index].NextFreeItem;

                // initialize item
                try {
                    new(&page->Items[index].Value) TValue(std::move(value));
                } catch (...) {
                    Y_ABORT("TValue ctor should not throw");
                }

                // adjust usage counter
                ++page->NumUsedItems;
                if (page->NumUsedItems == PageSize) {
                    page->Unlink();
                } else if (page->NumUsedItems == PageSize - 1) {
                    PagesWithFreeItems.PushFront(page);
                } else {
                    typename TIntrusiveList<TPage>::TIterator iter(page);
                    ++iter;
                    while (iter != PagesWithFreeItems.End() && page->NumUsedItems < iter->NumUsedItems) {
                        ++iter;
                    }
                    page->LinkBefore(iter.Item());
                }

                return nullptr;
            }

            TValue& Lookup(TIncrHugeBlobId id) {
                TPage *page;
                TPageSize index;
                bool status = FindExistingItem(id, &page, &index);
                Y_ABORT_UNLESS(status, "not found TIncrHugeBlobId# %016" PRIx64, id);

                // return reference
                return page->Items[index].Value;
            }

            bool Delete(TIncrHugeBlobId id, TValue *value = nullptr) {
                TPage *page;
                TPageSize index;
                bool status = FindExistingItem(id, &page, &index);
                if (!status) {
                    return false;
                }

                if (value) {
                    *value = std::move(page->Items[index].Value);
                }

                page->Items[index].Value.~TValue();
                if (!--page->NumUsedItems) {
                    delete page;
                    page = nullptr;
                } else if (page->NumUsedItems == PageSize - 1) {
                    PagesWithFreeItems.PushFront(page);
                } else {
                    typename TIntrusiveList<TPage>::TIterator iter(page);
                    ++iter;
                    while (iter != PagesWithFreeItems.End() && page->NumUsedItems < iter->NumUsedItems) {
                        ++iter;
                    }
                    page->LinkBefore(iter.Item());
                }

                if (page) {
                    // page was not deleted, so we can set up some pointers
                    page->Items[index].NextFreeItem = page->FirstFreeItem;
                    page->FirstFreeItem = index;
                    page->UsedItemsMap[index] = false;
                }

                return true;
            }

            void Replace(TIncrHugeBlobId id, TValue&& value) {
                TValue& existingValue = Lookup(id);
                existingValue = std::move(value);
            }

            template<typename Func>
            void Enumerate(Func&& callback) const {
                for (auto it = Pages.Begin(); it != Pages.End(); ++it) {
                    for (TPageSize index = 0; index < PageSize; ++index) {
                        if (it->UsedItemsMap[index]) {
                            callback(ComposeId(it->PageId, index), it->Items[index].Value);
                        }
                    }
                }
            }

            size_t GetNumPagesUsed() const {
                size_t numItems = 0;
                for (auto it = Pages.Begin(); it != Pages.End(); ++it) {
                    ++numItems;
                }
                return numItems;
            }

        private:
            bool FindExistingItem(TIncrHugeBlobId id, TPage **page, TPageSize *index) const {
                ui64 pageId;
                DecomposeId(id, &pageId, index);

                // find page containing this item
                *page = Pages.Find(pageId);
                if (!*page) {
                    return false;
                }

                // check if item is used
                return (*page)->UsedItemsMap[*index];
            }

            static TIncrHugeBlobId ComposeId(ui64 pageId, TPageSize index) {
                return pageId * PageSize + index;
            }

            static void DecomposeId(TIncrHugeBlobId id, ui64 *pageId, TPageSize *index) {
                *pageId = id / PageSize;
                *index = id % PageSize;
            }
        };

    } // NIncrHuge
} // NKikimr
