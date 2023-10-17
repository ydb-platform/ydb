#pragma once

#include <util/generic/bitops.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimr {

    /**
     * Space optimized array of TPage pointers
     */
    template<class TPointer>
    class TPageMap {
        enum : ui32 {
            InvalidPageId = Max<ui32>(),
        };

        struct TEntry {
            ui32 Page = InvalidPageId;
            ui32 Distance;
            TPointer Value;
        };

        template<class T>
        class TFakePointer {
        public:
            TFakePointer(T value)
                : Value(std::move(value))
            { }

            T* operator->() const {
                return &Value;
            }

        private:
            T Value;
        };

    public:
        class TIterator {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = std::pair<ui32, const TPointer&>;
            using difference_type = std::ptrdiff_t;
            using pointer = TFakePointer<const value_type>;
            using reference = const value_type;

        public:
            TIterator() = default;

            TIterator(const TPageMap* self, size_t index)
                : Self(self)
                , Index(index)
            {
                SkipEmpty();
            }

            bool operator==(const TIterator& other) const {
                return Index == other.Index;
            }

            bool operator!=(const TIterator& other) const {
                return Index != other.Index;
            }

            reference operator*() const {
                if (Self->Mask) {
                    Y_DEBUG_ABORT_UNLESS(Index < Self->Entries.size());
                    const auto& entry = Self->Entries[Index];
                    Y_DEBUG_ABORT_UNLESS(entry.Page != InvalidPageId);
                    return { entry.Page, entry.Value };
                } else {
                    Y_DEBUG_ABORT_UNLESS(Index < Self->Pages.size());
                    const auto& value = Self->Pages[Index];
                    Y_DEBUG_ABORT_UNLESS(value);
                    return { Index, value };
                }
            }

            pointer operator->() const {
                return { **this };
            }

            TIterator& operator++() {
                ++Index;
                SkipEmpty();
                return *this;
            }

            TIterator operator++(int) {
                TIterator copy = *this;
                ++*this;
                return copy;
            }

        private:
            void SkipEmpty() {
                if (Self->Mask) {
                    while (Index < Self->Entries.size() && Self->Entries[Index].Page == InvalidPageId) {
                        ++Index;
                    }
                    Y_ABORT_UNLESS(Index <= Self->Entries.size());
                } else {
                    while (Index < Self->Pages.size() && !Self->Pages[Index]) {
                        ++Index;
                    }
                    Y_ABORT_UNLESS(Index <= Self->Pages.size());
                }
            }

        private:
            const TPageMap* Self;
            size_t Index;
        };

    public:
        TPageMap()
            : MaxPages(0)
            , MaxEntries(0)
            , Mask(0)
            , Used(0)
            , Dummy() // initializes to nullptr for raw pointers
        { }

        size_t size() const {
            return MaxPages;
        }

        size_t used() const {
            return Used;
        }

        bool empty() const {
            return size() == 0;
        }

        TIterator begin() const {
            return TIterator(this, 0);
        }

        TIterator end() const {
            return TIterator(this, Mask ? Entries.size() : Pages.size());
        }

        const TPointer& operator[](ui32 page) const {
            Y_DEBUG_ABORT_UNLESS(page < MaxPages,
                "Trying to lookup page %" PRIu32 ", size is %" PRISZT,
                page, MaxPages);

            return FindPage(page);
        }

        bool emplace(ui32 page, TPointer value) {
            Y_DEBUG_ABORT_UNLESS(page < MaxPages,
                "Trying to insert page %" PRIu32 ", size is %" PRISZT,
                page, MaxPages);

            Y_DEBUG_ABORT_UNLESS(value,
                "Trying to insert page %" PRIu32 " with an empty value",
                page);

            MaybeGrow(Used + 1);

            if (EmplacePage(page, std::move(value))) {
                ++Used;
                return true;
            }

            return false;
        }

        bool erase(ui32 page) {
            Y_DEBUG_ABORT_UNLESS(page < MaxPages,
                "Trying to erase page %" PRIu32 ", size is %" PRISZT,
                page, MaxPages);

            if (ErasePage(page)) {
                MaybeShrink(--Used);
                return true;
            }

            return false;
        }

        void clear() {
            Mask = 0;
            Used = 0;
            DestroyVector(std::move(Entries));
            DestroyVector(std::move(Pages));
        }

        void resize(size_t max_pages) {
            if (MaxPages == max_pages) {
                return;
            }

            Y_ABORT_UNLESS(MaxPages < max_pages,
                "Decreasing virtual size from %" PRISZT " to %" PRISZT " not supported",
                MaxPages, max_pages);

            Y_DEBUG_ABORT_UNLESS(max_pages < InvalidPageId,
                "Cannot resize to %" PRISZT " pages (must be less than %" PRIu32 ")",
                max_pages, InvalidPageId);

            MaxPages = max_pages;
            MaxEntries = 0;

            size_t limit = sizeof(TPointer) * max_pages;
            for (size_t entries = 2; entries; entries <<= 1) {
                size_t size = sizeof(TEntry) * entries;

                if (size >= limit) {
                    // Hash table of this size will use more memory than simple vector
                    break;
                }

                MaxEntries = entries;
            }

            // N.B. We don't need to rehash, since MaxEntries did not decrease
        }

        bool IsHashTable() const {
            return Mask != 0;
        }

    private:
        size_t ExpectedIndex(ui32 page) const {
            // Finalization mix (MurmurHash 3)
            ui64 k = static_cast<ui64>(page);
            k ^= k >> 33;
            k *= 0xff51afd7ed558ccdULL;
            k ^= k >> 33;
            k *= 0xc4ceb9fe1a85ec53ULL;
            k ^= k >> 33;
            return static_cast<size_t>(k) & Mask;
        }

        const TPointer& FindPage(ui32 page) const {
            if (!Mask) {
                if (page < Pages.size()) {
                    return Pages[page];
                } else {
                    return Dummy;
                }
            }

            size_t index = ExpectedIndex(page);
            ui32 distance = 0;
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(index < Entries.size());
                auto& entry = Entries[index];
                if (entry.Page == page) {
                    return entry.Value; // found
                }
                if (entry.Page == InvalidPageId || entry.Distance < distance) {
                    return Dummy; // not found
                }
                index = (index + 1) & Mask;
                ++distance;
            }
        }

        bool EmplacePage(ui32 page, TPointer value) {
            if (!Mask) {
                Y_DEBUG_ABORT_UNLESS(page < Pages.size());
                if (!Pages[page]) {
                    Pages[page] = std::move(value);
                    return true;
                } else {
                    return false;
                }
            }

            size_t index = ExpectedIndex(page);
            ui32 distance = 0;
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(index < Entries.size());
                auto& entry = Entries[index];
                if (entry.Page == page) {
                    // Found existing page, cannot emplace
                    return false;
                }
                if (entry.Page == InvalidPageId) {
                    // Found empty slot, claim this index
                    entry.Page = page;
                    entry.Distance = distance;
                    entry.Value = std::move(value);
                    return true;
                }
                if (entry.Distance < distance) {
                    // Displace current index with our page
                    std::swap(entry.Page, page);
                    std::swap(entry.Distance, distance);
                    std::swap(entry.Value, value);
                    break;
                }
                index = (index + 1) & Mask;
                ++distance;
            }

            // Some value has been displaced, shift it down
            for (;;) {
                index = (index + 1) & Mask;
                ++distance;
                Y_DEBUG_ABORT_UNLESS(index < Entries.size());
                auto& entry = Entries[index];
                if (entry.Page == InvalidPageId) {
                    entry.Page = page;
                    entry.Distance = distance;
                    entry.Value = std::move(value);
                    break;
                }
                if (entry.Distance < distance) {
                    std::swap(entry.Page, page);
                    std::swap(entry.Distance, distance);
                    std::swap(entry.Value, value);
                }
            }

            return true;
        }

        bool ErasePage(ui32 page) {
            if (!Mask) {
                if (page < Pages.size() && Pages[page]) {
                    Pages[page] = TPointer();
                    return true;
                } else {
                    return false;
                }
            }

            size_t index = ExpectedIndex(page);
            size_t distance = 0;
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(index < Entries.size());
                auto& entry = Entries[index];
                if (entry.Page == page) {
                    entry.Page = InvalidPageId;
                    entry.Value = TPointer();
                    break;
                }
                if (entry.Page == InvalidPageId || entry.Distance < distance) {
                    return false; // not found
                }
                index = (index + 1) & Mask;
                ++distance;
            }

            // We now have a hole at index, move stuff up
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(index < Entries.size());
                auto& entry = Entries[index];
                Y_DEBUG_ABORT_UNLESS(entry.Page == InvalidPageId);
                index = (index + 1) & Mask;
                Y_DEBUG_ABORT_UNLESS(index < Entries.size());
                auto& next = Entries[index];
                if (next.Page == InvalidPageId || !next.Distance) {
                    break;
                }
                entry.Page = next.Page;
                entry.Distance = next.Distance - 1;
                entry.Value = std::move(next.Value);
                next.Page = InvalidPageId;
                next.Value = TPointer(); // N.B. backup for simple pointers, missing move assignment, etc.
            }

            return true;
        }

        void MaybeGrow(size_t count) {
            // See if count pages would occupy more than 50%
            const size_t min_entries = count * 2;
            if (Mask ? (min_entries > Entries.size()) : (MaxPages > Pages.size())) {
                Rehash(min_entries);
            }
        }

        void MaybeShrink(size_t count) {
            // See if count pages would fit in 25% of a smaller hash table
            // This way hash table will not need to grow immediately
            if (count && count * 8 < (Mask ? Entries.size() : MaxEntries)) {
                Rehash(count * 4);
            }
        }

        void Init(size_t min_entries) {
            Mask = 0;
            Used = 0;
            DestroyVector(std::move(Entries));
            DestroyVector(std::move(Pages));

            if (!min_entries) {
                return;
            }

            size_t entries = FastClp2(min_entries);
            if (entries && entries <= MaxEntries) {
                Y_DEBUG_ABORT_UNLESS(entries >= 2);
                Mask = entries - 1;
                Entries.resize(entries);
            } else {
                Mask = 0;
                Pages.resize(MaxPages);
            }
        }

        void Rehash(size_t min_entries) {
            if (Mask) {
                Rehash(min_entries, std::move(Entries));
            } else {
                Rehash(min_entries, std::move(Pages));
            }
        }

        void Rehash(size_t min_entries, TVector<TEntry> entries) {
            auto prevUsed = Used;
            Init(min_entries);
            for (auto& entry : entries) {
                if (entry.Page != InvalidPageId) {
                    Y_DEBUG_ABORT_UNLESS(Used < min_entries);
                    Y_ABORT_UNLESS(EmplacePage(entry.Page, std::move(entry.Value)));
                    ++Used;
                }
            }
            Y_DEBUG_ABORT_UNLESS(Used == prevUsed);
        }

        void Rehash(size_t min_entries, TVector<TPointer> pages) {
            auto prevUsed = Used;
            Init(min_entries);
            ui32 page = 0;
            for (auto& value : pages) {
                if (value) {
                    Y_DEBUG_ABORT_UNLESS(Used < min_entries);
                    Y_ABORT_UNLESS(EmplacePage(page, std::move(value)));
                    ++Used;
                }
                ++page;
            }
            Y_DEBUG_ABORT_UNLESS(Used == prevUsed);
        }

        template<class T>
        static void DestroyVector(TVector<T>&& v) {
            TVector<T> tmp;
            tmp.swap(v);
        }

    private:
        size_t MaxPages;
        size_t MaxEntries;
        size_t Mask;
        size_t Used;
        TVector<TEntry> Entries;
        TVector<TPointer> Pages;
        TPointer Dummy;
    };

}
