#pragma once

#include "defs.h"

namespace NKikimr::NBsController {

    template<typename TKey, typename TValue>
    class TMultiversionObjectMap {
        // a single item containing version and object pair; object may be marked as deleted if there is nullopt in the
        // value; there may be no two consequent deletion marks; also there could be no starting deletion mark -- it
        // should be deleted (starting item for a key is its committed value and deletion mark can't be committed)
        struct TItem {
            ui64 Version;
            std::optional<TValue> Value; // deleted if nullopt

            template<typename... TArgs>
            TItem(ui64 version, TArgs&&... args)
                : Version(version)
                , Value(std::forward<TArgs>(args)...)
            {}

            const TValue& operator *() const {
                return *Value;
            }

            operator const TValue *() const {
                return Value ? &Value.value() : nullptr;
            }

            operator TValue *() {
                return Value ? &Value.value() : nullptr;
            }

            operator bool() const {
                return Value;
            }

            template<typename... TArgs>
            void Construct(TArgs&&... args) {
                Y_DEBUG_ABORT_UNLESS(!Value);
                Value.emplace(std::forward<TArgs>(args)...);
            }

            void Destruct() {
                Y_DEBUG_ABORT_UNLESS(Value);
                Value.reset();
            }
        };

        // a map of items; for a single key there is a sorted history of versions in ascending order contaning different
        // object values or deletion marks
        using TItems = std::multimap<TKey, TItem>;
        std::multimap<TKey, TItem> Items;

        // per-version information structure
        struct TPerVersionInfo {
            struct TComp {
                bool operator ()(const typename TItems::iterator& x, const typename TItems::iterator& y) const {
                    return &*x < &*y;
                }
            };
            std::set<typename TItems::iterator, TComp> Touched; // a set of touched items; every item must have matching version
        };

        // per-version map
        std::map<ui64, TPerVersionInfo> PerVersionInfo;

        // current transaction version
        ui64 CurrentVersion = 0;

        // last committed version
        ui64 CommittedVersion = 0;

    public:
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TRANSACTIONS
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // start new transaction -- within this transaction any changes may be made
        void BeginTx(ui64 version) {
            Y_DEBUG_ABORT_UNLESS(!CurrentVersion);
            Y_DEBUG_ABORT_UNLESS(version);
            CurrentVersion = version;
            Y_DEBUG_ABORT_UNLESS(PerVersionInfo.empty() || PerVersionInfo.rbegin()->first < version);
            PerVersionInfo.emplace_hint(PerVersionInfo.end(), version, TPerVersionInfo());
        }

        // finish transaction -- it is not yet committed, but no more changes are made after this point
        void FinishTx() {
            Y_DEBUG_ABORT_UNLESS(CurrentVersion != 0);
            Y_DEBUG_ABORT_UNLESS(!PerVersionInfo.empty() && std::prev(PerVersionInfo.end())->first == CurrentVersion);
            CurrentVersion = 0;
        }

        // commit version in front of transaction queue
        void Commit(ui64 version) {
            const auto pvIter = PerVersionInfo.begin();
            Y_DEBUG_ABORT_UNLESS(pvIter != PerVersionInfo.end() && pvIter->first == version);
            // the main logic of commit is to remove obsolete versions of items to ensure at most one item with Version
            // <= CommittedVersion exists
            for (typename TItems::iterator it : pvIter->second.Touched) {
                // ensure the correctness of item's version
                Y_DEBUG_ABORT_UNLESS(it->second.Version == version);

                // this item was either newly added in this version, deleted, or replaced; let's figure out what happened
                if (const auto pred = Predecessor(it); pred != Items.end()) {
                    // item has a predecessor, so it may be either added or replaced; predecessor must have correct version
                    // and value in either way
                    Y_DEBUG_ABORT_UNLESS(pred->second.Version < version); // check version ordering
                    Y_DEBUG_ABORT_UNLESS(pred->second); // check that predecessor has value
                    Items.erase(pred); // terminate the predecessor as we don't need it anymore
                    Y_DEBUG_ABORT_UNLESS(Predecessor(it) == Items.end()); // ensure that there are no more predecessors
                    if (!it->second) {
                        Items.erase(it); // item was deleted itself in this version, so we delete it from the map
                    }
                } else {
                    // item does not have a predecessor, so it means that it is newly added, so it must have a value
                    Y_DEBUG_ABORT_UNLESS(it->second);
                }
            }
            PerVersionInfo.erase(pvIter);
            // update last committed version
            CommittedVersion = version;
        }

        // drop version in back of transaction queue
        void Drop(ui64 version) {
            Y_DEBUG_ABORT_UNLESS(!PerVersionInfo.empty());
            const auto pvIter = std::prev(PerVersionInfo.end());
            Y_DEBUG_ABORT_UNLESS(pvIter->first == version);
            // the logic of drop is to delete all entries with the specified version; actually it is simple delete
            // touched items from the map
            for (const typename TItems::iterator it : pvIter->second.Touched) {
                Y_DEBUG_ABORT_UNLESS(it->second.Version == version);
                Items.erase(it);
            }
            PerVersionInfo.erase(pvIter);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // READ-ONLY CODE
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        const TValue *FindCommitted(const TKey& key) {
            // find the item with the least version and ensure it is committed
            const auto it = Items.lower_bound(key);
            return it != Items.end() && it->first == key && it->second.Version <= CommittedVersion ? static_cast<const TValue*>(it->second) : nullptr;
        }

        const TValue *FindLatest(const TKey& key) {
            Y_DEBUG_ABORT_UNLESS(CurrentVersion);
            // find the item with the most version for this key; should be called only from inside the transaction code
            auto it = Items.upper_bound(key);
            if (it != Items.begin()) {
                --it;
            }
            return it != Items.end() && it->first == key ? static_cast<const TValue*>(it->second) : nullptr;
        }

        template<typename T>
        void ScanRangeCommitted(T&& callback, const std::optional<TKey>& min = std::nullopt,
                const std::optional<TKey>& max = std::nullopt) {
            auto it = min ? Items.lower_bound(*min) : Items.begin();
            while (it != Items.end() && (!max || it->first <= *max)) {
                const TKey& key = it->first;
                if (it->second.Version <= CommittedVersion) {
                    Y_DEBUG_ABORT_UNLESS(it->second);
                    if (!callback(key, *it->second)) {
                        break;
                    }
                }
                while (it != Items.end() && it->first == key) {
                    ++it;
                }
            }
        }

        template<typename T>
        void ScanRangeLatest(T&& callback, const std::optional<TKey>& min = std::nullopt,
                const std::optional<TKey>& max = std::nullopt) {
            Y_DEBUG_ABORT_UNLESS(CurrentVersion);
            for (auto it = min ? Items.lower_bound(*min) : Items.begin(); it != Items.end() && (!max || it->first <= *max); ++it) {
                const TKey& key = it->first;

                // find the latest version of this key
                while (it != Items.end() && it->first == key) {
                    ++it;
                }
                --it;

                // check if the item has value
                if (it->second) {
                    auto getMutPtr = [&] {
                        if (it->second.Version != CurrentVersion) {
                            Y_DEBUG_ABORT_UNLESS(it->second.Version < CurrentVersion);
                            const TValue *value = it->second;
                            Y_DEBUG_ABORT_UNLESS(value);
                            it = Items.emplace_hint(std::next(it), CurrentVersion, *value);
                            MarkTouched(it);
                        }
                        return static_cast<TValue*>(it->second);
                    };
                    if (!callback(key, *it->second, getMutPtr)) {
                        break;
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // UPDATE CODE
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        template<typename... TArgs>
        TValue *CreateNewEntry(TKey key, TArgs&&... args) {
            Y_DEBUG_ABORT_UNLESS(CurrentVersion);
            // here we have to possible cases:
            // 1. this is the new entry for this version, so we just place it into the map and update touched keys
            // 2. this is re-creation of alreay deleted item inside this version, so we just have to construct item again
            const auto upperIt = Items.upper_bound(key);
            auto it = upperIt != Items.begin() ? std::prev(upperIt) : Items.end();
            if (it != Items.end() && it->first == key && it->second.Version == CurrentVersion) {
                // the second case -- entry already exists and was deleted; if it wasn't deleted, the program will fail
                it->second.Construct(std::forward<TArgs>(args)...);
            } else {
                // item wasn't inserted for this version before, so we construct new entry
                it = Items.emplace_hint(upperIt, std::piecewise_construct, std::forward_as_tuple(std::move(key)),
                    std::forward_as_tuple(CurrentVersion, std::in_place, std::forward<TArgs>(args)...));
                // and mark it touched
                MarkTouched(it);
            }
            return it->second;
        }

        void DeleteExistingEntry(const TKey& key) {
            Y_DEBUG_ABORT_UNLESS(CurrentVersion);
            // we also have two possible cases as with insertion:
            // 1. item was inserted in previous version (and it must exist, so last value must be non-nullopt)
            // 2. item was constructed in this version and is going to be deleted
            const auto upperIt = Items.upper_bound(key);
            const auto it = upperIt != Items.begin() ? std::prev(upperIt) : Items.end();
            if (it != Items.end() && it->first == key && it->second.Version == CurrentVersion) {
                // the item being deleted may have a predecessor, which may have or may have no value; in first case,
                // it means that in current version item was first replaced, but then got erased, and we have to put
                // deletion mark; in second case, it means item was first created, but then erased, and we can delete
                // this item completely
                if (const auto pred = Predecessor(it); pred != Items.end() && pred->second) {
                    // item was first replaced/re-created, and then got deleted
                    it->second.Destruct();
                } else {
                    // item was created and then destroyed
                    MarkUntouched(it);
                    Items.erase(it);
                }
            } else {
                // the first case, create new entry and mark it touched
                MarkTouched(Items.emplace_hint(upperIt, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(CurrentVersion, std::nullopt)));
            }
        }

        TValue *FindForUpdate(const TKey& key) {
            Y_DEBUG_ABORT_UNLESS(CurrentVersion);
            // the code is same as before, but with some minor differences
            const auto upperIt = Items.upper_bound(key);
            auto it = upperIt != Items.begin() ? std::prev(upperIt) : Items.end();
            if (it == Items.end() || it->first != key) {
                // here we found that there is no item by such key, so we simply return null
                return nullptr;
            }
            if (it->second.Version != CurrentVersion) {
                Y_DEBUG_ABORT_UNLESS(it->second.Version < CurrentVersion);
                // here we found something from previous version; this may be an item, but can be a deletion marker
                if (!it->second) {
                    return nullptr; // item was deleted, so in this version we have no item at all by this key
                }
                // item was not deleted, we have to clone it in current version
                const TValue *value = it->second;
                Y_DEBUG_ABORT_UNLESS(value);
                it = Items.emplace_hint(upperIt, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(CurrentVersion, *value));
                MarkTouched(it);
            }
            return it->second;
        }

    private:
        void MarkTouched(typename TItems::iterator it) {
            Y_DEBUG_ABORT_UNLESS(!PerVersionInfo.empty());
            const auto pvIter = std::prev(PerVersionInfo.end());
            Y_DEBUG_ABORT_UNLESS(pvIter->first == CurrentVersion);
            const bool inserted = pvIter->second.Touched.insert(it).second;
            Y_DEBUG_ABORT_UNLESS(inserted);
        }

        void MarkUntouched(typename TItems::iterator it) {
            Y_DEBUG_ABORT_UNLESS(!PerVersionInfo.empty());
            const auto pvIter = std::prev(PerVersionInfo.end());
            Y_DEBUG_ABORT_UNLESS(pvIter->first == CurrentVersion);
            const ui32 erased = pvIter->second.Touched.erase(it);
            Y_DEBUG_ABORT_UNLESS(erased);
        }

        typename TItems::iterator Predecessor(const typename TItems::iterator& it) {
            const auto prevIt = it != Items.begin() ? std::prev(it) : Items.end();
            return prevIt != Items.end() && prevIt->first == it->first ? prevIt : Items.end();
        }
    };

} // NKikimr::NBsController
