#pragma once
#include "columns_storage.h"
#include "others_storage.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TGeneralIterator {
private:
    std::variant<TColumnsData::TIterator, TOthersData::TIterator> Iterator;
    std::optional<ui32> RemappedKey;
    std::vector<ui32> RemapKeys;

public:
    TGeneralIterator(TColumnsData::TIterator&& iterator, const std::optional<ui32> remappedKey = {})
        : Iterator(iterator)
        , RemappedKey(remappedKey) {
    }
    TGeneralIterator(TOthersData::TIterator&& iterator, const std::vector<ui32>& remapKeys = {})
        : Iterator(iterator)
        , RemapKeys(remapKeys) {
    }
    bool IsColumnKey() const {
        struct TVisitor {
            bool operator()(const TOthersData::TIterator& /*iterator*/) {
                return false;
            }
            bool operator()(const TColumnsData::TIterator& /*iterator*/) {
                return true;
            }
        };
        TVisitor visitor;
        return std::visit(visitor, Iterator);
    }
    bool Next() {
        struct TVisitor {
            bool operator()(TOthersData::TIterator& iterator) {
                return iterator.Next();
            }
            bool operator()(TColumnsData::TIterator& iterator) {
                return iterator.Next();
            }
        };
        return std::visit(TVisitor(), Iterator);
    }
    bool IsValid() const {
        struct TVisitor {
            bool operator()(const TOthersData::TIterator& iterator) {
                return iterator.IsValid();
            }
            bool operator()(const TColumnsData::TIterator& iterator) {
                return iterator.IsValid();
            }
        };
        return std::visit(TVisitor(), Iterator);
    }
    ui32 GetRecordIndex() const {
        struct TVisitor {
            ui32 operator()(const TOthersData::TIterator& iterator) {
                return iterator.GetRecordIndex();
            }
            ui32 operator()(const TColumnsData::TIterator& iterator) {
                return iterator.GetCurrentRecordIndex();
            }
        };
        return std::visit(TVisitor(), Iterator);
    }
    ui32 GetKeyIndex() const {
        struct TVisitor {
        private:
            const TGeneralIterator& Owner;

        public:
            TVisitor(const TGeneralIterator& owner)
                : Owner(owner) {
            }
            ui32 operator()(const TOthersData::TIterator& iterator) {
                return Owner.RemapKeys.size() ? Owner.RemapKeys[iterator.GetKeyIndex()] : iterator.GetKeyIndex();
            }
            ui32 operator()(const TColumnsData::TIterator& iterator) {
                return Owner.RemappedKey.value_or(iterator.GetKeyIndex());
            }
        };
        return std::visit(TVisitor(*this), Iterator);
    }
    std::string_view GetValue() const {
        struct TVisitor {
            std::string_view operator()(const TOthersData::TIterator& iterator) {
                return iterator.GetValue();
            }
            std::string_view operator()(const TColumnsData::TIterator& iterator) {
                return iterator.GetValue();
            }
        };
        return std::visit(TVisitor(), Iterator);
    }

    bool operator<(const TGeneralIterator& item) const {
        return std::tuple(item.GetRecordIndex(), item.GetKeyIndex()) < std::tuple(GetRecordIndex(), GetKeyIndex());
    }
};

class TReadIteratorUnorderedKeys {
private:
    TColumnsData ColumnsData;
    TOthersData OthersData;
    std::vector<TGeneralIterator> Iterators;
    std::vector<TGeneralIterator*> SortedIterators;

public:
    bool IsValid() const {
        return SortedIterators.size();
    }

    TReadIteratorUnorderedKeys(const TColumnsData& columnsData, const TOthersData& othersData)
        : ColumnsData(columnsData)
        , OthersData(othersData) {
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            Iterators.emplace_back(ColumnsData.BuildIterator(i));
        }
        Iterators.emplace_back(OthersData.BuildIterator());
        for (auto&& i : Iterators) {
            SortedIterators.emplace_back(&i);
        }
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecords(const ui32 recordsCount, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        for (ui32 i = 0; i < recordsCount; ++i) {
            startRecordActor(i);
            for (ui32 iIter = 0; iIter < SortedIterators.size();) {
                auto& itColumn = *SortedIterators[iIter];
                while (itColumn.GetRecordIndex() == i) {
                    kvActor(itColumn.GetKeyIndex(), itColumn.GetValue(), itColumn.IsColumnKey());
                    if (!itColumn.Next()) {
                        std::swap(SortedIterators[iIter], SortedIterators[SortedIterators.size() - 1]);
                        SortedIterators.pop_back();
                        break;
                    } else if (itColumn.GetRecordIndex() != i) {
                        ++iIter;
                        break;
                    }
                }
            }
            finishRecordActor();
        }
    }
};

class TReadIteratorOrderedKeys {
private:
    TColumnsData ColumnsData;
    TOthersData OthersData;
    std::vector<TGeneralIterator> Iterators;
    std::vector<TGeneralIterator*> SortedIterators;

public:
    bool IsValid() const {
        return SortedIterators.size();
    }

    struct TIteratorsComparator {
        bool operator()(const TGeneralIterator* l, const TGeneralIterator* r) {
            return *l < *r;
        }
    };

    TReadIteratorOrderedKeys(const TColumnsData& columnsData, const TOthersData& othersData)
        : ColumnsData(columnsData)
        , OthersData(othersData) {
        class TKeyAddress {
        private:
            YDB_READONLY_DEF(std::string_view, Name);
            ui32 OriginalIndex = 0;
            YDB_READONLY(bool, IsColumn, false);

        public:
            ui32 GetOriginalIndex() const {
                return OriginalIndex;
            }

            TKeyAddress(const std::string_view& keyName, const ui32 keyIndex, const bool isColumn)
                : Name(keyName)
                , OriginalIndex(keyIndex)
                , IsColumn(isColumn) {
            }

            bool operator<(const TKeyAddress& item) const {
                return Name < item.Name;
            }
        };

        std::vector<TKeyAddress> addresses;
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            addresses.emplace_back(ColumnsData.GetStats().GetColumnName(i), i, true);
        }
        for (ui32 i = 0; i < OthersData.GetStats().GetColumnsCount(); ++i) {
            addresses.emplace_back(OthersData.GetStats().GetColumnName(i), i, false);
        }
        std::sort(addresses.begin(), addresses.end());
        std::vector<ui32> remapColumns;
        remapColumns.resize(ColumnsData.GetStats().GetColumnsCount());
        std::vector<ui32> remapOthers;
        remapOthers.resize(OthersData.GetStats().GetColumnsCount());
        for (ui32 i = 0; i < addresses.size(); ++i) {
            if (i) {
                AFL_VERIFY(addresses[i].GetName() != addresses[i - 1].GetName());
            }
            if (addresses[i].GetIsColumn()) {
                remapColumns[addresses[i].GetOriginalIndex()] = i;
            } else {
                remapOthers[addresses[i].GetOriginalIndex()] = i;
            }
        }
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            Iterators.emplace_back(ColumnsData.BuildIterator(i), remapColumns[i]);
        }
        Iterators.emplace_back(OthersData.BuildIterator(), remapOthers);
        for (auto&& i : Iterators) {
            SortedIterators.emplace_back(&i);
        }
        std::make_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecord(const ui32 recordIndex, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        while (SortedIterators.size()) {
            if (SortedIterators.front()->GetRecordIndex() > recordIndex) {
                return;
            } else if (SortedIterators.front()->GetRecordIndex() < recordIndex) {
                std::pop_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                if (!itColumn.Next()) {
                    SortedIterators.pop_back();
                } else {
                    std::push_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                }
                continue;
            }
            startRecordActor(recordIndex);
            while (SortedIterators.size() && SortedIterators.front()->GetRecordIndex() == i) {
                std::pop_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                auto& itColumn = *SortedIterators.back();
                kvActor(itColumn.GetKeyIndex(), itColumn.GetValue(), itColumn.IsColumnKey());
                if (!itColumn.Next()) {
                    SortedIterators.pop_back();
                } else {
                    std::push_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                }
            }
            finishRecordActor();
            return;
        }
        startRecordActor(recordIndex);
        finishRecordActor();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
