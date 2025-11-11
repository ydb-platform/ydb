#pragma once
#include "columns_storage.h"
#include "others_storage.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TGeneralIterator {
private:
    std::variant<TColumnsData::TIterator, TOthersData::TIterator> Iterator;
    std::optional<ui32> RemappedKey;
    std::vector<ui32> RemapKeys;
    ui32 RecordIndex = 0;
    ui32 KeyIndex = 0;
    bool IsValidFlag = false;
    bool HasValueFlag = false;
    std::string_view Value;
    bool IsColumnKeyFlag = false;

    void InitFromIterator(const TColumnsData::TIterator& iterator) {
        RecordIndex = iterator.GetCurrentRecordIndex();
        KeyIndex = RemappedKey.value_or(iterator.GetKeyIndex());
        IsValidFlag = true;
        HasValueFlag = iterator.HasValue();
        Value = iterator.GetValue();
    }

    void InitFromIterator(const TOthersData::TIterator& iterator) {
        RecordIndex = iterator.GetRecordIndex();
        KeyIndex = RemapKeys.size() ? RemapKeys[iterator.GetKeyIndex()] : iterator.GetKeyIndex();
        IsValidFlag = true;
        HasValueFlag = iterator.HasValue();
        Value = iterator.GetValue();
    }

    bool Initialize() {
        struct TVisitor {
        private:
            TGeneralIterator& Owner;
        public:
            TVisitor(TGeneralIterator& owner)
                : Owner(owner) {
            }
            bool operator()(TOthersData::TIterator& iterator) {
                Owner.IsColumnKeyFlag = false;
                if (iterator.IsValid()) {
                    Owner.InitFromIterator(iterator);
                } else {
                    Owner.IsValidFlag = false;
                }
                return Owner.IsValidFlag;
            }
            bool operator()(TColumnsData::TIterator& iterator) {
                Owner.IsColumnKeyFlag = true;
                if (iterator.IsValid()) {
                    Owner.InitFromIterator(iterator);
                } else {
                    Owner.IsValidFlag = false;
                }
                return Owner.IsValidFlag;
            }
        };
        return std::visit(TVisitor(*this), Iterator);
    }

public:
    TGeneralIterator(TColumnsData::TIterator&& iterator, const std::optional<ui32> remappedKey = {})
        : Iterator(iterator)
        , RemappedKey(remappedKey) {
        Initialize();
    }
    TGeneralIterator(TOthersData::TIterator&& iterator, const std::vector<ui32>& remapKeys = {})
        : Iterator(iterator)
        , RemapKeys(remapKeys) {
        Initialize();
    }
    bool IsColumnKey() const {
        return IsColumnKeyFlag;
    }

    bool SkipRecordTo(const ui32 recordIndex) {
        struct TVisitor {
        private:
            TGeneralIterator& Owner;
            const ui32 RecordIndex;
        public:
            TVisitor(TGeneralIterator& owner, const ui32 recordIndex)
                : Owner(owner)
                , RecordIndex(recordIndex)
            {
            }
            bool operator()(TOthersData::TIterator& iterator) {
                if (iterator.SkipRecordTo(RecordIndex)) {
                    Owner.InitFromIterator(iterator);
                } else {
                    Owner.IsValidFlag = false;
                }
                return Owner.IsValidFlag;
            }
            bool operator()(TColumnsData::TIterator& iterator) {
                if (iterator.SkipRecordTo(RecordIndex)) {
                    Owner.InitFromIterator(iterator);
                } else {
                    Owner.IsValidFlag = false;
                }
                return Owner.IsValidFlag;
            }
        };
        return std::visit(TVisitor(*this, recordIndex), Iterator);
    }

    bool Next() {
        struct TVisitor {
        private:
            TGeneralIterator& Owner;
        public:
            TVisitor(TGeneralIterator& owner)
                : Owner(owner)
            {

            }
            bool operator()(TOthersData::TIterator& iterator) {
                if (iterator.Next()) {
                    Owner.InitFromIterator(iterator);
                } else {
                    Owner.IsValidFlag = false;
                }
                return Owner.IsValidFlag;
            }
            bool operator()(TColumnsData::TIterator& iterator) {
                if (iterator.Next()) {
                    Owner.InitFromIterator(iterator);
                } else {
                    Owner.IsValidFlag = false;
                }
                return Owner.IsValidFlag;
            }
        };
        return std::visit(TVisitor(*this), Iterator);
    }
    bool IsValid() const {
        return IsValidFlag;
    }
    ui32 GetRecordIndex() const {
        AFL_VERIFY(IsValidFlag);
        return RecordIndex;
    }
    ui32 GetKeyIndex() const {
        AFL_VERIFY(IsValidFlag);
        return KeyIndex;
    }
    std::string_view GetValue() const {
        AFL_VERIFY(IsValidFlag);
        return Value;
    }
    bool HasValue() const {
        AFL_VERIFY(IsValidFlag);
        return HasValueFlag;
    }
    bool operator<(const TGeneralIterator& item) const {
        return std::tie(item.RecordIndex, item.KeyIndex) < std::tie(RecordIndex, KeyIndex);
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
        auto checkIterator = [](const TGeneralIterator* it) {
            return !it->IsValid();
        };
        SortedIterators.erase(std::remove_if(SortedIterators.begin(), SortedIterators.end(), checkIterator), SortedIterators.end());
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecord(const ui32 recordIndex, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        startRecordActor(recordIndex);
        for (ui32 iIter = 0; iIter < SortedIterators.size();) {
            auto& itColumn = *SortedIterators[iIter];
            AFL_VERIFY(recordIndex <= itColumn.GetRecordIndex());
            while (itColumn.GetRecordIndex() == recordIndex) {
                if (itColumn.HasValue()) {
                    kvActor(itColumn.GetKeyIndex(), itColumn.GetValue(), itColumn.IsColumnKey());
                }
                if (!itColumn.Next()) {
                    break;
                }
            }
            if (!itColumn.IsValid()) {
                std::swap(SortedIterators[iIter], SortedIterators[SortedIterators.size() - 1]);
                SortedIterators.pop_back();
            } else {
                AFL_VERIFY(recordIndex < itColumn.GetRecordIndex());
                ++iIter;
            }
        }
        finishRecordActor();
    }

    void SkipRecordTo(const ui32 recordIndex) {
        for (ui32 iIter = 0; iIter < SortedIterators.size();) {
            if (!SortedIterators[iIter]->SkipRecordTo(recordIndex)) {
                std::swap(SortedIterators[iIter], SortedIterators[SortedIterators.size() - 1]);
                SortedIterators.pop_back();
            } else {
                AFL_VERIFY(recordIndex <= SortedIterators[iIter]->GetRecordIndex());
                ++iIter;
            }
        }
    }
};

class TReadIteratorOrderedKeys {
private:
    TColumnsData ColumnsData;
    TOthersData OthersData;
    std::vector<TGeneralIterator> Iterators;
    std::vector<TGeneralIterator*> SortedIterators;
    class TKeyAddress {
    private:
        YDB_READONLY_DEF(std::string_view, Name);
        YDB_READONLY(ui32, OriginalIndex, 0);
        YDB_READONLY(bool, IsColumn, false);

    public:
        TKeyAddress(const std::string_view& keyName, const ui32 keyIndex, const bool isColumn)
            : Name(keyName)
            , OriginalIndex(keyIndex)
            , IsColumn(isColumn) {
        }

        bool operator<(const TKeyAddress& item) const {
            return Name < item.Name;
        }
    };

    std::vector<TKeyAddress> Addresses;

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
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            Addresses.emplace_back(ColumnsData.GetStats().GetColumnName(i), i, true);
        }
        for (ui32 i = 0; i < OthersData.GetStats().GetColumnsCount(); ++i) {
            Addresses.emplace_back(OthersData.GetStats().GetColumnName(i), i, false);
        }
        std::sort(Addresses.begin(), Addresses.end());
        std::vector<ui32> remapColumns;
        remapColumns.resize(ColumnsData.GetStats().GetColumnsCount());
        std::vector<ui32> remapOthers;
        remapOthers.resize(OthersData.GetStats().GetColumnsCount());
        for (ui32 i = 0; i < Addresses.size(); ++i) {
            if (i) {
                AFL_VERIFY(Addresses[i].GetName() != Addresses[i - 1].GetName());
            }
            if (Addresses[i].GetIsColumn()) {
                remapColumns[Addresses[i].GetOriginalIndex()] = i;
            } else {
                remapOthers[Addresses[i].GetOriginalIndex()] = i;
            }
        }
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            Iterators.emplace_back(ColumnsData.BuildIterator(i), remapColumns[i]);
        }
        Iterators.emplace_back(OthersData.BuildIterator(), remapOthers);
        for (auto&& i : Iterators) {
            SortedIterators.emplace_back(&i);
        }
        auto checkIterator = [](const TGeneralIterator* it) {
            return !it->IsValid();
        };
        SortedIterators.erase(std::remove_if(SortedIterators.begin(), SortedIterators.end(), checkIterator), SortedIterators.end());
        std::make_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecord(const ui32 recordIndex, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        while (SortedIterators.size()) {
            while (SortedIterators.size() && SortedIterators.front()->GetRecordIndex() < recordIndex) {
                std::pop_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                auto& itColumn = *SortedIterators.back();
                if (!itColumn.Next()) {
                    SortedIterators.pop_back();
                } else {
                    std::push_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                }
                continue;
            }
            startRecordActor(recordIndex);
            while (SortedIterators.size() && SortedIterators.front()->GetRecordIndex() == recordIndex) {
                std::pop_heap(SortedIterators.begin(), SortedIterators.end(), TIteratorsComparator());
                auto& itColumn = *SortedIterators.back();
                kvActor(Addresses[itColumn.GetKeyIndex()].GetOriginalIndex(), itColumn.GetValue(), itColumn.IsColumnKey());
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
