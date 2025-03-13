#pragma once
#include "constructor_accessor.h"

namespace NKikimr::NOlap {

class TPortionConstructors {
private:
    THashMap<NColumnShard::TInternalPathId, THashMap<ui64, TPortionAccessorConstructor>> Constructors;

public:
    THashMap<NColumnShard::TInternalPathId, THashMap<ui64, TPortionAccessorConstructor>>::iterator begin() {
        return Constructors.begin();
    }

    THashMap<NColumnShard::TInternalPathId, THashMap<ui64, TPortionAccessorConstructor>>::iterator end() {
        return Constructors.end();
    }

    TPortionAccessorConstructor* GetConstructorVerified(const NColumnShard::TInternalPathId pathId, const ui64 portionId) {
        auto itPathId = Constructors.find(pathId);
        AFL_VERIFY(itPathId != Constructors.end());
        auto itPortionId = itPathId->second.find(portionId);
        AFL_VERIFY(itPortionId != itPathId->second.end());
        return &itPortionId->second;
    }

    TPortionAccessorConstructor* AddConstructorVerified(TPortionAccessorConstructor&& constructor) {
        const NColumnShard::TInternalPathId pathId = constructor.GetPortionConstructor().GetPathId();
        const ui64 portionId = constructor.GetPortionConstructor().GetPortionIdVerified();
        auto info = Constructors[pathId].emplace(portionId, std::move(constructor));
        AFL_VERIFY(info.second);
        return &info.first->second;
    }
};

class TInGranuleConstructors {
private:
    THashMap<ui64, TPortionAccessorConstructor> Constructors;

public:
    THashMap<ui64, TPortionAccessorConstructor>::iterator begin() {
        return Constructors.begin();
    }

    THashMap<ui64, TPortionAccessorConstructor>::iterator end() {
        return Constructors.end();
    }

    void ClearPortions() {
        Constructors.clear();
    }

    void ClearColumns() {
        for (auto&& i : Constructors) {
            i.second.ClearRecords();
        }
    }

    void ClearIndexes() {
        for (auto&& i : Constructors) {
            i.second.ClearIndexes();
        }
    }

    TPortionAccessorConstructor* GetConstructorVerified(const ui64 portionId) {
        auto itPortionId = Constructors.find(portionId);
        AFL_VERIFY(itPortionId != Constructors.end());
        return &itPortionId->second;
    }

    TPortionAccessorConstructor* AddConstructorVerified(TPortionAccessorConstructor&& constructor) {
        const ui64 portionId = constructor.GetPortionConstructor().GetPortionIdVerified();
        auto info = Constructors.emplace(portionId, std::move(constructor));
        AFL_VERIFY(info.second);
        return &info.first->second;
    }

    TPortionAccessorConstructor* AddConstructorVerified(TPortionInfoConstructor&& constructor) {
        const ui64 portionId = constructor.GetPortionIdVerified();
        auto info = Constructors.emplace(portionId, TPortionAccessorConstructor(std::move(constructor)));
        AFL_VERIFY(info.second);
        return &info.first->second;
    }
};

}   // namespace NKikimr::NOlap
