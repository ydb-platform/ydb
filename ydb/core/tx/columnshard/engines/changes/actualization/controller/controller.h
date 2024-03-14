#pragma once
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NActualizer {

class TController {
private:
    THashMap<NActualizer::TRWAddress, i32> ActualizationsInProgress;
    ui32 GetLimitForAddress(const NActualizer::TRWAddress& address) const;

public:
    void StartActualization(const NActualizer::TRWAddress& address) {
        ++ActualizationsInProgress[address];
    }

    void FinishActualization(const NActualizer::TRWAddress& address) {
        AFL_VERIFY(--ActualizationsInProgress[address] >= 0);
    }

    bool IsNewTaskAvailable(const NActualizer::TRWAddress& address, const ui32 readyTemporaryTasks) const {
        auto it = ActualizationsInProgress.find(address);
        if (it == ActualizationsInProgress.end()) {
            return readyTemporaryTasks < GetLimitForAddress(address);
        } else {
            return readyTemporaryTasks < GetLimitForAddress(address) + it->second;
        }
    }
};

}