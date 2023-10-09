#pragma once

#include <util/generic/hash.h>
#include <util/system/mutex.h>
#include <util/system/types.h>

namespace NYql::NDq {

using TAllocateCallback = std::function<void(const NDq::TTxId& txId, ui64 taskId, ui64 size, bool success)>;
using TNotifyCallback = std::function<void(const ui64 limit, ui64 allocated)>;

struct TTransactionTaskInfo {
    NDq::TTxId TxId;
    ui64 TaskId = 0;
    ui64 Size = 0;
};

class TResourceQuoter {

public:
    TResourceQuoter(ui64 limit, bool strict = false) : Limit(limit), Strict(strict) {
    }

    ui64 GetLimit() const {
        return Limit;
    }

    ui64 GetAllocatedTotal() const {
        TGuard<TMutex> lock(Mutex);
        return Allocated;
    }

    ui64 GetFreeTotal() const {
        TGuard<TMutex> lock(Mutex);
        return Limit > Allocated ? Limit - Allocated : 0;
    }

    bool Allocate(const NDq::TTxId& txId, ui64 taskId, ui64 size) {
        TGuard<TMutex> lock(Mutex);
        if (Limit && Allocated + size > Limit) {
            return false;
        }
        Allocated += size;

        auto& txMap = ResourceMap[txId];
        auto itt = txMap.find(taskId);
        if (itt == txMap.end()) {
            auto& info = txMap[taskId];
            info.TxId = txId;
            info.TaskId = taskId;
            info.Size = size;
        } else {
            itt->second.Size += size;
        }

        Notify();
        return true;
    }

    void Free(const NDq::TTxId& txId, ui64 taskId, ui64 size) {
        TGuard<TMutex> lock(Mutex);
        auto itx = ResourceMap.find(txId);

        if (Strict) {
            Y_ABORT_UNLESS(itx != ResourceMap.end());
        } else {
            if(itx == ResourceMap.end()) {
                return;
            }
        }

        auto itt = itx->second.find(taskId);
        Y_ABORT_UNLESS(itt != itx->second.end());

        if (Strict) {
            Y_ABORT_UNLESS(itt->second.Size >= size);
        } else {
            if (size > itt->second.Size) {
                size = itt->second.Size;
            }
        }

        Y_ABORT_UNLESS(Allocated >= size);

        itt->second.Size -= size;
        if (itt->second.Size == 0) {
            itx->second.erase(itt);
            if (itx->second.size() == 0) {
                ResourceMap.erase(itx);
            }
        }

        Allocated -= size;
        Notify();
    }

    ui64 GetAllocated(const NDq::TTxId& txId, ui64 taskId) const {
        TGuard<TMutex> lock(Mutex);
        auto itx = ResourceMap.find(txId);
        if (itx != ResourceMap.end()) {
            auto itt = itx->second.find(taskId);
            if (itt != itx->second.end()) {
                return itt->second.Size;
            }
        }
        return 0;
    }

    void Free(const NDq::TTxId& txId, ui64 taskId) {
        TGuard<TMutex> lock(Mutex);
        auto itx = ResourceMap.find(txId);
        if (itx != ResourceMap.end()) {
            auto itt = itx->second.find(taskId);
            if (itt != itx->second.end()) {
                Y_ABORT_UNLESS(Allocated >= itt->second.Size);
                Allocated -= itt->second.Size;
                itx->second.erase(itt);
                if (itx->second.size() == 0) {
                    ResourceMap.erase(itx);
                }
                Notify();
            }
        }
    }

    TNotifyCallback SetNotifier(const TNotifyCallback& newCb) {
        TNotifyCallback result = std::move(Cb);
        Cb = newCb;
        return result;
    }

private:
    ui64 Limit;
    bool Strict;
    ui64 Allocated = 0;
    TMutex Mutex;
    THashMap<NDq::TTxId, THashMap<ui64, TTransactionTaskInfo>> ResourceMap;
    TNotifyCallback Cb;

    void Notify() const {
        if (Cb) {
            Cb(Limit, Allocated);
        }
    }
};

} // namespace NYql::NDq
