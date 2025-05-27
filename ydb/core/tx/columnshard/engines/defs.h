#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/ctor_logger.h>

namespace NKikimr::NOlap {

using TLogThis = TCtorLogger<NKikimrServices::TX_COLUMNSHARD>;

enum class TOperationWriteId : ui64 {
};
enum class TInsertWriteId : ui64 {
};

inline TOperationWriteId operator++(TOperationWriteId& w) noexcept {
    w = TOperationWriteId{ ui64(w) + 1 };
    return w;
}

inline TInsertWriteId operator++(TInsertWriteId& w) noexcept {
    w = TInsertWriteId{ ui64(w) + 1 };
    return w;
}

}   // namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NOlap::TInsertWriteId> {
    inline size_t operator()(const NKikimr::NOlap::TInsertWriteId x) const noexcept {
        return THash<ui64>()(ui64(x));
    }
};

template <>
struct THash<NKikimr::NOlap::TOperationWriteId> {
    inline size_t operator()(const NKikimr::NOlap::TOperationWriteId x) const noexcept {
        return THash<ui64>()(ui64(x));
    }
};
