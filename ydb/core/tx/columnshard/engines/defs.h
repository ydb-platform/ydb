#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/ctor_logger.h>

namespace NKikimr::NOlap {

using TLogThis = TCtorLogger<NKikimrServices::TX_COLUMNSHARD>;

enum class TWriteId : ui64 {};

inline TWriteId operator++(TWriteId& w) noexcept {
    w = TWriteId{ui64(w) + 1};
    return w;
}

} // namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NOlap::TWriteId> {
    inline size_t operator()(const NKikimr::NOlap::TWriteId x) const noexcept {
        return THash<ui64>()(ui64(x));
    }
};
