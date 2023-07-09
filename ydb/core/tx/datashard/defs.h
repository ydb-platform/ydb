#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/tx/defs.h
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {

class TNoOpDestroy {
public:
    template <typename T>
    static inline void Destroy(const T &) noexcept {}
};

}
