#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/util/yverify_stream.h>
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/formats/program.h>
#include <ydb/core/formats/replace_key.h>
#include <ydb/core/formats/switch_type.h>
#include <ydb/core/tx/ctor_logger.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <util/generic/set.h>

namespace NKikimr::NOlap {

using TLogThis = TCtorLogger<NKikimrServices::TX_COLUMNSHARD>;

enum class TWriteId : ui64 {};

inline TWriteId operator ++(TWriteId& w) { w = TWriteId{ui64(w) + 1}; return w; }

struct TSnapshot {
    ui64 PlanStep{0};
    ui64 TxId{0};

    bool IsZero() const {
        return PlanStep == 0 && TxId == 0;
    }

    bool Valid() const {
        return PlanStep && TxId;
    }

    bool operator == (const TSnapshot& snap) const {
        return (PlanStep == snap.PlanStep) && (TxId == snap.TxId);
    }

    bool operator < (const TSnapshot& snap) const {
        return (PlanStep < snap.PlanStep) || (PlanStep == snap.PlanStep && TxId < snap.TxId);
    }

    bool operator <= (const TSnapshot& snap) const {
        return (PlanStep < snap.PlanStep) || (PlanStep == snap.PlanStep && TxId <= snap.TxId);
    }

    static TSnapshot Max() {
        return TSnapshot{(ui64)-1ll, (ui64)-1ll};
    }

    friend IOutputStream& operator << (IOutputStream& out, const TSnapshot& s) {
        return out << "{" << s.PlanStep << "," << s.TxId << "}";
    }
};

inline bool snapLess(ui64 planStep1, ui64 txId1, ui64 planStep2, ui64 txId2) {
    return TSnapshot{planStep1, txId1} < TSnapshot{planStep2, txId2};
}

inline bool snapLessOrEqual(ui64 planStep1, ui64 txId1, ui64 planStep2, ui64 txId2) {
    return TSnapshot{planStep1, txId1} <= TSnapshot{planStep2, txId2};
}


class IBlobGroupSelector {
protected:
    virtual ~IBlobGroupSelector() = default;
public:
    virtual ui32 GetGroup(const TLogoBlobID& blobId) const = 0;
};

}

template<>
struct THash<NKikimr::NOlap::TWriteId> {
    inline size_t operator()(const NKikimr::NOlap::TWriteId x) const noexcept {
        return THash<ui64>()(ui64(x));
    }
};

