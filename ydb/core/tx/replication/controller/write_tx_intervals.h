#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/hash.h>

#include <optional>

namespace NKikimr::NReplication::NController {

class TWriteTxIntervals {
public:
    void Start(ui64 writeTxId, const TRowVersion& intervalEnd);
    bool AddInterval(ui64 writeTxId, const TRowVersion& intervalEnd);
    bool CanReuse(ui64 writeTxId, const TRowVersion& intervalEnd) const;
    std::optional<TRowVersion> FindFirstIntervalEnd(ui64 writeTxId) const;
    void Complete(ui64 writeTxId);
    void Clear();

private:
    THashMap<ui64, TRowVersion> FirstIntervalEnds;
};

}
