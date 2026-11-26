#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/hash.h>

namespace NKikimr::NReplication::NController {

class TWriteTxIntervals {
public:
    void Start(ui64 writeTxId, const TRowVersion& intervalEnd);
    void AddInterval(ui64 writeTxId, const TRowVersion& intervalEnd);
    bool CanReuse(ui64 writeTxId, const TRowVersion& intervalEnd) const;
    void Complete(ui64 writeTxId);
    void Clear();

private:
    THashMap<ui64, TRowVersion> FirstIntervalEnds;
};

}
