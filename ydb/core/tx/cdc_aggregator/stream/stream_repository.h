#pragma once

#include "stream.h"

namespace NKikimr::NCdcAggregator {

// Durable storage of the stream. Implemented over the tablet's LocalDB.
class IStreamRepository {
public:
    virtual ~IStreamRepository() = default;

    // Persists the parts of the stream listed in changes as part of the
    // current transaction.
    virtual void Save(const TStream& stream, const TStreamChanges& changes) = 0;
};

} // namespace NKikimr::NCdcAggregator
