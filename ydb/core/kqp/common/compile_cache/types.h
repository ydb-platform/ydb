#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/string.h>

#include <memory>
#include <unordered_map>

namespace NKikimr::NKqp {

struct TPreCboSnapshot {
    virtual ~TPreCboSnapshot() = default;
    virtual ui64 ByteSize() const = 0;

    std::unordered_map<TPathId, ui64> SchemaVersions;   // every table, index and view the snapshot is bound to
};

struct TPeepholeResult {
    virtual ~TPeepholeResult() = default;
    virtual ui64 ByteSize() const = 0;
};

using TPreCboSnapshotPtr = std::shared_ptr<const TPreCboSnapshot>;
using TPeepholeResultPtr = std::shared_ptr<const TPeepholeResult>;

// Placeholder: an opaque string built by the optimizer. TString because that is what the optimizer builds.
using TPeepholeKey = TString;

// Thread-safe. nullptr on miss; a returned result stays valid regardless of later eviction.
struct IPeepholeLookup {
    virtual ~IPeepholeLookup() = default;
    virtual TPeepholeResultPtr Find(const TPeepholeKey& key) = 0;
};

using IPeepholeLookupPtr = std::shared_ptr<IPeepholeLookup>;

struct TOptimizerInput {
    TPreCboSnapshotPtr CacheSnapshot;    // nullptr: no snapshot for this query
    IPeepholeLookupPtr PeepholeLookup;   // nullptr: always miss
};

struct TOptimizerCacheOutput {
    // Set: KQP keeps the snapshot instead of the compiled plan. Null: as today.
    TPreCboSnapshotPtr CacheSnapshot;
    std::unordered_map<TPeepholeKey, TPeepholeResultPtr> NewPeephole;

    void Clear() {
        CacheSnapshot.reset();
        NewPeephole.clear();
    }
};

} // namespace NKikimr::NKqp
