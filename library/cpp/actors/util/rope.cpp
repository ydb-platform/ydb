#include "rope.h"
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

size_t TRope::GetOccupiedMemorySize() const {
    size_t res = 0;
    absl::flat_hash_set<const void*> chunks;
    for (const auto& chunk : Chain) {
        if (const auto [it, inserted] = chunks.insert(chunk.Backend.UniqueId()); inserted) {
            res += chunk.Backend.GetOccupiedMemorySize();
        }
    }
    return res;
}
