#ifndef LINEAR_PROBE_INL_H_
#error "Direct inclusion of this file is not allowed, include linear_probe.h"
// For the sake of sane code completion.
#include "linear_probe.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t N>
void TLinearProbeHashTable::Find(TFingerprint fingerprint, TCompactVector<TValue, N>* result) const
{
    DoFind(IndexFromFingerprint(fingerprint), StampFromFingerprint(fingerprint), result);
}

template <size_t N>
void TLinearProbeHashTable::DoFind(ui64 index, TStamp stamp, TCompactVector<TValue, N>* result) const
{
    YT_ASSERT(stamp != 0);

    ui64 wrappedIndex = index % HashTable_.size();
    for (int currentIndex = 0; currentIndex < std::ssize(HashTable_); ++currentIndex) {
        auto tableEntry = HashTable_[wrappedIndex].load(std::memory_order::relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            break;
        }
        if (tableStamp == stamp) {
            result->push_back(ValueFromEntry(tableEntry));
        }

        ++wrappedIndex;
        if (wrappedIndex == HashTable_.size()) {
            wrappedIndex = 0;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
