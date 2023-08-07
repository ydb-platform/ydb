#include "linear_probe.h"

#include <util/system/types.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TLinearProbeHashTable::TLinearProbeHashTable(size_t maxElementCount)
    : HashTable_(maxElementCount * HashTableExpansionParameter)
{ }

bool TLinearProbeHashTable::Insert(TFingerprint fingerprint, TValue value)
{
    return DoInsert(IndexFromFingerprint(fingerprint), StampFromFingerprint(fingerprint), value);
}

bool TLinearProbeHashTable::DoInsert(ui64 index, TStamp stamp, TValue value)
{
    YT_VERIFY(stamp != 0);
    YT_VERIFY((value >> ValueLog) == 0);

    ui64 wrappedIndex = index % HashTable_.size();
    auto entry = MakeEntry(stamp, value);
    for (int currentIndex = 0; currentIndex < std::ssize(HashTable_); ++currentIndex) {
        auto tableEntry = HashTable_[wrappedIndex].load(std::memory_order::relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            auto success = HashTable_[wrappedIndex].compare_exchange_strong(
                tableEntry,
                entry,
                std::memory_order::release,
                std::memory_order::relaxed);
            if (success) {
                return true;
            }
        }

        ++wrappedIndex;
        if (wrappedIndex == HashTable_.size()) {
            wrappedIndex = 0;
        }
    }

    return false;
}

size_t TLinearProbeHashTable::GetByteSize() const
{
    return sizeof(std::atomic<TEntry>) * HashTable_.size();
}

TLinearProbeHashTable::TStamp TLinearProbeHashTable::StampFromEntry(TLinearProbeHashTable::TEntry entry)
{
    return entry >> ValueLog;
}

TLinearProbeHashTable::TValue TLinearProbeHashTable::ValueFromEntry(TLinearProbeHashTable::TEntry entry)
{
    return entry & ((1ULL << ValueLog) - 1);
}

TLinearProbeHashTable::TEntry TLinearProbeHashTable::MakeEntry(TLinearProbeHashTable::TStamp stamp, TLinearProbeHashTable::TValue value)
{
    return (static_cast<TLinearProbeHashTable::TEntry>(stamp) << ValueLog) | value;
}

ui64 TLinearProbeHashTable::IndexFromFingerprint(TFingerprint fingerprint)
{
    return fingerprint;
}

TLinearProbeHashTable::TStamp TLinearProbeHashTable::StampFromFingerprint(TFingerprint fingerprint)
{
    TLinearProbeHashTable::TStamp stamp = fingerprint;
    if (stamp == 0) {
        stamp = 1;
    }
    return stamp;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

