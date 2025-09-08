#pragma once

#include <util/generic/string.h>

#include <expected>
#include <map>

namespace NKikimr::NPQ {

// The GetNextUnreservedId method returns an ascending sequence of partition indexes starting with the first available one.
// The GetNextReservedId method always returns the same index that was requested.
// This class ensures that an arbitrary sequence of calls to GetNextUnreservedId and GetNextReservedId does not lead to duplicate indices or gaps in them.
    class TPartitionIndexGenerator {
    public:
        using TErrorMessage = TString;
        using TPartitionId = ui32;

    public:
        explicit TPartitionIndexGenerator(TPartitionId nextId);
        ~TPartitionIndexGenerator();

        // Mark new partition `id` as derived from the `sourceId`. For example it may be produceb by the split event
        std::expected<void, TErrorMessage> ReservePartitionIndex(TPartitionId id, TPartitionId sourceId, bool allowToReuseExistingPartition);

        std::expected<TPartitionId, TErrorMessage> GetNextUnreservedId();
        std::expected<TPartitionId, TErrorMessage> GetNextReservedId(TPartitionId id);

        std::expected<void, TErrorMessage> ValidateAllocationSequence();

    private:
        struct TReservationInfo {
            TPartitionId PartitionId = 0;
            TPartitionId SourceId = 0;
            bool Existed = false;
        };

        enum class EAllocationType {
            Free,
            Reserved,
        };

        static TPartitionId Advance(TPartitionId& id);
        std::expected<TPartitionId, TErrorMessage> Allocate(TPartitionId id, EAllocationType type);

    private:
        const TPartitionId First;
        TPartitionId Next;
        std::map<TPartitionId, TReservationInfo> Reserved;
        std::map<TPartitionId, EAllocationType> Allocated;
    };
} // namespace NKikimr::NPQ
