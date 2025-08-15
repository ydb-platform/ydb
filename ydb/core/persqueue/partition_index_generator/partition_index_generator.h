#pragma once

#include <util/generic/string.h>

#include <expected>
#include <map>

namespace NKikimr::NPQ {

    class TPartitionIndexGenerator {
    public:
        using TErrorMessage = TString;

        struct TIdPair {
            ui32 Id;
            ui32 GroupId;
        };

    public:
        explicit TPartitionIndexGenerator(ui32 nextId, ui32 nextGroupId);
        ~TPartitionIndexGenerator();

        std::expected<void, TErrorMessage> ReservePartitionIndex(ui32 id, ui32 sourceId, bool allowToReuseExistingPartition);
        std::expected<TIdPair, TErrorMessage> GetNextUnreservedIdAndGroupId();
        std::expected<TIdPair, TErrorMessage> GetNextReservedIdAndGroupId(ui32 id);
        std::expected<void, TErrorMessage> ValidateAllocationSequence();

    private:
        struct TReservationInfo {
            TIdPair Partition;
            ui32 SourceId = 0;
            bool Existed = false;
        };

        enum class EAllocationType {
            Free,
            Reserved,
        };

        static TIdPair Advance(TIdPair& pair);
        std::expected<TIdPair, TErrorMessage> Allocate(TIdPair pair, EAllocationType type);
        ui32 GenerateGroupIdFor(ui32 id) const;

    private:
        const TIdPair First;
        TIdPair Next;
        std::map<ui32, TReservationInfo> Reserved;
        std::map<ui32, EAllocationType> Allocated;
    };
} // namespace NKikimr::NPQ
