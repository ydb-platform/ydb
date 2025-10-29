#include "partition_index_generator.h"

#include <util/generic/mapfindptr.h>
#include <format>

namespace NKikimr::NPQ {

    TPartitionIndexGenerator::TPartitionIndexGenerator(TPartitionId nextId)
        : First{nextId}
        , Next(First)
    {
    }

    TPartitionIndexGenerator::~TPartitionIndexGenerator() = default;

    std::expected<void, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::ReservePartitionIndex(TPartitionId id, TPartitionId sourceId, bool allowToReuseExistingPartition) {
        if (id < First && !allowToReuseExistingPartition) {
            return std::unexpected(std::format("Attempt to reserve partition id ({}) that is less than the first availiable id ({})", id, First));
        }
        TReservationInfo res{
            .PartitionId = id,
            .SourceId = sourceId,
            .Existed = (id < First),
        };
        const auto [it, unique] = Reserved.try_emplace(id, std::move(res));
        if (!unique) {
            if (it->second.SourceId == sourceId) {
                return std::unexpected(std::format("Splitting parition id ({}) has repetition in the children partition ids ({})",
                                                   sourceId, id));
            } else {
                return std::unexpected(std::format("Attempt to reserve parition id ({}) for multiple split/merge operations ({}, {})",
                                                   id, it->second.SourceId, sourceId));
            }
        }
        return {};
    }

    std::expected<TPartitionIndexGenerator::TPartitionId, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::GetNextUnreservedId() {
        while (Reserved.contains(Next)) {
            Advance(Next);
        }
        return Allocate(Advance(Next), EAllocationType::Free);
    }

    std::expected<TPartitionIndexGenerator::TPartitionId, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::GetNextReservedId(TPartitionId id) {
        const TReservationInfo* res = MapFindPtr(Reserved, id);
        if (!res) {
            return std::unexpected(std::format("Partition id ({}) is not reserved", id));
        }
        return Allocate(res->PartitionId, EAllocationType::Reserved);
    }

    std::expected<void, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::ValidateAllocationSequence() {
        for (const auto& [id, res] : Reserved) {
            if (!Allocated.contains(id)) {
                return std::unexpected(std::format("Partition id ({}) is reserved but not allocated", id));
            }
        }
        TPartitionId id = First;
        for (const auto& [allocId, type] : Allocated) {
            const TReservationInfo* reserve = MapFindPtr(Reserved, allocId);
            if (!reserve) {
                if (type == EAllocationType::Reserved) {
                    return std::unexpected(std::format("Partition id ({}) is not reserved", id));
                }
            } else if (reserve->Existed) {
                continue;
            }
            if (id != allocId) {
                return std::unexpected(std::format("Gap in the partition indices: attempt to create new partition ({}) without creating a previous one ({})", allocId, id));
            }
            ++id;
        }
        return {};
    }

    TPartitionIndexGenerator::TPartitionId TPartitionIndexGenerator::Advance(TPartitionId& id) {
        return id++;
    }

    std::expected<TPartitionIndexGenerator::TPartitionId, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::Allocate(TPartitionId id, EAllocationType type) {
        auto [it, ins] = Allocated.try_emplace(id, type);
        if (!ins) {
            return std::unexpected(std::format("Attempt to allocate partition id ({}) that is already allocated", id));
        }
        return id;
    }

} // namespace NKikimr::NPQ
