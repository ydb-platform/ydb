#include "partition_index_generator.h"

#include <util/generic/mapfindptr.h>
#include <format>

namespace NKikimr::NPQ {

    TPartitionIndexGenerator::TPartitionIndexGenerator(ui32 nextId, ui32 nextGroupId)
        : First{
              .Id = nextId,
              .GroupId = nextGroupId,
          }
        , Next(First)
    {
    }

    TPartitionIndexGenerator::~TPartitionIndexGenerator() = default;

    std::expected<void, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::ReservePartitionIndex(ui32 id, ui32 sourceId, bool allowToReuseExistingPartition) {
        if (id < First.Id && !allowToReuseExistingPartition) {
            return std::unexpected(std::format("Attempt to reserve partition id ({}) that is less than the first availiable id ({})", id, First.Id));
        }
        TReservationInfo res{
            .Partition{
                .Id = id,
                .GroupId = GenerateGroupIdFor(id),
            },
            .SourceId = sourceId,
            .Existed = (id < First.Id),
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

    std::expected<TPartitionIndexGenerator::TIdPair, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::GetNextUnreservedIdAndGroupId() {
        while (Reserved.contains(Next.Id)) {
            Advance(Next);
        }
        return Allocate(Advance(Next), EAllocationType::Free);
    }

    std::expected<TPartitionIndexGenerator::TIdPair, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::GetNextReservedIdAndGroupId(ui32 id) {
        const TReservationInfo* res = MapFindPtr(Reserved, id);
        if (!res) {
            return std::unexpected(std::format("Partition id ({}) is not reserved", id));
        }
        return Allocate(res->Partition, EAllocationType::Reserved);
    }

    std::expected<void, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::ValidateAllocationSequence() {
        for (const auto& [id, res] : Reserved) {
            if (!Allocated.contains(id)) {
                return std::unexpected(std::format("Partition id ({}) is reserved but not allocated", id));
            }
        }
        ui32 id = First.Id;
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

    TPartitionIndexGenerator::TIdPair TPartitionIndexGenerator::Advance(TIdPair& pair) {
        TIdPair prev = pair;
        ++pair.Id;
        ++pair.GroupId;
        return prev;
    }

    std::expected<TPartitionIndexGenerator::TIdPair, TPartitionIndexGenerator::TErrorMessage> TPartitionIndexGenerator::Allocate(TIdPair pair, EAllocationType type) {
        auto [it, ins] = Allocated.try_emplace(pair.Id, type);
        if (!ins) {
            return std::unexpected(std::format("Attempt to allocate partition id ({}) that is already allocated", pair.Id));
        }
        return pair;
    }

    ui32 TPartitionIndexGenerator::GenerateGroupIdFor(ui32 id) const {
        ui32 offset = id - First.Id;
        ui32 g = First.GroupId + offset;
        return g;
    }

} // namespace NKikimr::NPQ
