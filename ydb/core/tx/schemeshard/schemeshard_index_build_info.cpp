#include "schemeshard_index_build_info.h"

namespace NKikimr {

namespace NSchemeShard {

using namespace NKikimr::NSchemeShard;

TIndexBuildShardStatus::TIndexBuildShardStatus(TSerializedTableRange range, TString lastKeyAck)
    : Range(std::move(range))
    , LastKeyAck(std::move(lastKeyAck))
{}

void TIndexBuildInfo::SerializeToProto(TSchemeShard* ss, NKikimrSchemeOp::TIndexBuildConfig* result) const {
    Y_ENSURE(IsBuildIndex());
    result->SetTable(TPath::Init(TablePathId, ss).PathString());

    auto& index = *result->MutableIndex();
    index.SetName(IndexName);
    index.SetType(IndexType);

    *index.MutableKeyColumnNames() = {
        IndexColumns.begin(),
        IndexColumns.end()
    };

    *index.MutableDataColumnNames() = {
        DataColumns.begin(),
        DataColumns.end()
    };

    *index.MutableIndexImplTableDescriptions() = {
        ImplTableDescriptions.begin(),
        ImplTableDescriptions.end()
    };

    switch (IndexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            // no specialized index description
            Y_ASSERT(std::holds_alternative<std::monostate>(SpecializedIndexDescription));
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
            *index.MutableVectorIndexKmeansTreeDescription() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(SpecializedIndexDescription);
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltext:
            *index.MutableFulltextIndexDescription() = std::get<NKikimrSchemeOp::TFulltextIndexDescription>(SpecializedIndexDescription);
            break;
        default:
            Y_DEBUG_ABORT_S(InvalidIndexType(IndexType));
            break;
    }
}

void TIndexBuildInfo::SerializeToProto([[maybe_unused]] TSchemeShard* ss, NKikimrIndexBuilder::TColumnBuildSettings* result) const {
    Y_ENSURE(IsBuildColumns());
    Y_ASSERT(!TargetName.empty());
    result->SetTable(TargetName);
    for(const auto& column : BuildColumns) {
        column.SerializeToProto(result->add_column());
    }
}

ui64 TIndexBuildInfo::TKMeans::ParentEnd() const noexcept {  // included
    return ChildBegin - 1;
}
ui64 TIndexBuildInfo::TKMeans::ChildEnd() const noexcept {  // included
    return ChildBegin + ChildCount() - 1;
}

ui64 TIndexBuildInfo::TKMeans::ParentCount() const noexcept {
    return ParentEnd() - ParentBegin + 1;
}
ui64 TIndexBuildInfo::TKMeans::ChildCount() const noexcept {
    return ParentCount() * K;
}

TString TIndexBuildInfo::TKMeans::DebugString() const {
    return TStringBuilder()
        << "{ "
        << "State = " << State
        << ", Level = " << Level << " / " << Levels
        << ", K = " << K
        << ", Round = " << Round
        << (IsEmpty ? ", IsEmpty = true" : "")
        << ", Parent = [" << ParentBegin << ".." << Parent << ".." << ParentEnd() << "]"
        << ", Child = [" << ChildBegin << ".." << Child << ".." << ChildEnd() << "]"
        << ", TableSize = " << TableSize
        << " }";
}

bool TIndexBuildInfo::TKMeans::NeedsAnotherLevel() const noexcept {
    return Level < Levels;
}
bool TIndexBuildInfo::TKMeans::NeedsAnotherParent() const noexcept {
    return Parent < ParentEnd();
}

bool TIndexBuildInfo::TKMeans::NextParent() noexcept {
    if (!NeedsAnotherParent()) {
        return false;
    }
    ++Parent;
    Child += K;
    return true;
}

bool TIndexBuildInfo::TKMeans::NextLevel() noexcept {
    if (!NeedsAnotherLevel()) {
        return false;
    }
    NextLevel(ChildCount());
    return true;
}

void TIndexBuildInfo::TKMeans::PrefixIndexDone(ui64 shards) {
    Y_ENSURE(NeedsAnotherLevel());
    // There's two worst cases, but in both one shard contains TableSize rows
    // 1. all rows have unique prefix (*), in such case we need 1 id for each row (parent, id in prefix table)
    // 2. all unique prefixes have size K, so we have TableSize/K parents + TableSize childs
    // * it doesn't work now, because now prefix should have at least K embeddings, but it's bug
    NextLevel((2 * TableSize) * shards);
    Parent = ParentEnd();
}

void TIndexBuildInfo::TKMeans::Set(ui32 level,
    NTableIndex::NKMeans::TClusterId parentBegin, NTableIndex::NKMeans::TClusterId parent,
    NTableIndex::NKMeans::TClusterId childBegin, NTableIndex::NKMeans::TClusterId child,
    ui32 state, ui64 tableSize, ui32 round, bool isEmpty) {
    Level = level;
    Round = round;
    IsEmpty = isEmpty;
    ParentBegin = parentBegin;
    Parent = parent;
    ChildBegin = childBegin;
    Child = child;
    State = static_cast<EState>(state);
    TableSize = tableSize;
}

NKikimrTxDataShard::EKMeansState TIndexBuildInfo::TKMeans::GetUpload() const {
    if (State == Filter) {
        if (NeedsAnotherLevel()) {
            return NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD;
        } else {
            return NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING;
        }
    } else if (Level == 1) {
        if (NeedsAnotherLevel()) {
            return NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD;
        } else {
            return NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING;
        }
    } else {
        if (NeedsAnotherLevel()) {
            return NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD;
        } else {
            return NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING;
        }
    }
}

TString TIndexBuildInfo::TKMeans::WriteTo(bool needsBuildTable) const {
    using namespace NTableIndex::NKMeans;
    TString name = PostingTable;
    if (needsBuildTable || NeedsAnotherLevel() || OverlapClusters > 1 && Levels > 1 && State != Filter && State != FilterBorders) {
        name += NextBuildIndex() == 0 ? BuildSuffix0 : BuildSuffix1;
    }
    return name;
}

TString TIndexBuildInfo::TKMeans::ReadFrom() const {
    Y_ENSURE(Level > 1 || OverlapClusters > 1 && Levels > 1);
    using namespace NTableIndex::NKMeans;
    TString name = PostingTable;
    name += NextBuildIndex() == 0 ? BuildSuffix1 : BuildSuffix0;
    return name;
}

int TIndexBuildInfo::TKMeans::NextBuildIndex() const {
    if (OverlapClusters > 1 && Levels > 1) {
        if (IsPrefixed && Level == 1) {
            return 1;
        }
        return (State == Filter || State == FilterBorders ? 1 : 0);
    }
    return Level % 2 != 0 ? 0 : 1;
}

const char* TIndexBuildInfo::TKMeans::NextBuildSuffix() const {
    using namespace NTableIndex::NKMeans;
    return NextBuildIndex() == 1 ? BuildSuffix1 : BuildSuffix0;
}

std::pair<NTableIndex::NKMeans::TClusterId, NTableIndex::NKMeans::TClusterId> TIndexBuildInfo::TKMeans::RangeToBorders(const TSerializedTableRange& range) const {
    const NTableIndex::NKMeans::TClusterId minParent = ParentBegin;
    const NTableIndex::NKMeans::TClusterId maxParent = ParentEnd();
    const NTableIndex::NKMeans::TClusterId parentFrom = [&, from = range.From.GetCells()] {
        if (!from.empty()) {
            if (!from[0].IsNull()) {
                return from[0].AsValue<NTableIndex::NKMeans::TClusterId>() + static_cast<NTableIndex::NKMeans::TClusterId>(from.size() == 1);
            }
        }
        return minParent;
    }();
    const NTableIndex::NKMeans::TClusterId parentTo = [&, to = range.To.GetCells()] {
        if (!to.empty()) {
            if (!to[0].IsNull()) {
                return to[0].AsValue<NTableIndex::NKMeans::TClusterId>() - static_cast<NTableIndex::NKMeans::TClusterId>(to.size() != 1 && to[1].IsNull());
            }
        }
        return maxParent;
    }();
    Y_ENSURE(minParent <= parentFrom, "minParent(" << minParent << ") > parentFrom(" << parentFrom << ") " << DebugString());
    Y_ENSURE(parentFrom <= parentTo, "parentFrom(" << parentFrom << ") > parentTo(" << parentTo << ") " << DebugString());
    Y_ENSURE(parentTo <= maxParent, "parentTo(" << parentTo << ") > maxParent(" << maxParent << ") " << DebugString());
    return {parentFrom, parentTo};
}

TString TIndexBuildInfo::TKMeans::RangeToDebugStr(const TSerializedTableRange& range) const {
    auto toStr = [&](const TSerializedCellVec& v) -> TString {
        const auto cells = v.GetCells();
        if (cells.empty()) {
            return "inf";
        }
        if (cells[0].IsNull()) {
            return "-inf";
        }
        auto str = TStringBuilder{} << "{ count: " << cells.size();
        if (Level > 1) {
            str << ", parent: " << cells[0].AsValue<NTableIndex::NKMeans::TClusterId>();
            if (cells.size() != 1 && cells[1].IsNull()) {
                str << ", pk: null";
            }
        }
        return str << " }";
    };
    return TStringBuilder{} << "{ From: " << toStr(range.From) << ", To: " << toStr(range.To) << " }";
}

void TIndexBuildInfo::TKMeans::NextLevel(ui64 childCount) noexcept {
    ParentBegin = ChildBegin;
    Parent = ParentBegin;
    ChildBegin = ParentBegin + childCount;
    Child = ChildBegin;
    ++Level;
}

void TIndexBuildInfo::AddParent(const TSerializedTableRange& range, TShardIdx shard) {
    // For Parent == 0 only single kmeans needed, so there are two options:
    // 1. It fits entirely in the single shard => local kmeans for single shard
    // 2. It doesn't fit entirely in the single shard => global kmeans for all shards
    auto [parentFrom, parentTo] = KMeans.Parent == 0
        ? std::pair<NTableIndex::NKMeans::TClusterId, NTableIndex::NKMeans::TClusterId>{0, 0}
        : KMeans.RangeToBorders(range);

    auto itFrom = Cluster2Shards.lower_bound(parentFrom);
    if (itFrom == Cluster2Shards.end() || parentTo < itFrom->second.From) {
        // The new range does not intersect with other ranges, just add it with 1 shard
        Cluster2Shards.emplace_hint(itFrom, parentTo, TClusterShards{.From = parentFrom, .Shards = {shard}});
        return;
    }

    for (auto it = itFrom; it != Cluster2Shards.end() && it->second.From <= parentTo && it->first >= parentFrom; it++) {
        // The new shard may only intersect with existing shards by its starting or ending edge
        Y_ENSURE(it->second.From == parentTo || it->first == parentFrom);
    }

    if (parentFrom == itFrom->first) {
        // Intersects by parentFrom
        if (itFrom->second.From < itFrom->first) {
            Cluster2Shards.emplace_hint(itFrom, itFrom->first-1, itFrom->second);
            itFrom->second.From = parentFrom;
        }
        itFrom->second.Shards.push_back(shard);
        // Increment to also check intersection by parentTo
        itFrom++;
        if (parentTo == parentFrom) {
            return;
        }
        parentFrom++;
    }

    if (itFrom != Cluster2Shards.end() && parentTo == itFrom->second.From) {
        // Intersects by parentTo
        if (itFrom->second.From < itFrom->first) {
            auto endShards = itFrom->second.Shards;
            endShards.push_back(shard);
            Cluster2Shards.emplace_hint(itFrom, parentTo, TClusterShards{.From = parentTo, .Shards = std::move(endShards)});
            itFrom->second.From = parentTo+1;
        } else {
            itFrom->second.Shards.push_back(shard);
        }
        if (parentTo == parentFrom) {
            return;
        }
        parentTo--;
    }

    // Add the remaining range
    Cluster2Shards.emplace_hint(itFrom, parentTo, TClusterShards{.From = parentFrom, .Shards = {shard}});
}

bool TIndexBuildInfo::IsValidState(EState value)
{
    switch (value) {
        case EState::Invalid:
        case EState::AlterMainTable:
        case EState::Locking:
        case EState::GatheringStatistics:
        case EState::Initiating:
        case EState::Filling:
        case EState::DropBuild:
        case EState::CreateBuild:
        case EState::LockBuild:
        case EState::Applying:
        case EState::Unlocking:
        case EState::AlterSequence:
        case EState::Done:
        case EState::Cancellation_Applying:
        case EState::Cancellation_Unlocking:
        case EState::Cancellation_DroppingColumns:
        case EState::Cancelled:
        case EState::Rejection_Applying:
        case EState::Rejection_Unlocking:
        case EState::Rejection_DroppingColumns:
        case EState::Rejected:
            return true;
    }
    return false;
}

bool TIndexBuildInfo::IsValidSubState(ESubState value)
{
    switch (value) {
        case ESubState::None:
        case ESubState::UniqIndexValidation:
        case ESubState::FulltextIndexStats:
        case ESubState::FulltextIndexDictionary:
        case ESubState::FulltextIndexBorders:
            return true;
    }
    return false;
}

bool TIndexBuildInfo::IsValidBuildKind(EBuildKind value)
{
    switch (value) {
        case EBuildKind::BuildKindUnspecified:
        case EBuildKind::BuildSecondaryIndex:
        case EBuildKind::BuildVectorIndex:
        case EBuildKind::BuildPrefixedVectorIndex:
        case EBuildKind::BuildSecondaryUniqueIndex:
        case EBuildKind::BuildColumns:
        case EBuildKind::BuildFulltext:
            return true;
    }
    return false;
}

} // namespace NSchemeShard
} // namespace NKikimr
