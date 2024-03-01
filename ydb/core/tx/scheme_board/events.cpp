#include <util/string/join.h>

#include "helpers.h"
#include "opaque_path_description.h"
#include "events.h"


namespace NKikimr {

using namespace NSchemeBoard;

namespace {

//NOTE: MakeOpaquePathDescription moves out DescribeSchemeResultSerialized from TEvUpdate message.
// It cannot be used twice on the same message.
//
// This code would be much simpler without backward compatibility support.
// Consider removing compatibility support at version stable-25-1.
TOpaquePathDescription MakeOpaquePathDescription(NKikimrSchemeBoard::TEvUpdate& update) {
    // PathSubdomainPathId's absence is a marker that input message was sent
    // from the older populator implementation

    // Move out elements out of the repeated field.
    // Mark field as empty to prevent subsequent extraction attempts
    TString data;
    auto* field = update.MutableDescribeSchemeResultSerialized();
    if (field->size() == 1) {
        data = std::move(*field->begin());
    } else {
        data = JoinRange("", field->begin(), field->end());
    }
    field->Clear();

    if (update.HasPathSubdomainPathId()) {
        return TOpaquePathDescription{
            .DescribeSchemeResultSerialized = std::move(data),
            //NOTE: unsuccessful describe results cannot be here, by design
            .Status = NKikimrScheme::StatusSuccess,
            .PathId = TPathId(update.GetPathOwnerId(), update.GetLocalPathId()),
            .Path = update.GetPath(),
            .PathVersion = update.GetPathDirEntryPathVersion(),
            .SubdomainPathId = PathIdFromPathId(update.GetPathSubdomainPathId()),
            .PathAbandonedTenantsSchemeShards = TSet<ui64>(
                update.GetPathAbandonedTenantsSchemeShards().begin(),
                update.GetPathAbandonedTenantsSchemeShards().end()
            )
        };

    } else {
        google::protobuf::Arena arena;
        const auto* proto = DeserializeDescribeSchemeResult(data, &arena);

        return TOpaquePathDescription{
            .DescribeSchemeResultSerialized = std::move(data),
            //NOTE: unsuccessful describe results cannot be here, by design
            .Status = NKikimrScheme::StatusSuccess,
            .PathId = NSchemeBoard::GetPathId(*proto),
            .Path = proto->GetPath(),
            .PathVersion = NSchemeBoard::GetPathVersion(*proto),
            .SubdomainPathId = NSchemeBoard::GetDomainId(*proto),
            .PathAbandonedTenantsSchemeShards = NSchemeBoard::GetAbandonedSchemeShardIds(*proto)
        };
    }
}

}  // anonymous namespace

// TSchemeBoardEvents::TEvUpdate
//

TOpaquePathDescription TSchemeBoardEvents::TEvUpdate::ExtractPathDescription() {
    return MakeOpaquePathDescription(Record);
}

// TSchemeBoardEvents::TEvUpdateBuilder
//

TSchemeBoardEvents::TEvUpdateBuilder::TEvUpdateBuilder(const ui64 owner, const ui64 generation) {
    Record.SetOwner(owner);
    Record.SetGeneration(generation);
}

TSchemeBoardEvents::TEvUpdateBuilder::TEvUpdateBuilder(const ui64 owner, const ui64 generation, const TPathId& pathId) {
    Record.SetOwner(owner);
    Record.SetGeneration(generation);
    Record.SetPathOwnerId(pathId.OwnerId);
    Record.SetLocalPathId(pathId.LocalPathId);
    Record.SetIsDeletion(true);
}

TSchemeBoardEvents::TEvUpdateBuilder::TEvUpdateBuilder(
    const ui64 owner,
    const ui64 generation,
    const TOpaquePathDescription& pathDescription,
    const bool isDeletion
) {
    Record.SetOwner(owner);
    Record.SetGeneration(generation);
    Record.SetIsDeletion(isDeletion);

    Record.SetPath(pathDescription.Path);
    Record.SetPathOwnerId(pathDescription.PathId.OwnerId);
    Record.SetLocalPathId(pathDescription.PathId.LocalPathId);

    Record.SetPathDirEntryPathVersion(pathDescription.PathVersion);
    PathIdFromPathId(pathDescription.SubdomainPathId, Record.MutablePathSubdomainPathId());

    Record.MutablePathAbandonedTenantsSchemeShards()->Assign(
        pathDescription.PathAbandonedTenantsSchemeShards.begin(),
        pathDescription.PathAbandonedTenantsSchemeShards.end()
    );
}

void TSchemeBoardEvents::TEvUpdateBuilder::SetDescribeSchemeResultSerialized(const TString& serialized) {
    Record.AddDescribeSchemeResultSerialized(serialized);
}

void TSchemeBoardEvents::TEvUpdateBuilder::SetDescribeSchemeResultSerialized(TString&& serialized) {
    Record.AddDescribeSchemeResultSerialized(std::move(serialized));
}

// TSchemeBoardEvents::TEvNotifyBuilder
//

TSchemeBoardEvents::TEvNotifyBuilder::TEvNotifyBuilder(const TString& path, const bool isDeletion /*= false*/) {
    Record.SetPath(path);
    Record.SetIsDeletion(isDeletion);
}

TSchemeBoardEvents::TEvNotifyBuilder::TEvNotifyBuilder(const TPathId& pathId, const bool isDeletion /*= false*/) {
    Record.SetPathOwnerId(pathId.OwnerId);
    Record.SetLocalPathId(pathId.LocalPathId);
    Record.SetIsDeletion(isDeletion);
}

TSchemeBoardEvents::TEvNotifyBuilder::TEvNotifyBuilder(const TString& path, const TPathId& pathId, const bool isDeletion /*= false*/) {
    Record.SetPath(path);
    Record.SetPathOwnerId(pathId.OwnerId);
    Record.SetLocalPathId(pathId.LocalPathId);
    Record.SetIsDeletion(isDeletion);
}

void TSchemeBoardEvents::TEvNotifyBuilder::SetPathDescription(const TOpaquePathDescription& pathDescription) {
    Record.SetDescribeSchemeResultSerialized(pathDescription.DescribeSchemeResultSerialized);
    PathIdFromPathId(pathDescription.SubdomainPathId, Record.MutablePathSubdomainPathId());
    Record.MutablePathAbandonedTenantsSchemeShards()->Assign(
        pathDescription.PathAbandonedTenantsSchemeShards.begin(),
        pathDescription.PathAbandonedTenantsSchemeShards.end()
    );
}

} // NKikimr
