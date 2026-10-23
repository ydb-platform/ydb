#include "schemeshard_replication_helpers.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

namespace {

bool ReplicationDestinationsOverlap(TStringBuf lhs, TStringBuf rhs) {
    return lhs == rhs
        || (lhs.size() > rhs.size() && lhs.StartsWith(rhs) && lhs[rhs.size()] == '/')
        || (rhs.size() > lhs.size() && rhs.StartsWith(lhs) && rhs[lhs.size()] == '/');
}

} // anonymous namespace

bool ValidateReplicationDestinations(TProposeResponse& result,
        const NKikimrReplication::TReplicationConfig& config, const TOperationContext& context,
        const TPathId& selfPathId)
{
    THashSet<TString> destinations;
    for (const auto& target : config.GetSpecific().GetTargets()) {
        const auto path = CanonizePath(target.GetDstPath());
        if (destinations.contains(path)) {
            result.SetError(NKikimrScheme::StatusInvalidParameter,
                TStringBuilder() << "Duplicate replication destination path '" << path << "'");
            return false;
        }

        // A directory is expanded into table targets by the controller later,
        // so reserve the whole subtree of each declared destination.
        for (const auto& destination : destinations) {
            if (ReplicationDestinationsOverlap(path, destination)) {
                result.SetError(NKikimrScheme::StatusInvalidParameter,
                    TStringBuilder() << "Overlapping replication destination paths '"
                        << path << "' and '" << destination << "'");
                return false;
            }
        }
        destinations.insert(path);
    }

    for (const auto& [pathId, replication] : context.SS->Replications) {
        if (pathId == selfPathId) {
            continue;
        }
        // CREATE stores its configuration in AlterData until it is committed.
        const auto& description = replication->AlterData
            ? replication->AlterData->Description : replication->Description;
        for (const auto& target : description.GetConfig().GetSpecific().GetTargets()) {
            const auto path = CanonizePath(target.GetDstPath());
            for (const auto& destination : destinations) {
                if (ReplicationDestinationsOverlap(path, destination)) {
                    result.SetError(NKikimrScheme::StatusPreconditionFailed,
                        TStringBuilder() << "Replication destination path '" << destination
                            << "' overlaps destination path '" << path << "' used by replication '"
                            << TPath::Init(pathId, context.SS).PathString() << "'");
                    return false;
                }
            }
        }
    }

    return true;
}

} // namespace NKikimr::NSchemeShard
