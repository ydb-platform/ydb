#include "schemeshard_types.h"

namespace NKikimr::NSchemeShard {

TSchemeLimits::TSchemeLimits(const NKikimrScheme::TSchemeLimits& proto) {
    if (proto.HasMaxDepth()) {
        MaxDepth = proto.GetMaxDepth();
    }
    if (proto.HasMaxPaths()) {
        MaxPaths = proto.GetMaxPaths();
    }
    if (proto.HasMaxChildrenInDir()) {
        MaxChildrenInDir = proto.GetMaxChildrenInDir();
    }
    if (proto.HasMaxAclBytesSize()) {
        MaxAclBytesSize = proto.GetMaxAclBytesSize();
    }
    if (proto.HasMaxTableColumns()) {
        MaxTableColumns = proto.GetMaxTableColumns();
    }
    if (proto.HasMaxTableColumnNameLength()) {
        MaxTableColumnNameLength = proto.GetMaxTableColumnNameLength();
    }
    if (proto.HasMaxTableKeyColumns()) {
        MaxTableKeyColumns = proto.GetMaxTableKeyColumns();
    }
    if (proto.HasMaxTableIndices()) {
        MaxTableIndices = proto.GetMaxTableIndices();
    }
    if (proto.HasMaxTableCdcStreams()) {
        MaxTableCdcStreams = proto.GetMaxTableCdcStreams();
    }
    if (proto.HasMaxShards()) {
        MaxShards = proto.GetMaxShards();
    }
    if (proto.HasMaxShardsInPath()) {
        MaxShardsInPath = proto.GetMaxShardsInPath();
    }
    if (proto.HasMaxConsistentCopyTargets()) {
        MaxConsistentCopyTargets = proto.GetMaxConsistentCopyTargets();
    }
    if (proto.HasMaxPathElementLength()) {
        MaxPathElementLength = proto.GetMaxPathElementLength();
    }
    if (proto.HasExtraPathSymbolsAllowed()) {
        ExtraPathSymbolsAllowed = proto.GetExtraPathSymbolsAllowed();
    }
    if (proto.HasMaxPQPartitions()) {
        MaxPQPartitions = proto.GetMaxPQPartitions();
    }
}

NKikimrScheme::TSchemeLimits TSchemeLimits::AsProto() const {
    NKikimrScheme::TSchemeLimits result;

    result.SetMaxDepth(MaxDepth);
    result.SetMaxPaths(MaxPaths);
    result.SetMaxChildrenInDir(MaxChildrenInDir);
    result.SetMaxAclBytesSize(MaxAclBytesSize);

    result.SetMaxTableColumns(MaxTableColumns);
    result.SetMaxTableColumnNameLength(MaxTableColumnNameLength);
    result.SetMaxTableKeyColumns(MaxTableKeyColumns);
    result.SetMaxTableIndices(MaxTableIndices);
    result.SetMaxTableCdcStreams(MaxTableCdcStreams);
    result.SetMaxShards(MaxShards);
    result.SetMaxShardsInPath(MaxShardsInPath);
    result.SetMaxConsistentCopyTargets(MaxConsistentCopyTargets);

    result.SetMaxPathElementLength(MaxPathElementLength);
    result.SetExtraPathSymbolsAllowed(ExtraPathSymbolsAllowed);

    result.SetMaxPQPartitions(MaxPQPartitions);

    return result;
}

}
