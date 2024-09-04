#include "schemeshard_types.h"

namespace NKikimr::NSchemeShard {

TSchemeLimits TSchemeLimits::FromProto(const NKikimrScheme::TSchemeLimits& proto) {
    TSchemeLimits result;

    if (proto.HasMaxDepth()) {
        result.MaxDepth = proto.GetMaxDepth();
    }
    if (proto.HasMaxPaths()) {
        result.MaxPaths = proto.GetMaxPaths();
    }
    if (proto.HasMaxChildrenInDir()) {
        result.MaxChildrenInDir = proto.GetMaxChildrenInDir();
    }
    if (proto.HasMaxAclBytesSize()) {
        result.MaxAclBytesSize = proto.GetMaxAclBytesSize();
    }
    if (proto.HasMaxTableColumns()) {
        result.MaxTableColumns = proto.GetMaxTableColumns();
    }
    if (proto.HasMaxColumnTableColumns()) {
        result.MaxColumnTableColumns = proto.GetMaxColumnTableColumns();
    }
    if (proto.HasMaxTableColumnNameLength()) {
        result.MaxTableColumnNameLength = proto.GetMaxTableColumnNameLength();
    }
    if (proto.HasMaxTableKeyColumns()) {
        result.MaxTableKeyColumns = proto.GetMaxTableKeyColumns();
    }
    if (proto.HasMaxTableIndices()) {
        result.MaxTableIndices = proto.GetMaxTableIndices();
    }
    if (proto.HasMaxTableCdcStreams()) {
        result.MaxTableCdcStreams = proto.GetMaxTableCdcStreams();
    }
    if (proto.HasMaxShards()) {
        result.MaxShards = proto.GetMaxShards();
    }
    if (proto.HasMaxShardsInPath()) {
        result.MaxShardsInPath = proto.GetMaxShardsInPath();
    }
    if (proto.HasMaxConsistentCopyTargets()) {
        result.MaxConsistentCopyTargets = proto.GetMaxConsistentCopyTargets();
    }
    if (proto.HasMaxPathElementLength()) {
        result.MaxPathElementLength = proto.GetMaxPathElementLength();
    }
    if (proto.HasExtraPathSymbolsAllowed()) {
        result.ExtraPathSymbolsAllowed = proto.GetExtraPathSymbolsAllowed();
    }
    if (proto.HasMaxPQPartitions()) {
        result.MaxPQPartitions = proto.GetMaxPQPartitions();
    }
    if (proto.HasMaxExports()) {
        result.MaxExports = proto.GetMaxExports();
    }
    if (proto.HasMaxImports()) {
        result.MaxImports = proto.GetMaxImports();
    }

    return result;
}

NKikimrScheme::TSchemeLimits TSchemeLimits::AsProto() const {
    NKikimrScheme::TSchemeLimits result;

    result.SetMaxDepth(MaxDepth);
    result.SetMaxPaths(MaxPaths);
    result.SetMaxChildrenInDir(MaxChildrenInDir);
    result.SetMaxAclBytesSize(MaxAclBytesSize);

    result.SetMaxTableColumns(MaxTableColumns);
    result.SetMaxColumnTableColumns(MaxColumnTableColumns);
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

    result.SetMaxExports(MaxExports);
    result.SetMaxImports(MaxImports);

    return result;
}

}
