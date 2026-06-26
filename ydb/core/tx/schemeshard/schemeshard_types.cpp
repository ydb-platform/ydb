#include "schemeshard_types.h"

#include <util/string/cast.h>
#include <util/system/env.h>

namespace NKikimr::NSchemeShard {

// TODO(YDBAPPTEAM-773): revert this override after the ticket is closed.
namespace {
    ui64 EnvUi64(const char* name, ui64 def) {
        ui64 v = 0;
        return TryFromString<ui64>(GetEnv(name), v) ? v : def;
    }
}

ui64 SchemeLimitsDefaultMaxObjectsInBackup() {
    static const ui64 value = EnvUi64("YDB_TEST_SCHEMESHARD_MAX_OBJECTS_IN_BACKUP", 10*1000);
    return value;
}

ui64 SchemeLimitsDefaultMaxTableColumns() {
    static const ui64 value = EnvUi64("YDB_TEST_SCHEMESHARD_MAX_TABLE_COLUMNS", 200);
    return value;
}

ui64 SchemeLimitsDefaultMaxTableIndices() {
    static const ui64 value = EnvUi64("YDB_TEST_SCHEMESHARD_MAX_TABLE_INDICES", 20);
    return value;
}


void TSchemeLimits::MergeFromProto(const NKikimrSubDomains::TSchemeLimits& proto) {
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
    if (proto.HasMaxColumnTableColumns()) {
        MaxColumnTableColumns = proto.GetMaxColumnTableColumns();
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
    if (proto.HasMaxExports()) {
        MaxExports = proto.GetMaxExports();
    }
    if (proto.HasMaxImports()) {
        MaxImports = proto.GetMaxImports();
    }
}

TSchemeLimits TSchemeLimits::FromProto(const NKikimrSubDomains::TSchemeLimits& proto) {
    TSchemeLimits result;
    result.MergeFromProto(proto);
    return result;
}

NKikimrSubDomains::TSchemeLimits TSchemeLimits::AsProto() const {
    NKikimrSubDomains::TSchemeLimits result;

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
