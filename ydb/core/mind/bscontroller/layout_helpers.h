#pragma once

#include "defs.h"

#include <ydb/core/mind/bscontroller/group_layout_checker.h>
#include <ydb/core/mind/bscontroller/group_mapper.h>
#include <ydb/core/mind/bscontroller/group_geometry_info.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NKikimr {

namespace NBsController {

bool CheckLayoutByGroupDefinition(const TGroupMapper::TGroupDefinition& group,
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition>& pdisks, 
        const TGroupGeometryInfo& geom, TString& error);

bool CheckBaseConfigLayout(const TGroupGeometryInfo& geom, const NKikimrBlobStorage::TBaseConfig& cfg, TString& error);

TGroupGeometryInfo CreateGroupGeometry(TBlobStorageGroupType type, ui32 numFailRealms = 0, ui32 numFailDomains = 0,
        ui32 numVDisks = 0, ui32 realmBegin = 0, ui32 realmEnd = 0, ui32 domainBegin = 0, ui32 domainEnd = 0);

} // namespace NBsController

} // namespace NKikimr
