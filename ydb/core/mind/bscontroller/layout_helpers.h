#pragma once

#include "defs.h"

#include "group_layout_checker.h"
#include "group_mapper.h"
#include "group_geometry_info.h"
#include "types.h"

namespace NKikimr {

namespace NBsController {

bool CheckLayoutByGroupDefinition(const TGroupMapper::TGroupDefinition& group,
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition>& pdisks, 
        const TGroupGeometryInfo& geom, bool allowMultipleRealmsOccupation,
        TString& error);

bool CheckBaseConfigLayout(const TGroupGeometryInfo& geom, const NKikimrBlobStorage::TBaseConfig& cfg,
        bool allowMultipleRealmsOccupation, TString& error);

TGroupGeometryInfo CreateGroupGeometry(TBlobStorageGroupType type, ui32 numFailRealms = 0, ui32 numFailDomains = 0,
        ui32 numVDisks = 0, ui32 realmBegin = 0, ui32 realmEnd = 0, ui32 domainBegin = 0, ui32 domainEnd = 0);

} // namespace NBsController

} // namespace NKikimr
