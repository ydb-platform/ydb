#pragma once

#include "mon_model.h"

#include <util/generic/map.h>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void AddScript(IOutputStream& str, TStringBuf resourceName);
void AddStyle(IOutputStream& str, TStringBuf resourceName);
TString HtmlEscape(TStringBuf in);
const char* PageParam(EMonPage page);
const char* PageTitle(EMonPage page);
TString MakeDDiskMonPageUrl(const NKikimr::NBsController::TDDiskId& ddiskId);
void RenderDDiskLink(
    IOutputStream& str,
    const NKikimr::NBsController::TDDiskId& ddiskId);
void RenderPBufferLink(
    IOutputStream& str,
    const NKikimr::NBsController::TDDiskId& pbufferId);
TString HealthRollup(const TMap<EHostHealth, size_t>& counts);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
