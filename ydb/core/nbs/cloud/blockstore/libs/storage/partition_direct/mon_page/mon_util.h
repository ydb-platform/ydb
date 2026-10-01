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
const char* DDiskBalanceStrategyParam(EDDiskBalanceStrategy strategy);
EDDiskBalanceStrategy ParseDDiskBalanceStrategy(TStringBuf value);
// Formats the number and percentage of DDisks that need to move.
TString FormatDDiskImbalance(const TDDiskImbalance& imbalance);
// Renders a POST button that balances the half-open DBG range [from, to).
void RenderBalanceDDisksButton(
    IOutputStream& str,
    ui64 tabletId,
    EMonPage page,
    size_t from,
    size_t to,
    EDDiskBalanceStrategy strategy);
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
