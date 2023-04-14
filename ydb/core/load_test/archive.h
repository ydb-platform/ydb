#pragma once

#include "aggregated_result.h"

namespace NKikimr {

static constexpr TStringBuf kTableCreatedResult = "Table created";
static constexpr TStringBuf kRecordsInsertedResult = "Records inserted";
static constexpr TStringBuf kRecordsSelectedResult = "Records selected";

TString MakeTableCreationYql();
TString MakeRecordInsertionYql(const TVector<TAggregatedResult>& items);
TString MakeRecordSelectionYql(ui32 offset, ui32 limit);

}  // namespace NKikimr
