#pragma once

#include <util/generic/string.h>

namespace NKikimrSchemeOp {
class TBackupTask;
} // namespace NKikimrSchemeOp

namespace NKikimr::NBackup::NFieldsWrappers {

template <typename TSettings>
TSettings GetSettings(const NKikimrSchemeOp::TBackupTask& task);

template <typename TSettings>
TString GetCommonDestination(const TSettings&);

template <typename TItem>
TString& GetMutableItemDestination(TItem&);

template <typename TItem>
TString GetItemDestination(const TItem&);

} // NKikimr::NBackup
