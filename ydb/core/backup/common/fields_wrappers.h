#pragma once

#include <util/generic/string.h>

namespace NKikimrSchemeOp {
    class TBackupTask;
} // namespace NKikimrSchemeOp

namespace NKikimr::NBackup::NFieldsWrappers {

template <typename TSettings>
const TSettings& GetSettings(const NKikimrSchemeOp::TBackupTask& task);

template <typename TSettings>
const TString& GetCommonDestination(const TSettings&);

template <typename TItem>
TString& MutableItemDestination(TItem&);

template <typename TItem>
const TString& GetItemDestination(const TItem&);

} // NKikimr::NBackup
