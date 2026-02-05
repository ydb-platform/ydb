#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NBackup::NFieldsWrappers {

template <typename TSettings>
TSettings GetSettings(const NKikimrSchemeOp::TBackupTask& task);

template <typename TSettings>
TString GetCommonDestination(const TSettings&);

template <typename TItem>
TString& GetMutableItemDestination(TItem&);

template <typename TItem>
TString GetItemDestination(const TItem&);

} // NKikimr::NBackup::NFieldsWrappers
