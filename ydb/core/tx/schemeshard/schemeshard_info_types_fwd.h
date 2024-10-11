#pragma once

#include <util/generic/ptr.h>

// Forward declarations for pointer types to schemeshard's info types.
// Mainly for use by schemeshard__operation_{memory,db}_changes.h.

namespace NKikimr::NSchemeShard {

struct TPathElement;
using TPathElementPtr = TIntrusivePtr<TPathElement>;

struct TTableIndexInfo;
using TTableIndexInfoPtr = TIntrusivePtr<TTableIndexInfo>;

struct TCdcStreamInfo;
using TCdcStreamInfoPtr = TIntrusivePtr<TCdcStreamInfo>;

struct TTopicInfo;
using TTopicInfoPtr = TIntrusivePtr<TTopicInfo>;

struct TTableInfo;
using TTableInfoPtr = TIntrusivePtr<TTableInfo>;

struct TSequenceInfo;
using TSequenceInfoPtr = TIntrusivePtr<TSequenceInfo>;

struct TShardInfo;

struct TSubDomainInfo;
using TSubDomainInfoPtr = TIntrusivePtr<TSubDomainInfo>;

struct TTxState;

struct TExternalTableInfo;
using TExternalTableInfoPtr = TIntrusivePtr<TExternalTableInfo>;

struct TExternalDataSourceInfo;
using TExternalDataSourceInfoPtr = TIntrusivePtr<TExternalDataSourceInfo>;

struct TViewInfo;
using TViewInfoPtr = TIntrusivePtr<TViewInfo>;

struct TResourcePoolInfo;
using TResourcePoolInfoPtr = TIntrusivePtr<TResourcePoolInfo>;

struct TBackupCollectionInfo;
using TBackupCollectionInfoPtr = TIntrusivePtr<TBackupCollectionInfo>;

}  // namespace NKikimr::NSchemeShard
