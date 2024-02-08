#pragma once
#include "defs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/shared_quota.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/base/resource_profile.h>
#include <functional>

namespace NKikimr {

class TTabletStorageInfo;

class TTabletSetupInfo : public TThrRefBase {
public:
    typedef std::function<IActor* (const TActorId &, TTabletStorageInfo*)> TTabletCreationFunc;
    using TPtr = TIntrusivePtr<TTabletSetupInfo>;

private:
    const TTabletCreationFunc Op;
    const TMailboxType::EType MailboxType;
    const TMailboxType::EType TabletMailboxType;
    const ui32 PoolId;
    const ui32 TabletPoolId;

public:
    TTabletSetupInfo(TTabletCreationFunc op, TMailboxType::EType mailboxType, ui32 poolId, TMailboxType::EType tabletMailboxType, ui32 tabletPoolId)
        : Op(op)
        , MailboxType(mailboxType)
        , TabletMailboxType(tabletMailboxType)
        , PoolId(poolId)
        , TabletPoolId(tabletPoolId)
    {}

    TActorId Apply(TTabletStorageInfo *info, TActorIdentity owner);
    TActorId Apply(TTabletStorageInfo *info, const TActorContext &ctx);
    TActorId Tablet(TTabletStorageInfo *info, const TActorId &launcher, const TActorContext &ctx,
                    ui32 suggestedGeneration, TResourceProfilesPtr profiles = nullptr,
                    TSharedQuotaPtr txCacheQuota = nullptr);
    TActorId Follower(TTabletStorageInfo *info, const TActorId &launcher, const TActorContext &ctx,
                   ui32 followerID, TResourceProfilesPtr profiles = nullptr,
                   TSharedQuotaPtr txCacheQuota = nullptr);
};

IActor* CreateTablet(const TActorId &launcher, TTabletStorageInfo *info, TTabletSetupInfo *setupInfo,
                     ui32 suggestedGeneration, TResourceProfilesPtr profiles = nullptr,
                     TSharedQuotaPtr txCacheQuota = nullptr);
IActor* CreateTabletFollower(const TActorId &launcher, TTabletStorageInfo *info, TTabletSetupInfo *setupInfo,
                          ui32 followerID, TResourceProfilesPtr profiles = nullptr,
                          TSharedQuotaPtr txCacheQuota = nullptr);


struct ITabletFactory: public virtual TThrRefBase {
    virtual TIntrusivePtr<TTabletSetupInfo> CreateTablet(
        const TString& typeName,
        const TIntrusivePtr<TTabletStorageInfo>& tabletInfo,
        const TAppData& appData) = 0;
};

}
