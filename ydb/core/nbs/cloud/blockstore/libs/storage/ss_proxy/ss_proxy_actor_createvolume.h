#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_label.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>

#include <ydb/core/base/path.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;

using TVolumeConfig = NKikimrBlockStore::TVolumeConfig;

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeActor final: public TActorBootstrapped<TCreateVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString& SchemeShardDir;
    const TVolumeConfig VolumeConfig;

    bool FirstCreationAttempt = true;

    TString VolumeDir;
    TString VolumeName;
    TVector<TString> VolumePathItems;
    TVector<TString> ParentDirs;
    size_t NextItemToCreate = 0;

public:
    TCreateVolumeActor(TRequestInfoPtr requestInfo,
                       const TString& schemeShardDir,
                       TVolumeConfig volumeConfig);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolumeBeforeCreate(const TActorContext& ctx);

    void CreateVolume(const TActorContext& ctx);

    void EnsureDirs(const TActorContext& ctx);

    void CreateNextDir(const TActorContext& ctx);

    void DescribeVolumeAfterCreate(const TActorContext& ctx);

    bool VerifyVolume(const TActorContext& ctx,
                      const NKikimrBlockStore::TVolumeConfig& actual);

    void HandleDescribeVolumeBeforeCreateResponse(
        const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateVolumeResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMkDirResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeAfterCreateResponse(
        const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvCreateVolumeResponse> response);

    STFUNC(StateDescribeVolumeBeforeCreate);
    STFUNC(StateCreateVolume);
    STFUNC(StateCreateNextDir);
    STFUNC(StateDescribeVolumeAfterCreate);
};

}   // namespace NYdb::NBS::NStorage
