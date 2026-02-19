#include "ss_proxy_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
// #include <ydb/core/nbs/cloud/blockstore/libs/storage/core/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_label.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_model.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>

#include <ydb/core/base/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;

using TVolumeConfig = NKikimrBlockStore::TVolumeConfig;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetBlocksCount(const TVolumeConfig& config)
{
    ui64 res = 0;
    for (const auto& partition: config.GetPartitions()) {
        res += partition.GetBlockCount();
    }
    return res;
}

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeActor final: public TActorBootstrapped<TCreateVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString SchemeShardDir;
    const TVolumeConfig VolumeConfig;

    bool FirstCreationAttempt = true;

    TString VolumeDir;
    TString VolumeName;
    TVector<TString> VolumePathItems;
    TVector<TString> ParentDirs;
    size_t NextItemToCreate = 0;

public:
    TCreateVolumeActor(TRequestInfoPtr requestInfo, TString schemeShardDir,
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

////////////////////////////////////////////////////////////////////////////////

TCreateVolumeActor::TCreateVolumeActor(TRequestInfoPtr requestInfo,
                                       TString schemeShardDir,
                                       TVolumeConfig volumeConfig)
    : RequestInfo(std::move(requestInfo))
    , SchemeShardDir(std::move(schemeShardDir))
    , VolumeConfig(std::move(volumeConfig))
{
    const auto& diskId = VolumeConfig.GetDiskId();
    Y_ABORT_UNLESS(diskId);

    std::tie(VolumeDir, VolumeName) =
        DiskIdToVolumeDirAndName(SchemeShardDir, diskId);
    VolumePathItems = SplitPath(DiskIdToPath(diskId));
    Y_ABORT_UNLESS(VolumeName);
    Y_ABORT_UNLESS(VolumePathItems);

    TString dir = SchemeShardDir;
    for (size_t i = 0; i + 1 < VolumePathItems.size(); ++i) {
        ParentDirs.push_back(dir);
        dir += "/" + VolumePathItems[i];
    }
}

void TCreateVolumeActor::Bootstrap(const TActorContext& ctx)
{
    // Try to describe volume first.
    DescribeVolumeBeforeCreate(ctx);
}

void TCreateVolumeActor::DescribeVolumeBeforeCreate(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolumeBeforeCreate);

    const auto& diskId = VolumeConfig.GetDiskId();

    TString volumeDir, volumeName;
    std::tie(volumeDir, volumeName) =
        DiskIdToVolumeDirAndName(SchemeShardDir, diskId);
    const auto volumePath = volumeDir + "/" + volumeName;

    LOG_DEBUG(
        ctx, NKikimrServices::NBS_SS_PROXY,
        "Sending describe request before create, for volume %s and path %s",
        diskId.Quote().data(), volumePath.data());

    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(volumePath);

    NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(request),
                    RequestInfo->Cookie);
}

void TCreateVolumeActor::CreateVolume(const TActorContext& ctx)
{
    Become(&TThis::StateCreateVolume);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume);

    modifyScheme.SetWorkingDir(VolumeDir);

    auto* op = modifyScheme.MutableCreateBlockStoreVolume();
    op->SetName(VolumeName);

    op->MutableVolumeConfig()->CopyFrom(VolumeConfig);

    auto request =
        std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(modifyScheme);

    LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
              "Sending create request for %s in directory %s",
              VolumeName.Quote().data(), VolumeDir.Quote().data());

    NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TCreateVolumeActor::EnsureDirs(const TActorContext& ctx)
{
    if (NextItemToCreate < ParentDirs.size()) {
        CreateNextDir(ctx);
    } else {
        CreateVolume(ctx);
    }
}

void TCreateVolumeActor::CreateNextDir(const TActorContext& ctx)
{
    Become(&TThis::StateCreateNextDir);

    TString parentDir = ParentDirs[NextItemToCreate];
    TString itemName = VolumePathItems[NextItemToCreate];

    LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
              "Sending mkdir request for %s in directory %s",
              itemName.Quote().data(), parentDir.Quote().data());

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(parentDir);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);

    auto* op = modifyScheme.MutableMkDir();
    op->SetName(itemName);

    auto request = std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(request),
                    RequestInfo->Cookie);
}

void TCreateVolumeActor::DescribeVolumeAfterCreate(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolumeAfterCreate);

    const auto volumePath = VolumeDir + "/" + VolumeName;

    LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
              "Volume %s: sending describe request after create for path %s",
              VolumeConfig.GetDiskId().Quote().data(), volumePath.data());

    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(volumePath);

    NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(request),
                    RequestInfo->Cookie);
}

bool TCreateVolumeActor::VerifyVolume(
    const TActorContext& ctx, const NKikimrBlockStore::TVolumeConfig& actual)
{
    // Idempotency check.
    // Selectively compare fields corresponding to TCreateVolumeRequest.
    // See NBS-1250.

    if (VolumeConfig.GetDiskId() != actual.GetDiskId()) {
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Created volume DiskId mismatch: expected=%s, actual=%s",
                  VolumeConfig.GetDiskId().Quote().data(),
                  actual.GetDiskId().Quote().data());
        return false;
    }

    if (VolumeConfig.GetProjectId() != actual.GetProjectId()) {
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Created volume ProjectId mismatch: expected=%s, actual=%s",
                  VolumeConfig.GetProjectId().Quote().data(),
                  actual.GetProjectId().Quote().data());
        return false;
    }

    if (VolumeConfig.GetBlockSize() != actual.GetBlockSize()) {
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Created volume BlockSize mismatch: expected=%lu, actual=%lu",
                  VolumeConfig.GetBlockSize(), actual.GetBlockSize());
        return false;
    }

    if (GetBlocksCount(VolumeConfig) != GetBlocksCount(actual)) {
        LOG_ERROR(
            ctx, NKikimrServices::NBS_SS_PROXY,
            "Created volume BlocksCount mismatch: expected=%lu, actual=%lu",
            GetBlocksCount(VolumeConfig), GetBlocksCount(actual));
        return false;
    }

    if (VolumeConfig.GetStorageMediaKind() != actual.GetStorageMediaKind()) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_SS_PROXY,
            "Created volume StorageMediaKind mismatch: expected=%lu, "
            "actual=%lu", VolumeConfig.GetStorageMediaKind(),
            actual.GetStorageMediaKind());
        return false;
    }

    if (VolumeConfig.GetFolderId() != actual.GetFolderId()) {
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Created volume FolderId mismatch: expected=%s, actual=%s",
                  VolumeConfig.GetFolderId().Quote().data(),
                  actual.GetFolderId().Quote().data());
        return false;
    }

    if (VolumeConfig.GetCloudId() != actual.GetCloudId()) {
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Created volume CloudId mismatch: expected=%s, actual=%s",
                  VolumeConfig.GetCloudId().Quote().data(),
                  actual.GetCloudId().Quote().data());
        return false;
    }

    if (VolumeConfig.GetTabletVersion() != actual.GetTabletVersion()) {
        LOG_ERROR(
            ctx, NKikimrServices::NBS_SS_PROXY,
            "Created volume TabletVersion mismatch: expected=%lu, actual=%lu",
            VolumeConfig.GetTabletVersion(), actual.GetTabletVersion());
        return false;
    }

    if (VolumeConfig.GetBaseDiskCheckpointId() !=
        actual.GetBaseDiskCheckpointId())
    {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_SS_PROXY,
            "Created volume BaseDiskCheckpointId mismatch: expected=%s, "
            "actual=%s", VolumeConfig.GetBaseDiskCheckpointId().Quote().data(),
            actual.GetBaseDiskCheckpointId().Quote().data());
        return false;
    }

    if (VolumeConfig.GetFillGeneration() != actual.GetFillGeneration()) {
        LOG_ERROR(
            ctx, NKikimrServices::NBS_SS_PROXY,
            "Created volume FillGeneration mismatch: expected=%lu, actual=%lu",
            VolumeConfig.GetFillGeneration(), actual.GetFillGeneration());
        return false;
    }

    return true;
}

void TCreateVolumeActor::HandleDescribeVolumeBeforeCreateResponse(
    const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();

    // TODO: use E_NOT_FOUND instead of StatusPathDoesNotExist
    if (FAILED(error.GetCode())) {
        if (FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD) {
            auto status = static_cast<NKikimrScheme::EStatus>(
                STATUS_FROM_CODE(error.GetCode()));
            if (status == NKikimrScheme::StatusPathDoesNotExist) {
                CreateVolume(ctx);
                return;
            }
        }

        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Volume %s: describe before create failed: %s",
                  VolumeConfig.GetDiskId().Quote().data(),
                  FormatError(error).data());

        ReplyAndDie(
            ctx, std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto pathType = pathDescription.GetSelf().GetPathType();

    if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
        LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Volume %s: described path %s is not a BlockStoreVolume",
                  VolumeConfig.GetDiskId().Quote().data(), msg->Path.data());

        CreateVolume(ctx);
        return;
    }

    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& describedVolumeConfig = volumeDescription.GetVolumeConfig();

    if (!VerifyVolume(ctx, describedVolumeConfig)) {
        const auto& diskId = VolumeConfig.GetDiskId();
        const TString errorMsg =
            TStringBuilder()
            << "Volume " << diskId.Quote()
            << " with different config already exists at path " << msg->Path;
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY, errorMsg);

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(
                MakeError(E_FAIL, errorMsg)));
        return;
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(
            MakeError(S_ALREADY)));
}

void TCreateVolumeActor::HandleCreateVolumeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    const auto& diskId = VolumeConfig.GetDiskId();

    if (FAILED(error.GetCode())) {
        if (FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD) {
            auto status = static_cast<NKikimrScheme::EStatus>(
                STATUS_FROM_CODE(error.GetCode()));
            if (FirstCreationAttempt && ParentDirs &&
                status == NKikimrScheme::StatusPathDoesNotExist)
            {
                // Try creating intermediate directories
                FirstCreationAttempt = false;
                EnsureDirs(ctx);
                return;
            }
        }

        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Volume %s: create failed: %s", diskId.Quote().data(),
                  FormatError(error).data());

        ReplyAndDie(
            ctx, std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error));
        return;
    }

    LOG_INFO(ctx, NKikimrServices::NBS_SS_PROXY,
             "Volume %s created successfully", diskId.Quote().data());

    DescribeVolumeAfterCreate(ctx);
}

void TCreateVolumeActor::HandleMkDirResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY,
                  "Volume %s: mkdir parentDir %s itemName %s failed: %s",
                  VolumeConfig.GetDiskId().Quote().data(),
                  ParentDirs[NextItemToCreate].Quote().data(),
                  VolumePathItems[NextItemToCreate].Quote().data(),
                  FormatError(error).data());

        ReplyAndDie(
            ctx, std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error));
        return;
    }

    ++NextItemToCreate;
    EnsureDirs(ctx);
}

void TCreateVolumeActor::HandleDescribeVolumeAfterCreateResponse(
    const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& diskId = VolumeConfig.GetDiskId();

    auto error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_WARN(ctx, NKikimrServices::NBS_SS_PROXY,
                 "Volume %s: describe after create failed: %s",
                 diskId.Quote().data(), FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(
                std::move(error)));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto pathType = pathDescription.GetSelf().GetPathType();

    if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_SS_PROXY,
            "Volume %s: describe after create failed: described path %s is not "
            "a BlockStoreVolume", diskId.Quote().data(), msg->Path.data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Described path is not a BlockStoreVolume")));
        return;
    }

    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& describedVolumeConfig = volumeDescription.GetVolumeConfig();

    if (VolumeConfig.GetFillGeneration() >
        describedVolumeConfig.GetFillGeneration())
    {
        const auto& diskId = VolumeConfig.GetDiskId();
        const TString errorMsg = TStringBuilder()
                                 << "Volume " << diskId.Quote()
                                 << " has outdated FillGeneration";
        LOG_ERROR(ctx, NKikimrServices::NBS_SS_PROXY, errorMsg);

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(
                MakeError(E_ABORTED, errorMsg)));
        return;
    }

    if (!VerifyVolume(ctx, describedVolumeConfig)) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_SS_PROXY,
            "Volume %s: describe after create failed: described volume config "
            "mismatch", diskId.Quote().data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "Described volume config mismatch")));
        return;
    }

    ReplyAndDie(ctx, std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>());
}

void TCreateVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvCreateVolumeResponse> response)
{
    NYdb::NBS::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TCreateVolumeActor::StateDescribeVolumeBeforeCreate)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeSchemeResponse,
              HandleDescribeVolumeBeforeCreateResponse);

        default:
            HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateVolumeActor::StateCreateVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleCreateVolumeResponse);

        default:
            HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateVolumeActor::StateCreateNextDir)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleMkDirResponse);

        default:
            HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateVolumeActor::StateDescribeVolumeAfterCreate)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeSchemeResponse,
              HandleDescribeVolumeAfterCreateResponse);

        default:
            HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleCreateVolume(
    const TEvSSProxy::TEvCreateVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NKikimrBlockStore::TVolumeConfig volumeConfig;
    volumeConfig.CopyFrom(msg->VolumeConfig);

    LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
              "Volume %s: volumeConfig before ResizeVolume: %s",
              volumeConfig.GetDiskId().Quote().data(),
              volumeConfig.DebugString().data());

    ResizeVolume(NbsStorageConfig, volumeConfig);

    LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
              "Volume %s: volumeConfig after ResizeVolume: %s",
              volumeConfig.GetDiskId().Quote().data(),
              volumeConfig.DebugString().data());

    NYdb::NBS::Register<TCreateVolumeActor>(
        ctx, std::move(requestInfo), NbsStorageConfig.GetSchemeShardDir(),
        volumeConfig);
}

}   // namespace NYdb::NBS::NStorage
