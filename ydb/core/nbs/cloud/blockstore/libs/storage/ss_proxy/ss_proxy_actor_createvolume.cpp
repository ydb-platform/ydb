#include "ss_proxy_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_label.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_model.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>

#include <ydb/core/base/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_SS_PROXY

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
    TCreateVolumeActor(
        TRequestInfoPtr requestInfo,
        TString schemeShardDir,
        TVolumeConfig volumeConfig);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolumeBeforeCreate(const TActorContext& ctx);

    void CreateVolume(const TActorContext& ctx);

    void EnsureDirs(const TActorContext& ctx);

    void CreateNextDir(const TActorContext& ctx);

    void DescribeVolumeAfterCreate(const TActorContext& ctx);

    bool VerifyVolume(
        const TActorContext& ctx,
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

TCreateVolumeActor::TCreateVolumeActor(
    TRequestInfoPtr requestInfo,
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

    YDB_LOG_DEBUG_CTX(ctx, "Sending describe request before create, for volume and diskId",
        {"#_diskId.Quote().data", diskId.Quote().data()},
        {"#_diskId.data", diskId.data()});

    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(diskId.data());

    NYdb::NBS::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
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

    YDB_LOG_DEBUG_CTX(ctx, "Sending create request for in directory",
        {"#_VolumeName.Quote().data", VolumeName.Quote().data()},
        {"#_VolumeDir.Quote().data", VolumeDir.Quote().data()});

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

    YDB_LOG_DEBUG_CTX(ctx, "Sending mkdir request for in directory",
        {"#_itemName.Quote().data", itemName.Quote().data()},
        {"#_parentDir.Quote().data", parentDir.Quote().data()});

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(parentDir);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);

    auto* op = modifyScheme.MutableMkDir();
    op->SetName(itemName);

    auto request = std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    NYdb::NBS::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCreateVolumeActor::DescribeVolumeAfterCreate(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolumeAfterCreate);

    YDB_LOG_DEBUG_CTX(ctx, "Volume sending describe request after create",
        {"#_VolumeConfig.GetDiskId().Quote().data", VolumeConfig.GetDiskId().Quote().data()});

    auto request = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(
        VolumeConfig.GetDiskId());

    NYdb::NBS::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

bool TCreateVolumeActor::VerifyVolume(
    const TActorContext& ctx,
    const NKikimrBlockStore::TVolumeConfig& actual)
{
    // Idempotency check.
    // Selectively compare fields corresponding to TCreateVolumeRequest.
    // See NBS-1250.

    if (VolumeConfig.GetDiskId() != actual.GetDiskId()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume DiskId mismatch",
            {"expected", VolumeConfig.GetDiskId().Quote().data()},
            {"actual", actual.GetDiskId().Quote().data()});
        return false;
    }

    if (VolumeConfig.GetProjectId() != actual.GetProjectId()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume ProjectId mismatch",
            {"expected", VolumeConfig.GetProjectId().Quote().data()},
            {"actual", actual.GetProjectId().Quote().data()});
        return false;
    }

    if (VolumeConfig.GetBlockSize() != actual.GetBlockSize()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume BlockSize mismatch",
            {"expected", VolumeConfig.GetBlockSize()},
            {"actual", actual.GetBlockSize()});
        return false;
    }

    if (GetBlocksCount(VolumeConfig) != GetBlocksCount(actual)) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume BlocksCount mismatch",
            {"expected", GetBlocksCount(VolumeConfig)},
            {"actual", GetBlocksCount(actual)});
        return false;
    }

    if (VolumeConfig.GetStorageMediaKind() != actual.GetStorageMediaKind()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume StorageMediaKind mismatch",
            {"expected", VolumeConfig.GetStorageMediaKind()},
            {"actual", actual.GetStorageMediaKind()});
        return false;
    }

    if (VolumeConfig.GetFolderId() != actual.GetFolderId()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume FolderId mismatch",
            {"expected", VolumeConfig.GetFolderId().Quote().data()},
            {"actual", actual.GetFolderId().Quote().data()});
        return false;
    }

    if (VolumeConfig.GetCloudId() != actual.GetCloudId()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume CloudId mismatch",
            {"expected", VolumeConfig.GetCloudId().Quote().data()},
            {"actual", actual.GetCloudId().Quote().data()});
        return false;
    }

    if (VolumeConfig.GetTabletVersion() != actual.GetTabletVersion()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume TabletVersion mismatch",
            {"expected", VolumeConfig.GetTabletVersion()},
            {"actual", actual.GetTabletVersion()});
        return false;
    }

    if (VolumeConfig.GetBaseDiskCheckpointId() !=
        actual.GetBaseDiskCheckpointId())
    {
        YDB_LOG_ERROR_CTX(ctx, "Created volume BaseDiskCheckpointId mismatch",
            {"expected", VolumeConfig.GetBaseDiskCheckpointId().Quote().data()},
            {"actual", actual.GetBaseDiskCheckpointId().Quote().data()});
        return false;
    }

    if (VolumeConfig.GetFillGeneration() != actual.GetFillGeneration()) {
        YDB_LOG_ERROR_CTX(ctx, "Created volume FillGeneration mismatch",
            {"expected", VolumeConfig.GetFillGeneration()},
            {"actual", actual.GetFillGeneration()});
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

        YDB_LOG_ERROR_CTX(ctx, "Volume describe before create",
            {"#_VolumeConfig.GetDiskId().Quote().data", VolumeConfig.GetDiskId().Quote().data()},
            {"failed", FormatError(error).data()});

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto pathType = pathDescription.GetSelf().GetPathType();

    if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
        YDB_LOG_DEBUG_CTX(ctx, "Volume described path is not a BlockStoreVolume",
            {"#_VolumeConfig.GetDiskId().Quote().data", VolumeConfig.GetDiskId().Quote().data()},
            {"#_msg->Path.data", msg->Path.data()});

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
        YDB_LOG_ERROR_CTX(ctx, errorMsg);

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

        YDB_LOG_ERROR_CTX(ctx, "Volume create",
            {"#_diskId.Quote().data", diskId.Quote().data()},
            {"failed", FormatError(error).data()});

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error));
        return;
    }

    YDB_LOG_INFO_CTX(ctx, "Volume created successfully",
        {"#_diskId.Quote().data", diskId.Quote().data()});

    DescribeVolumeAfterCreate(ctx);
}

void TCreateVolumeActor::HandleMkDirResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        YDB_LOG_ERROR_CTX(ctx, "Volume mkdir parentDir itemName",
            {"#_VolumeConfig.GetDiskId().Quote().data", VolumeConfig.GetDiskId().Quote().data()},
            {"#_ParentDirs[NextItemToCreate].Quote().data", ParentDirs[NextItemToCreate].Quote().data()},
            {"#_VolumePathItems[NextItemToCreate].Quote().data", VolumePathItems[NextItemToCreate].Quote().data()},
            {"failed", FormatError(error).data()});

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error));
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
        YDB_LOG_WARN_CTX(ctx, "Volume describe after create",
            {"#_diskId.Quote().data", diskId.Quote().data()},
            {"failed", FormatError(error).data()});

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(
                std::move(error)));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto pathType = pathDescription.GetSelf().GetPathType();

    if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
        YDB_LOG_ERROR_CTX(ctx, "Volume describe after create failed: described path is not a BlockStoreVolume",
            {"#_diskId.Quote().data", diskId.Quote().data()},
            {"#_msg->Path.data", msg->Path.data()});

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
        YDB_LOG_ERROR_CTX(ctx, errorMsg);

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(
                MakeError(E_ABORTED, errorMsg)));
        return;
    }

    if (!VerifyVolume(ctx, describedVolumeConfig)) {
        YDB_LOG_ERROR_CTX(ctx, "Volume describe after create failed: described volume config mismatch",
            {"#_diskId.Quote().data", diskId.Quote().data()});

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
        HFunc(
            TEvSSProxy::TEvDescribeSchemeResponse,
            HandleDescribeVolumeBeforeCreateResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                NKikimrServices::NBS_SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateVolumeActor::StateCreateVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleCreateVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                NKikimrServices::NBS_SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateVolumeActor::StateCreateNextDir)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleMkDirResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                NKikimrServices::NBS_SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateVolumeActor::StateDescribeVolumeAfterCreate)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeSchemeResponse,
            HandleDescribeVolumeAfterCreateResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                NKikimrServices::NBS_SS_PROXY,
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

    YDB_LOG_DEBUG_CTX(ctx, "Volume volumeConfig before",
        {"#_volumeConfig.GetDiskId().Quote().data", volumeConfig.GetDiskId().Quote().data()},
        {"resizeVolume", volumeConfig.DebugString().data()});

    ResizeVolume(NbsStorageConfig, volumeConfig);

    YDB_LOG_DEBUG_CTX(ctx, "Volume volumeConfig after",
        {"#_volumeConfig.GetDiskId().Quote().data", volumeConfig.GetDiskId().Quote().data()},
        {"resizeVolume", volumeConfig.DebugString().data()});

    NYdb::NBS::Register<TCreateVolumeActor>(
        ctx,
        std::move(requestInfo),
        NbsStorageConfig.GetSchemeShardDir(),
        volumeConfig);
}

}   // namespace NYdb::NBS::NStorage
