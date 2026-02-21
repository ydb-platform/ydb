#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <ydb/library/actors/core/actorid.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SS_PROXY_REQUESTS(xxx, ...) \
    xxx(CreateVolume, __VA_ARGS__)             \
    xxx(ModifyScheme, __VA_ARGS__)             \
    xxx(DescribeScheme, __VA_ARGS__)           \
    xxx(WaitSchemeTx, __VA_ARGS__)             \
                                               \
    xxx(BackupPathDescriptions, __VA_ARGS__)

// BLOCKSTORE_SS_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvSSProxy
{
    //
    // CreateVolume
    //

    struct TCreateVolumeRequest
    {
        const NKikimrBlockStore::TVolumeConfig VolumeConfig;

        explicit TCreateVolumeRequest(NKikimrBlockStore::TVolumeConfig config)
            : VolumeConfig(std::move(config))
        {}
    };

    struct TCreateVolumeResponse
    {
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TCreateVolumeResponse(
            NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
            TString reason = {})
            : Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // ModifyScheme
    //

    struct TModifySchemeRequest
    {
        const NKikimrSchemeOp::TModifyScheme ModifyScheme;

        explicit TModifySchemeRequest(
            NKikimrSchemeOp::TModifyScheme modifyScheme)
            : ModifyScheme(std::move(modifyScheme))
        {}
    };

    struct TModifySchemeResponse
    {
        const ui64 SchemeShardTabletId;
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TModifySchemeResponse(
            ui64 schemeShardTabletId = 0,
            NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
            TString reason = TString())
            : SchemeShardTabletId(schemeShardTabletId)
            , Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // ModifyVolume
    //

    struct TModifyVolumeRequest
    {
        enum class EOpType
        {
            Assign,
            Destroy
        };

        const EOpType OpType;
        const TString DiskId;

        const TString NewMountToken;
        const ui64 TokenVersion;

        const ui64 FillGeneration;

        TModifyVolumeRequest(EOpType opType, TString diskId,
                             TString newMountToken, ui64 tokenVersion,
                             ui64 fillGeneration = 0)
            : OpType(opType)
            , DiskId(std::move(diskId))
            , NewMountToken(std::move(newMountToken))
            , TokenVersion(tokenVersion)
            , FillGeneration(fillGeneration)
        {}
    };

    struct TModifyVolumeResponse
    {
        const ui64 SchemeShardTabletId;
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TModifyVolumeResponse(
            ui64 schemeShardTabletId = 0,
            NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
            TString reason = TString())
            : SchemeShardTabletId(schemeShardTabletId)
            , Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // DescribeScheme
    //

    struct TDescribeSchemeRequest
    {
        const TString Path;

        explicit TDescribeSchemeRequest(TString path)
            : Path(std::move(path))
        {}
    };

    struct TDescribeSchemeResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TDescribeSchemeResponse() = default;

        TDescribeSchemeResponse(
            TString path, NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    //
    // DescribeVolume
    //

    struct TDescribeVolumeRequest
    {
        const TString DiskId;

        explicit TDescribeVolumeRequest(TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    struct TDescribeVolumeResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TDescribeVolumeResponse() = default;

        TDescribeVolumeResponse(
            TString path, NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}

        TDescribeVolumeResponse(TString path)
            : TDescribeVolumeResponse(std::move(path), {})
        {}
    };

    //
    // WaitSchemeTx
    //

    struct TWaitSchemeTxRequest
    {
        const ui64 SchemeShardTabletId;
        const ui64 TxId;

        TWaitSchemeTxRequest(ui64 schemeShardTabletId, ui64 txId)
            : SchemeShardTabletId(schemeShardTabletId)
            , TxId(txId)
        {}
    };

    struct TWaitSchemeTxResponse
    {
    };

    //
    // BackupPathDescriptions
    //

    struct TBackupPathDescriptionsRequest
    {
    };

    struct TBackupPathDescriptionsResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2_SS_PROXY),

        EvCreateVolumeRequest = EvBegin + 1,
        EvCreateVolumeResponse = EvBegin + 2,

        EvModifySchemeRequest = EvBegin + 3,
        EvModifySchemeResponse = EvBegin + 4,

        EvModifyVolumeRequest = EvBegin + 5,
        EvModifyVolumeResponse = EvBegin + 6,

        EvDescribeSchemeRequest = EvBegin + 7,
        EvDescribeSchemeResponse = EvBegin + 8,

        EvDescribeVolumeRequest = EvBegin + 9,
        EvDescribeVolumeResponse = EvBegin + 10,

        EvWaitSchemeTxRequest = EvBegin + 11,
        EvWaitSchemeTxResponse = EvBegin + 12,

        EvBackupPathDescriptionsRequest = EvBegin + 13,
        EvBackupPathDescriptionsResponse = EvBegin + 14,

        EvEnd
    };

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeSSProxyServiceId();

}   // namespace NYdb::NBS::NStorage
