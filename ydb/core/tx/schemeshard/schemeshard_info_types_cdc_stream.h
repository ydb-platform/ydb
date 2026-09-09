#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NSchemeShard {

struct TCdcStreamSettings {
    using TSelf = TCdcStreamSettings;
    using EMode = NKikimrSchemeOp::ECdcStreamMode;
    using EFormat = NKikimrSchemeOp::ECdcStreamFormat;
    using EState = NKikimrSchemeOp::ECdcStreamState;

    #define OPTION(type, name) \
        TSelf&& With##name(type value) && { \
            name = std::move(value); \
            return std::move(*this); \
        } \
        type name;

    OPTION(EMode, Mode);
    OPTION(EFormat, Format);
    OPTION(bool, VirtualTimestamps);
    OPTION(TDuration, ResolvedTimestamps);
    OPTION(bool, SchemaChanges);
    OPTION(TString, AwsRegion);
    OPTION(EState, State);
    OPTION(bool, UserSIDs);
    OPTION(bool, TraceIds);

    #undef OPTION
};

struct TCdcStreamInfo
    : public TCdcStreamSettings
    , public TSimpleRefCount<TCdcStreamInfo>
{
    using TPtr = TIntrusivePtr<TCdcStreamInfo>;

    // shards of the table
    struct TShardStatus {
        NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus Status;

        explicit TShardStatus(NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus status)
            : Status(status)
        {}
    };

    TCdcStreamInfo(ui64 version, TCdcStreamSettings&& settings)
        : TCdcStreamSettings(std::move(settings))
        , AlterVersion(version)
    {}

    TCdcStreamInfo(const TCdcStreamInfo&) = default;

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TCdcStreamInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static TPtr New(TCdcStreamSettings settings) {
        settings.State = EState::ECdcStreamStateInvalid;
        return new TCdcStreamInfo(0, std::move(settings));
    }

    static TPtr Create(const NKikimrSchemeOp::TCdcStreamDescription& desc) {
        TPtr result = New(TCdcStreamSettings()
            .WithMode(desc.GetMode())
            .WithFormat(desc.GetFormat())
            .WithVirtualTimestamps(desc.GetVirtualTimestamps())
            .WithResolvedTimestamps(TDuration::MilliSeconds(desc.GetResolvedTimestampsIntervalMs()))
            .WithSchemaChanges(desc.GetSchemaChanges())
            .WithAwsRegion(desc.GetAwsRegion())
            .WithUserSIDs(desc.GetUserSIDs())
            .WithTraceIds(desc.GetTraceIds())
        );
        TPtr alterData = result->CreateNextVersion();
        alterData->State = EState::ECdcStreamStateReady;
        if (desc.HasState()) {
            alterData->State = desc.GetState();
        }

        return result;
    }

    void Serialize(NKikimrSchemeOp::TCdcStreamDescription& desc) const {
        desc.SetSchemaVersion(AlterVersion);
        desc.SetMode(Mode);
        desc.SetFormat(Format);
        desc.SetVirtualTimestamps(VirtualTimestamps);
        desc.SetResolvedTimestampsIntervalMs(ResolvedTimestamps.MilliSeconds());
        desc.SetSchemaChanges(SchemaChanges);
        desc.SetAwsRegion(AwsRegion);
        desc.SetState(State);
        if (ScanShards) {
            auto& scanProgress = *desc.MutableScanProgress();
            scanProgress.SetShardsTotal(ScanShards.size());
            scanProgress.SetShardsCompleted(DoneShards.size());
        }
        desc.SetUserSIDs(UserSIDs);
        desc.SetTraceIds(TraceIds);
    }

    void FinishAlter() {
        Y_ENSURE(AlterData);

        AlterVersion = AlterData->AlterVersion;
        static_cast<TCdcStreamSettings&>(*this) = static_cast<TCdcStreamSettings&>(*AlterData);

        AlterData.Reset();
    }

    ui64 AlterVersion = 1;
    TCdcStreamInfo::TPtr AlterData = nullptr;

    TMap<TShardIdx, TShardStatus> ScanShards;
    THashSet<TShardIdx> PendingShards;
    THashSet<TShardIdx> InProgressShards;
    THashSet<TShardIdx> DoneShards;
};

}
}
