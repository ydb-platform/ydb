#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

class TTargetBase: public TReplication::ITarget {
protected:
    using ETargetKind = TReplication::ETargetKind;
    using EDstState = TReplication::EDstState;
    using EStreamState = TReplication::EStreamState;

public:
    explicit TTargetBase(ETargetKind kind, ui64 rid, ui64 tid, const TString& srcPath, const TString& dstPath);

    const TString& GetSrcPath() const override;
    const TString& GetDstPath() const override;

    EDstState GetDstState() const override;
    void SetDstState(const EDstState value) override;

    const TPathId& GetDstPathId() const override;
    void SetDstPathId(const TPathId& value) override;

    const TString& GetStreamName() const override;
    void SetStreamName(const TString& value) override;

    EStreamState GetStreamState() const override;
    void SetStreamState(EStreamState value) override;

    const TString& GetIssue() const override;
    void SetIssue(const TString& value) override;

    void Progress(ui64 schemeShardId, const TActorId& proxy, const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

protected:
    ui64 GetReplicationId() const;
    ui64 GetTargetId() const;

private:
    const ETargetKind Kind;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TString SrcPath;
    const TString DstPath;

    EDstState DstState = EDstState::Creating;
    TPathId DstPathId;
    TString StreamName;
    EStreamState StreamState = EStreamState::Ready;
    TString Issue;

    TActorId DstCreator;
    TActorId DstRemover;

}; // TTargetBase

}
