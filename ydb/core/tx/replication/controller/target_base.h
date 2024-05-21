#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

class TTargetBase: public TReplication::ITarget {
protected:
    using ETargetKind = TReplication::ETargetKind;
    using EDstState = TReplication::EDstState;
    using EStreamState = TReplication::EStreamState;

public:
    explicit TTargetBase(ETargetKind kind, ui64 id, const TString& srcPath, const TString& dstPath);

    ui64 GetId() const override;
    ETargetKind GetKind() const override;

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

    void Progress(TReplication::TPtr replication, const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

private:
    const ui64 Id;
    const ETargetKind Kind;
    const TString SrcPath;
    const TString DstPath;

    EDstState DstState = EDstState::Creating;
    TPathId DstPathId;
    TString StreamName;
    EStreamState StreamState = EStreamState::Ready;
    TString Issue;

    TActorId DstCreator;
    TActorId DstAlterer;
    TActorId DstRemover;
    TActorId WorkerRegistar;

}; // TTargetBase

}
