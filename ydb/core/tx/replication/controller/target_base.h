#pragma once

#include "lag_provider.h"
#include "replication.h"

namespace NKikimr::NReplication::NController {

class TTargetBase
    : public TReplication::ITarget
    , public TLagProvider
{
protected:
    using ETargetKind = TReplication::ETargetKind;
    using EDstState = TReplication::EDstState;
    using EStreamState = TReplication::EStreamState;
    struct TWorker: public TItemWithLag {};

    inline TReplication* GetReplication() const {
        return Replication;
    }

    const THashMap<ui64, TWorker>& GetWorkers() const;
    void RemoveWorkers(const TActorContext& ctx);

public:
    explicit TTargetBase(TReplication* replication, ETargetKind kind,
        ui64 id, const TString& srcPath, const TString& dstPath);

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

    void AddWorker(ui64 id) override;
    void RemoveWorker(ui64 id) override;
    void UpdateLag(ui64 workerId, TDuration lag) override;
    const TMaybe<TDuration> GetLag() const override;

    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

private:
    TReplication* const Replication;
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
    THashMap<ui64, TWorker> Workers;
    bool PendingRemoveWorkers = false;

}; // TTargetBase

}
