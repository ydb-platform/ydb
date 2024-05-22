#pragma once

#include "sys_params.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/ptr.h>

#include <memory>
#include <optional>

namespace NKikimrReplication {
    class TReplicationConfig;
}

namespace NKikimr::NReplication::NController {

class TReplication: public TSimpleRefCount<TReplication> {
public:
    using TPtr = TIntrusivePtr<TReplication>;

    enum class EState: ui8 {
        Ready,
        Removing,
        Error = 255
    };

    enum class ETargetKind: ui8 {
        Table,
    };

    enum class EDstState: ui8 {
        Creating,
        Syncing,
        Ready,
        Alter,
        Done,
        Removing,
        Error = 255
    };

    enum class EStreamState: ui8 {
        Creating,
        Ready,
        Removing,
        Removed,
        Error = 255
    };

    class ITarget {
    public:
        virtual ~ITarget() = default;

        virtual ui64 GetId() const = 0;
        virtual ETargetKind GetKind() const = 0;

        virtual const TString& GetSrcPath() const = 0;
        virtual const TString& GetDstPath() const = 0;

        virtual EDstState GetDstState() const = 0;
        virtual void SetDstState(const EDstState value) = 0;

        virtual const TPathId& GetDstPathId() const = 0;
        virtual void SetDstPathId(const TPathId& value) = 0;

        virtual const TString& GetStreamName() const = 0;
        virtual void SetStreamName(const TString& value) = 0;

        virtual EStreamState GetStreamState() const = 0;
        virtual void SetStreamState(EStreamState value) = 0;

        virtual const TString& GetIssue() const = 0;
        virtual void SetIssue(const TString& value) = 0;

        virtual void Progress(TReplication::TPtr replication, const TActorContext& ctx) = 0;
        virtual void Shutdown(const TActorContext& ctx) = 0;

    protected:
        virtual IActor* CreateWorkerRegistar(TReplication::TPtr replication, const TActorContext& ctx) const = 0;
    };

    struct TDropOp {
        TActorId Sender;
        std::pair<ui64, ui32> OperationId; // txId, partId
    };

public:
    explicit TReplication(ui64 id, const TPathId& pathId, const NKikimrReplication::TReplicationConfig& config);
    explicit TReplication(ui64 id, const TPathId& pathId, NKikimrReplication::TReplicationConfig&& config);
    explicit TReplication(ui64 id, const TPathId& pathId, const TString& config);

    ui64 AddTarget(ETargetKind kind, const TString& srcPath, const TString& dstPath);
    ITarget* AddTarget(ui64 id, ETargetKind kind, const TString& srcPath, const TString& dstPath);
    const ITarget* FindTarget(ui64 id) const;
    ITarget* FindTarget(ui64 id);
    void RemoveTarget(ui64 id);

    void Progress(const TActorContext& ctx);
    void Shutdown(const TActorContext& ctx);

    ui64 GetId() const;
    const TPathId& GetPathId() const;
    const TActorId& GetYdbProxy() const;
    ui64 GetSchemeShardId() const;
    const NKikimrReplication::TReplicationConfig& GetConfig() const;
    void SetState(EState state, TString issue = {});
    EState GetState() const;
    const TString& GetIssue() const;

    void SetNextTargetId(ui64 value);
    ui64 GetNextTargetId() const;

    void UpdateSecret(const TString& secretValue);

    void SetTenant(const TString& value);
    const TString& GetTenant() const;

    void SetDropOp(const TActorId& sender, const std::pair<ui64, ui32>& opId);
    const std::optional<TDropOp>& GetDropOp() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;
    std::optional<TDropOp> DropOp;

}; // TReplication

}
