#pragma once

#include "sys_params.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/base/pathid.h>

#include <util/generic/ptr.h>

#include <memory>

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
        Removing,
        Error = 255
    };

    enum class EStreamState: ui8 {
        Creating,
        Ready,
        Removing,
        Error = 255
    };

    class ITarget {
    public:
        virtual ~ITarget() = default;

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

        virtual void Progress(ui64 schemeShardId, const TActorId& proxy, const TActorContext& ctx) = 0;
        virtual void Shutdown(const TActorContext& ctx) = 0;
    };

public:
    explicit TReplication(ui64 id, const TPathId& pathId, const NKikimrReplication::TReplicationConfig& config);
    explicit TReplication(ui64 id, const TPathId& pathId, NKikimrReplication::TReplicationConfig&& config);
    explicit TReplication(ui64 id, const TPathId& pathId, const TString& config);

    ui64 AddTarget(ETargetKind kind, const TString& srcPath, const TString& dstPath);
    ITarget* AddTarget(ui64 id, ETargetKind kind, const TString& srcPath, const TString& dstPath);
    const ITarget* FindTarget(ui64 id) const;
    ITarget* FindTarget(ui64 id);

    void Progress(const TActorContext& ctx);
    void Shutdown(const TActorContext& ctx);

    ui64 GetId() const;
    void SetState(EState state, TString issue = {});
    EState GetState() const;
    const TString& GetIssue() const;

    void SetNextTargetId(ui64 value);
    ui64 GetNextTargetId() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;

}; // TReplication

}
