#include "lock.h"

#include "yt_wrapper.h"

#include <ydb/library/yql/utils/log/log.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/core/ytree/public.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/svnversion/svnversion.h>

#include <ydb/library/yql/providers/dq/common/attrs.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>

#include <util/string/builder.h>
#include <util/system/hostname.h>
#include <util/system/getpid.h>

namespace NYql {

using namespace NActors;

struct TLockRequest: public TActor<TLockRequest> {
    TLockRequest(
        const NYT::NApi::ITransactionPtr& transaction,
        TActorId ytWrapper,
        TActorId lockActorId,
        TActorId parentId,
        const TString prefix,
        const TString lockName,
        NYT::TNode attributes)
        : TActor<TLockRequest>(&TLockRequest::Handler)
        , Transaction(transaction)
        , LockActorId(lockActorId)
        , ParentId(parentId)
        , Prefix(prefix)
        , LockName(lockName)
        , Attributes(attributes)
        , Revision(GetProgramCommitId())
        , YtWrapper(ytWrapper)
    { }

    ~TLockRequest()
    { }

private:
    STRICT_STFUNC(Handler, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        CFunc(TEvents::TEvBootstrap::EventType, CreateLockNode);
        HFunc(TEvCreateNodeResponse, OnCreateLockNode);
        HFunc(TEvSetNodeResponse, OnNodeLocked);
    });

    STRICT_STFUNC(ReadInfoNodeHandler, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(TEvGetNodeResponse, SendBecomeFollower);
    });

    STRICT_STFUNC(GetOrCreateEpochHandler, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(TEvGetNodeResponse, OnGetEpochNode);
        HFunc(TEvCreateNodeResponse, OnCreateEpochNode);
    });

    STRICT_STFUNC(SetAttributesHandler, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(TEvSetNodeResponse, OnSetAttribute);
    });

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
    }

    void OnCreateLockNode(TEvCreateNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            NYT::NApi::TLockNodeOptions options;
            options.Waitable = false;
            auto* actorSystem = ctx.ExecutorThread.ActorSystem;
            auto selfId = SelfId();
            try {
                YT_UNUSED_FUTURE(Transaction->LockNode("#" + ToString(result.ValueOrThrow()), NYT::NCypressClient::ELockMode::Exclusive, options).As<void>()
                    .Apply(BIND([actorSystem, selfId](const NYT::TErrorOr<void>& result) {
                        actorSystem->Send(selfId, new TEvSetNodeResponse(0, result));
                    })));
            } catch (...) {
                Finish(ctx);
            }
        } else {
            Finish(result, ctx);
        }
    }

    void OnNodeLocked(TEvSetNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            GetOrCreateEpoch();
        } else {
            Finish(result, ctx);
        }
    }

    void PassAway() override {
        YT_UNUSED_FUTURE(Transaction->Abort());
        Transaction.Reset();
        Send(LockActorId, new TEvTick());
        IActor::PassAway();
    }

    void OnGetEpochNode(TEvGetNodeResponse::TPtr& ev, const TActorContext& ctx) {
        auto result = std::get<0>(*ev->Get());
        try {
            if (!result.IsOK() && !result.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                Finish(result, ctx);
            } else if (result.IsOK()) {
                auto epoch = static_cast<ui32>(NYT::NodeFromYsonString(result.Value().AsStringBuf()).AsUint64());
                SetInfoNodeAttributes(epoch);
            } else {
                NYT::NApi::TCreateNodeOptions createNodeOptions;
                auto prereqId = Transaction->GetId();

                createNodeOptions.IgnoreExisting = true;
                createNodeOptions.PrerequisiteTransactionIds.push_back(prereqId);

                auto nodePath = Prefix + "/" + LockName;
                Send(YtWrapper, new TEvCreateNode(nodePath, NYT::NObjectClient::EObjectType::StringNode, createNodeOptions));
            }
        } catch (...) {
            Finish(ctx);
        }
    }

    void OnCreateEpochNode(TEvCreateNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            SetInfoNodeAttributes(0);
        } else {
            Finish(result, ctx);
        }
    }

    void SetInfoNodeAttributes(ui32 epoch) {
        epoch += 1;
        Epoch = epoch;

        auto prereqId = Transaction->GetId();

        NYT::NApi::TSetNodeOptions setNodeOptions;
        setNodeOptions.PrerequisiteTransactionIds.push_back(prereqId);

        auto attributesMap = Attributes.AsMap();
        attributesMap[NCommonAttrs::EPOCH_ATTR] = NYT::TNode(epoch);
        attributesMap[NCommonAttrs::REVISION_ATTR] = NYT::TNode(Revision);

        AttributesCount = attributesMap.size();
        Become(&TLockRequest::SetAttributesHandler);

        auto nodePath = Prefix + "/" + LockName;
        for (const auto& [k, v]: attributesMap) {
            Send(YtWrapper, new TEvSetNode(
                nodePath + "/@" + k,
                NYT::NYson::TYsonString(NYT::NodeToYsonString(v)),
                setNodeOptions));
        }
    }

    void OnSetAttribute(TEvSetNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            if (--AttributesCount == 0) {
                SendBecomeLeader();
                Finish(result, ctx);
            }
        } else {
            Finish(result, ctx);
        }
    }

    void GetOrCreateEpoch()
    {
        Become(&TLockRequest::GetOrCreateEpochHandler);

        auto prereqId = Transaction->GetId();

        NYT::NApi::TGetNodeOptions getNodeOptions;
        getNodeOptions.PrerequisiteTransactionIds.push_back(prereqId);
        auto nodePath = Prefix + "/" + LockName;

        Send(YtWrapper, new TEvGetNode(nodePath + "/@" + NCommonAttrs::EPOCH_ATTR, getNodeOptions));
    }

    void ReadInfoNode() {
        Become(&TLockRequest::ReadInfoNodeHandler);

        auto nodePath = Prefix + "/" + LockName + "/@";

        Send(YtWrapper, new TEvGetNode(nodePath, NYT::NApi::TGetNodeOptions()));
    }

    void SendBecomeLeader() {
        auto attributes = NYT::NYson::TYsonString(NYT::NodeToYsonString(Attributes));
        Send(ParentId, new TEvBecomeLeader(Epoch, ToString(Transaction->GetId()), attributes.ToString()));
    }

    void SendBecomeFollower(TEvGetNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            Send(ParentId, new TEvBecomeFollower(result.ValueOrThrow().ToString()));
        }
        Send(SelfId(), new TEvents::TEvPoison());
    }

    template<typename T>
    void Finish(const NYT::TErrorOr<T>& result, const TActorContext& ctx) {
        if (result.IsOK()) {
            auto* actorSystem = ctx.ExecutorThread.ActorSystem;
            auto selfId = SelfId();
            Transaction->SubscribeAborted(BIND([actorSystem, selfId](const NYT::TError& /*error*/) {
                actorSystem->Send(selfId, new TEvents::TEvPoison());
            }));
        } else {
            if (!result.FindMatching(NYT::NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
                YQL_CLOG(WARN, ProviderDq) << ToString(result);
            }
            ReadInfoNode();
        }
    }

    void Finish(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_CLOG(WARN, ProviderDq) << CurrentExceptionMessage();
        ReadInfoNode();
    }

    void CreateLockNode(const TActorContext& ctx) {
        auto lockNode = Prefix + "/" + LockName + ".lock";

        try {
            auto* actorSystem = ctx.ExecutorThread.ActorSystem;
            auto selfId = SelfId();
            YT_UNUSED_FUTURE(Transaction->CreateNode(
                lockNode,
                NYT::NObjectClient::EObjectType::StringNode,
                NYT::NApi::TCreateNodeOptions())
                .Apply(BIND([actorSystem, selfId](const NYT::TErrorOr<NYT::NCypressClient::TNodeId>& result) {
                    actorSystem->Send(selfId, new TEvCreateNodeResponse(0, result));
                })));
        } catch (...) {
            Finish(ctx);
        }
    }

    NYT::NApi::ITransactionPtr Transaction;

    const TActorId LockActorId;
    const TActorId ParentId;
    const TString Prefix;
    const TString LockName;
    const NYT::TNode Attributes;

    const TString Revision;
    ui32 Epoch;
    int AttributesCount = 0;

    TActorId YtWrapper;
};

class TYtLock: public TRichActor<TYtLock> {
public:
    static constexpr char ActorName[] = "LOCK";

    TYtLock(
        const TActorId& ytWrapper,
        const TString& prefix,
        const TString& lockName,
        const TString& lockAttributes,
        bool temporary)
        : TRichActor<TYtLock>(&TYtLock::Handler)
        , YtWrapper(ytWrapper)
        , Prefix(prefix)
        , LockName(lockName)
        , Attributes(NYT::NodeFromYsonString(lockAttributes))
        , Temporary(temporary)
        , Revision(GetProgramCommitId())
    { }

private:
    STRICT_STFUNC(Handler, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        CFunc(TEvTick::EventType, OnTick);
        HFunc(TEvStartTransactionResponse, OnStartTransactionResponse);
        HFunc(TEvCreateNodeResponse, OnCreateNode);
        HFunc(TEvGetNodeResponse, SendBecomeFollower);
        cFunc(TEvBecomeFollower::EventType, OnFollowingForever);
    });

    void DoPassAway() override {
        YQL_CLOG(DEBUG, ProviderDq) << "Unlock " << LockName;
        Send(Request, new TEvents::TEvPoison());
        auto nodePath = Prefix + "/" + LockName;

        if (Temporary) {
            Send(YtWrapper, new TEvRemoveNode(nodePath, {}));
        }
    }

    static TEvStartTransaction* GetStartTransactionCommand() {
        NYT::NApi::TTransactionStartOptions options;
        auto attrs = NYT::NYTree::CreateEphemeralAttributes();
        attrs->Set("title", TStringBuilder{} << "Host name: " << HostName() << " Pid: " << GetPID());
        options.Attributes = attrs;
        auto command = new TEvStartTransaction(
            NYT::NTransactionClient::ETransactionType::Master,
            options
        );
        return command;
    }

    void TryLock() {
        NYT::NApi::TCreateNodeOptions options;
        options.Recursive = true;
        options.IgnoreExisting = true;

        TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());

        IEventBase* event;
        if (FollowingMode == false) {
            event = new TEvCreateNode(Prefix, NYT::NObjectClient::EObjectType::MapNode, options);
        } else {
            event = new TEvGetNode(Prefix + "/" + LockName + "/@", NYT::NApi::TGetNodeOptions());
        }

        TActivationContext::Schedule(
            TDuration::Seconds(5),
            new IEventHandle(YtWrapper, SelfId(), event, 0), TimerCookieHolder.Get());
    }

    void OnFollowingForever() {
        FollowingMode = true;
        YQL_CLOG(DEBUG, ProviderDq) << "Unlock and follow " << LockName;
        Send(Request, new TEvents::TEvPoison());
    }

    void SendBecomeFollower(TEvGetNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            Send(ParentId, new TEvBecomeFollower(result.ValueOrThrow().ToString()));
        } else {
            YQL_CLOG(WARN, ProviderDq) << "SendBecomeFollower error: " << ToString(result);
        }

        TryLock(); // update leader info
    }

    void OnCreateNode(TEvCreateNodeResponse::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            Send(YtWrapper, GetStartTransactionCommand());
        } else {
            TryLock();
        }
    }

    void OnStartTransactionResponse(TEvStartTransactionResponse::TPtr& ev, const TActorContext& ctx)
    {
        auto result = std::get<0>(*ev->Get());

        if (result.IsOK()) {
            Send(Request, new TEvents::TEvPoison());
            auto requestActor = new TLockRequest(
                result.Value(),
                YtWrapper,
                SelfId(),
                ParentId,
                Prefix,
                LockName,
                Attributes
            );

            Request = ctx.Register(requestActor);
        } else {
            YQL_CLOG(WARN, ProviderDq) << "OnStartTransactionResponse " << ToString(result);

            TryLock();
        }
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        ParentId = parentId;
        return new IEventHandle(self, parentId, new TEvTick(), 0);
    }

    void OnTick(const TActorContext& ctx) {
        Y_UNUSED(ctx);

        TryLock();
    }

    const TActorId YtWrapper;
    TActorId ParentId;
    const TString Prefix;
    const TString LockName;
    NYT::TNode Attributes;

    TActorId Request;
    const bool Temporary;

    const TString Revision;
    NActors::TSchedulerCookieHolder TimerCookieHolder;
    bool FollowingMode = false;
};

NActors::IActor* CreateYtLock(
    NActors::TActorId ytWrapper,
    const TString& prefix,
    const TString& lockName,
    const TString& lockAttributesYson,
    bool temporary)
{
    return new TYtLock(ytWrapper, prefix, lockName, lockAttributesYson, temporary);
}

} // namespace NYql
