#include "local_proxy.h"
#include "local_proxy_request.h"
#include "logging.h"

#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NReplication {

TLocalProxyActor::TLocalProxyActor(const TString& database)
    : Database(database)
{
}

void TLocalProxyActor::Bootstrap() {
    LogPrefix = TStringBuilder() << "[" << SelfId() << ":" << Database << "] ";
    Become(&TLocalProxyActor::StateWork);
}

ui64 TLocalProxyActor::GetChannelBufferSize() const {
    return Max<ui32>();
}

TActorId TLocalProxyActor::RegisterActor(IActor* actor) const {
    return RegisterWithSameMailbox(actor);
}

TString TLocalProxyActor::MakeLocalPath(TString path) const {
    if (path.StartsWith(Database + "/")) {
        return path.substr(Database.length() + 1);
    }

    return path;
}

class TCaptureContext {
    const TActorSystem* ActorSystem;
    const TActorId Sender;
    const ui64 Cookie;

public:
    explicit TCaptureContext(const TActorId& sender, ui64 cookie = 0)
        : ActorSystem(TlsActivationContext->ActorSystem())
        , Sender(sender)
        , Cookie(cookie)
    {}

    virtual ~TCaptureContext() = default;

    virtual void OnResponse(Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* message) = 0;

protected:
    void Send(IEventBase* ev) {
        ActorSystem->Send(Sender, ev, 0, Cookie);
    }
};

template <typename T>
auto CreateCallback(std::shared_ptr<T>&& ctx) {
    return [ctx = std::move(ctx)](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* message) {
        ctx->OnResponse(statusCode, message);
    };
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvAlterTopicRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto [path, settings] = std::move(ev->Get()->GetArgs());
    path = MakeLocalPath(path);

    auto request = std::make_unique<Ydb::Topic::AlterTopicRequest>();
    request->set_path(TStringBuilder() << Database << "/" << path);

    for (const auto& consumer : settings.AddConsumers_) {
        request->add_add_consumers()->set_name(consumer.ConsumerName_);
    }

    for (const auto& consumerName : settings.DropConsumers_) {
        request->add_drop_consumers(consumerName);
    }

    class TAlterTopicContext: public TCaptureContext {
    public:
        using TCaptureContext::TCaptureContext;

        void OnResponse(Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message*) override {
            NYdb::NIssue::TIssues issues;
            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
            Send(new TEvYdbProxy::TEvAlterTopicResponse(std::move(status)));
        }
    };

    auto cb = CreateCallback(std::make_shared<TAlterTopicContext>(ev->Sender, ev->Cookie));
    NGRpcService::DoAlterTopicRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), cb), *this);
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto [path, _] = std::move(ev->Get()->GetArgs());
    path = MakeLocalPath(path);

    auto request = std::make_unique<Ydb::Topic::DescribeTopicRequest>();
    request->set_path(TStringBuilder() << Database << "/" << path);

    class TDescribeTopicContext: public TCaptureContext {
    public:
        using TCaptureContext::TCaptureContext;

        void OnResponse(Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* message) override {
            NYdb::NIssue::TIssues issues;
            Ydb::Topic::DescribeTopicResult describe;
            if (statusCode == Ydb::StatusIds::SUCCESS) {
                const auto* v = dynamic_cast<const Ydb::Topic::DescribeTopicResult*>(message);
                if (v) {
                    describe = *v;
                } else {
                    statusCode = Ydb::StatusIds::INTERNAL_ERROR;
                    issues.AddIssue(TStringBuilder() << "Unexpected result type " << message->GetTypeName());
                }
            }

            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
            NYdb::NTopic::TDescribeTopicResult result(std::move(status), std::move(describe));
            Send(new TEvYdbProxy::TEvDescribeTopicResponse(std::move(result)));
        }
    };

    auto cb = CreateCallback(std::make_shared<TDescribeTopicContext>(ev->Sender, ev->Cookie));
    NGRpcService::DoDescribeTopicRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), cb), *this);
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto [path, _] = std::move(ev->Get()->GetArgs());
    path = MakeLocalPath(path);

    auto request = std::make_unique<Ydb::Scheme::DescribePathRequest>();
    request->set_path(TStringBuilder() << Database << "/" << path);

    class TDescribePathContext: public TCaptureContext {
    public:
        using TCaptureContext::TCaptureContext;

        void OnResponse(Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* message) override {
            NYdb::NIssue::TIssues issues;
            NYdb::NScheme::TSchemeEntry entry;
            if (statusCode == Ydb::StatusIds::SUCCESS) {
                const auto* v = dynamic_cast<const Ydb::Scheme::DescribePathResult*>(message);
                if (v) {
                    entry = v->self();
                } else {
                    statusCode = Ydb::StatusIds::INTERNAL_ERROR;
                    issues.AddIssue(TStringBuilder() << "Unexpected result type " << message->GetTypeName());
                }
            }

            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
            NYdb::NScheme::TDescribePathResult result(std::move(status), std::move(entry));
            Send(new TEvYdbProxy::TEvDescribePathResponse(std::move(result)));
        }
    };

    auto cb = CreateCallback(std::make_shared<TDescribePathContext>(ev->Sender, ev->Cookie));
    NGRpcService::DoDescribePathRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), cb), *this);
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvDescribeTableRequest::TPtr& ev) {
    LOG_E("Handle " << ev->Get()->ToString());

    auto [path, settings] = std::move(ev->Get()->GetArgs());

    NYdb::NIssue::TIssues issues;
    issues.AddIssue(TStringBuilder() << "Table '" << path << "' not found");
    NYdb::TStatus status(NYdb::EStatus::NOT_FOUND, std::move(issues));
    Ydb::Table::DescribeTableResult desc;

    Send(ev->Sender, new TEvYdbProxy::TEvDescribeTableResponse(std::move(status), std::move(desc), settings), 0, ev->Cookie);
}

STATEFN(TLocalProxyActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvYdbProxy::TEvCreateTopicReaderRequest, Handle);
        hFunc(TEvYdbProxy::TEvAlterTopicRequest, Handle);
        hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);
        hFunc(TEvYdbProxy::TEvDescribeTableRequest, Handle);
        hFunc(TEvYdbProxy::TEvDescribeTopicRequest, Handle);
        hFunc(TEvYdbProxy::TEvDescribePathRequest, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    default:
        Y_DEBUG_ABORT_S(TStringBuilder() << "Unhandled message " << ev->GetTypeName());
    }
}

IActor* CreateLocalYdbProxy(const TString& database) {
    return new TLocalProxyActor(database);
}

}
