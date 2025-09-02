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

void TLocalProxyActor::Handle(TEvYdbProxy::TEvAlterTopicRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto args = std::move(ev->Get()->GetArgs());
    auto& path = std::get<TString>(args);
    auto& settings = std::get<NYdb::NTopic::TAlterTopicSettings>(args);

    auto request = std::make_unique<Ydb::Topic::AlterTopicRequest>();
    request.get()->set_path(TStringBuilder() << "/" << Database << path);
    for (auto& c : settings.AddConsumers_) {
        auto* consumer = request.get()->add_add_consumers();
        consumer->set_name(c.ConsumerName_);
    }
    for (auto& c : settings.DropConsumers_) {
        auto* consumer = request.get()->add_drop_consumers();
        *consumer = c;
    }

    auto callback = [replyTo = ev->Sender, cookie = ev->Cookie, path = path, this](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message*) {
        NYdb::NIssue::TIssues issues;
        NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
        Send(replyTo, new TEvYdbProxy::TEvAlterTopicResponse(std::move(status)), 0, cookie);
    };

    NGRpcService::DoAlterTopicRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), callback), *this);
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto args = std::move(ev->Get()->GetArgs());
    auto& path = std::get<TString>(args);
    auto& settings = std::get<NYdb::NTopic::TDescribeTopicSettings>(args);
    Y_UNUSED(settings);

    auto request = std::make_unique<Ydb::Topic::DescribeTopicRequest>();
    request.get()->set_path(TStringBuilder() << "/" << Database << path);

    auto callback = [replyTo = ev->Sender, cookie = ev->Cookie, path = path, this](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* result) {
        NYdb::NIssue::TIssues issues;
        Ydb::Topic::DescribeTopicResult describe;
        if (statusCode == Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS) {
            const auto* v = dynamic_cast<const Ydb::Topic::DescribeTopicResult*>(result);
            if (v) {
                describe = *v;
            } else {
                statusCode = Ydb::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR;
                issues.AddIssue(TStringBuilder() << "Unexpected result type " << result->GetTypeName());
            }
        }

        NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
        NYdb::NTopic::TDescribeTopicResult r(std::move(status), std::move(describe));
        Send(replyTo, new TEvYdbProxy::TEvDescribeTopicResponse(r), 0, cookie);
    };

    NGRpcService::DoDescribeTopicRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), callback), *this);
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto args = std::move(ev->Get()->GetArgs());
    auto& path = std::get<TString>(args);
    auto& settings = std::get<NYdb::NScheme::TDescribePathSettings>(args);
    Y_UNUSED(settings);

    auto request = std::make_unique<Ydb::Scheme::DescribePathRequest>();
    request.get()->set_path(TStringBuilder() << "/" << Database << path);

    auto callback = [replyTo = ev->Sender, cookie = ev->Cookie, path = path, this](Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message* result) {
        NYdb::NIssue::TIssues issues;
        NYdb::NScheme::TSchemeEntry entry;
        if (statusCode == Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS) {
            const auto* v = dynamic_cast<const Ydb::Scheme::DescribePathResult*>(result);
            if (v) {
                entry = v->self();
            } else {
                statusCode = Ydb::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR;
                issues.AddIssue(TStringBuilder() << "Unexpected result type " << result->GetTypeName());
            }
        }

        NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
        NYdb::NScheme::TDescribePathResult r(std::move(status), std::move(entry));
        Send(replyTo, new TEvYdbProxy::TEvDescribePathResponse(r), 0, cookie);
    };

    NGRpcService::DoDescribePathRequest(std::make_unique<TLocalProxyRequest>(path, Database, std::move(request), callback), *this);
}

void TLocalProxyActor::Handle(TEvYdbProxy::TEvDescribeTableRequest::TPtr& ev) {
    LOG_E("Handle " << ev->Get()->ToString());

    auto [table, settings] = std::move(ev->Get()->GetArgs());

    NYdb::NIssue::TIssues issues;
    issues.AddIssue(TStringBuilder() << "Table '" << table << "' not found");
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
