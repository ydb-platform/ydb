#include "yql_yt_table_write_distributed_session.h"

#include <library/cpp/yson/node/node_io.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>
#include <yql/essentials/utils/log/log.h>

#include <util/datetime/base.h>

namespace NYql::NFmr {

TTableWriteDistributedSession::TTableWriteDistributedSession(
    NYT::TDistributedWriteTableSession session,
    TVector<NYT::TDistributedWriteTableCookie>&& cookies,
    const TTableWriteDistributedSessionOptions& options,
    const TClusterConnection& clusterConnection)
    : Session_(std::move(session))
    , Cookies_(std::move(cookies))
    , Options_(options)
    , ClusterConnection_(clusterConnection)
{
    PingState_.Thread = MakeHolder<TThread>([this]() {
        PingThreadFunc();
    });
    PingState_.Thread->Start();
}

TTableWriteDistributedSession::~TTableWriteDistributedSession() {
    PingState_.Stop.store(true);
    if (PingState_.Thread) {
        PingState_.Thread->Join();
    }
}

void TTableWriteDistributedSession::PingThreadFunc() {
    while (!PingState_.Stop.load()) {
        try {
            Ping();
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "Error pinging distributed write table session";
        }
        Sleep(Options_.PingInterval);
    }
}

TString TTableWriteDistributedSession::GetId() const {
    return NYT::NodeToYsonString(static_cast<const NYT::TNode&>(Session_), NYson::EYsonFormat::Text);
}

std::vector<TString> TTableWriteDistributedSession::GetCookies() const {
    std::vector<TString> result;
    result.reserve(Cookies_.size());
    for (const auto& cookie : Cookies_) {
        result.push_back(NYT::NodeToYsonString(static_cast<const NYT::TNode&>(cookie), NYson::EYsonFormat::Text));
    }
    return result;
}

void TTableWriteDistributedSession::Ping() {
    auto client = CreateClient(ClusterConnection_);
    client->PingDistributedWriteTableSession(Session_);
}

void TTableWriteDistributedSession::Finish(
    const std::vector<TString>& fragmentResultsYson)
{
    auto client = CreateClient(ClusterConnection_);

    TVector<NYT::TWriteTableFragmentResult> results;
    results.reserve(fragmentResultsYson.size());
    for (const auto& yson : fragmentResultsYson) {
        NYT::TNode outer = NYT::NodeFromYsonString(yson);
        NYT::TNode fragmentNode;
        if (outer.GetType() == NYT::TNode::EType::String) {
            fragmentNode = NYT::NodeFromYsonString(outer.AsString());
        } else {
            fragmentNode = std::move(outer);
        }
        results.push_back(NYT::TWriteTableFragmentResult(std::move(fragmentNode)));
    }

    PingState_.Stop.store(true);
    if (PingState_.Thread) {
        PingState_.Thread->Join();
    }

    client->FinishDistributedWriteTableSession(Session_, results);
}
} // namespace NYql::NFmr

