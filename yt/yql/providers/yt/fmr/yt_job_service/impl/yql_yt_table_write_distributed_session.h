#pragma once

#include <yt/cpp/mapreduce/interface/distributed_session.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_write_distributed_session.h>

#include <util/system/thread.h>
#include <util/system/event.h>

#include <atomic>

namespace NYql::NFmr {

struct TTableWriteDistributedSessionOptions {
    TDuration PingInterval = TDuration::Seconds(1);
};

class TTableWriteDistributedSession : public IWriteDistributedSession {
public:
    TTableWriteDistributedSession(
        NYT::TDistributedWriteTableSession session,
        TVector<NYT::TDistributedWriteTableCookie>&& cookies,
        const TTableWriteDistributedSessionOptions& options,
        const TClusterConnection& clusterConnection);

    ~TTableWriteDistributedSession();

    TString GetId() const override;
    std::vector<TString> GetCookies() const override;
    void Finish(
        const std::vector<TString>& fragmentResultsYson) override;

private:
    void Ping();
    void PingThreadFunc();

private:
    struct TPingState {
        std::atomic<bool> Stop{false};
        THolder<TThread> Thread;
    };

    NYT::TDistributedWriteTableSession Session_;
    TVector<NYT::TDistributedWriteTableCookie> Cookies_;
    TTableWriteDistributedSessionOptions Options_;
    const TClusterConnection ClusterConnection_;

    TPingState PingState_;
};

} // namespace NYql::NFmr

