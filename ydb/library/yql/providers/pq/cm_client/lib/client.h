#pragma once

#include <ydb/library/yql/providers/pq/cm_client/interface/client.h>

namespace NPq::NConfigurationManager {

struct TConnections : public IConnections {
    class TImpl;

    explicit TConnections(size_t numWorkerThreads = 2);
    ~TConnections();

    void Stop(bool wait = false) override;

    IClient::TPtr GetClient(const TClientOptions& = {}) override;

private:
    TIntrusivePtr<TImpl> Impl;
};

} // namespace NPq::NConfigurationManager
