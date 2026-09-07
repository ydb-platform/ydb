#pragma once

#include "public.h"

#include <contrib/libs/grpc/include/grpcpp/server_builder.h>

#include <util/datetime/base.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TKeepAliveOption final: public grpc::ServerBuilderOption
{
    class TMutator;

private:
    std::unique_ptr<TMutator> Mutator;

public:
    TKeepAliveOption(
        TDuration idleTimeout,
        TDuration probeTimeout,
        ui32 probesCount);
    ~TKeepAliveOption();

    void UpdateArguments(grpc::ChannelArguments* args) override;
    void UpdatePlugins(
        std::vector<std::unique_ptr<grpc::ServerBuilderPlugin>>* plugins)
        override;
};

}   // namespace NYdb::NBS
