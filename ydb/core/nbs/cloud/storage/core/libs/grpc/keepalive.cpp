#include "keepalive.h"

#include <contrib/libs/grpc/src/core/lib/iomgr/socket_mutator.h>

#include <util/generic/singleton.h>
#include <util/network/socket.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TKeepAliveOption::TMutator: public grpc_socket_mutator
{
    struct TVTable: public grpc_socket_mutator_vtable
    {
        TVTable()
        {
            mutate_fd = &TMutator::Mutate;
            compare = &TMutator::Compare;
            destroy = &TMutator::Destroy;
        }
    };

private:
    TDuration IdleTimeout;
    TDuration ProbeTimeout;
    ui32 ProbesCount;

public:
    TMutator(TDuration idleTimeout, TDuration probeTimeout, ui32 probesCount)
        : IdleTimeout(idleTimeout)
        , ProbeTimeout(probeTimeout)
        , ProbesCount(probesCount)
    {
        grpc_socket_mutator_init(this, Singleton<TVTable>());
    }

    auto ConvertToTuple() const
    {
        return std::make_tuple(IdleTimeout, ProbeTimeout, ProbesCount);
    }

private:
    static bool Mutate(int fd, grpc_socket_mutator* mutator)
    {
        TMutator* m = static_cast<TMutator*>(mutator);

        if (SetSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE, 1) != 0) {
            return false;
        }

#if defined(__linux__)
        ui32 idleTimeout = m->IdleTimeout.Seconds();
        if (idleTimeout &&
            (SetSockOpt(fd, IPPROTO_TCP, TCP_KEEPIDLE, idleTimeout) != 0))
        {
            return false;
        }

        ui32 probeTimeout = m->ProbeTimeout.Seconds();
        if (probeTimeout &&
            (SetSockOpt(fd, IPPROTO_TCP, TCP_KEEPINTVL, probeTimeout) != 0))
        {
            return false;
        }

        if (m->ProbesCount &&
            (SetSockOpt(fd, IPPROTO_TCP, TCP_KEEPCNT, m->ProbesCount) != 0))
        {
            return false;
        }
#endif
        return true;
    }

    static int Compare(grpc_socket_mutator* a, grpc_socket_mutator* b)
    {
        auto xa = static_cast<TMutator*>(a)->ConvertToTuple();
        auto xb = static_cast<TMutator*>(b)->ConvertToTuple();
        return xa < xb ? -1 : xa > xb ? 1 : 0;
    }

    static void Destroy(grpc_socket_mutator* mutator)
    {
        TMutator* m = static_cast<TMutator*>(mutator);
        delete m;
    }
};

////////////////////////////////////////////////////////////////////////////////

TKeepAliveOption::TKeepAliveOption(
    TDuration idleTimeout,
    TDuration probeTimeout,
    ui32 probesCount)
    : Mutator(new TMutator(idleTimeout, probeTimeout, probesCount))
{}

TKeepAliveOption::~TKeepAliveOption()
{}

void TKeepAliveOption::UpdateArguments(grpc::ChannelArguments* args)
{
    args->SetSocketMutator(Mutator.release());
}

void TKeepAliveOption::UpdatePlugins(
    std::vector<std::unique_ptr<grpc::ServerBuilderPlugin>>* plugins)
{
    Y_UNUSED(plugins);
}

}   // namespace NYdb::NBS
