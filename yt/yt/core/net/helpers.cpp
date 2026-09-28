#include "helpers.h"

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/socket.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> AllocateFreePorts(
    int portCount,
    const THashSet<int>& availablePorts,
    const NLogging::TLogger& logger)
{
    if (portCount == 0) {
        return {};
    }

    const auto& Logger = logger;

    // Here goes our best effort to make sure we provide free ports to user job.
    // No doubt there may still be race conditions in which user job will still not be
    // able to bind to the port, but it should happen pretty rarely.
    std::vector<int> allocatedPorts;

    for (int port : availablePorts) {
        SOCKET socket = INVALID_SOCKET;

        try {
            socket = CreateTcpServerSocket();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while creating a socket for preliminary port bind")
                .With(ex);
        }

        YT_VERIFY(socket != INVALID_SOCKET);

        try {
            YT_TLOG_DEBUG("Making a preliminary port bind")
                .With("Port", port)
                .With("Socket", socket);
            BindSocket(socket, TNetworkAddress::CreateIPv6Any(port));
        } catch (const std::exception& ex) {
            SafeClose(socket, false /*ignoreBadFD*/);
            YT_TLOG_DEBUG("Error while trying making a preliminary port bind, skipping it")
                .With("Port", port)
                .With("Socket", socket)
                .With(ex);
            continue;
        }

        SafeClose(socket, false /*ignoreBadFD*/);
        YT_TLOG_DEBUG("Socket used in preliminary bind is closed")
            .With("Port", port)
            .With("Socket", socket);

        allocatedPorts.push_back(port);

        if (std::ssize(allocatedPorts) >= portCount) {
            break;
        }
    }

    YT_VERIFY(std::ssize(allocatedPorts) <= portCount);

    return allocatedPorts;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
