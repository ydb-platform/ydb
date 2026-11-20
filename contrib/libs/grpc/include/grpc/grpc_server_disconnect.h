#ifndef GRPC_GRPC_SERVER_DISCONNECT_H
#define GRPC_GRPC_SERVER_DISCONNECT_H

#include <grpc/impl/grpc_types.h>

#ifdef __cplusplus
extern "C" {
#endif

/** YDB extension. Initiate a forced disconnect of all transports currently
    attached to this server, including idle connections and active calls.
    The server keeps accepting connections. Transports attached after the
    snapshot are unaffected. */
GRPCAPI void grpc_server_disconnect_clients(grpc_server* server);

#ifdef __cplusplus
}
#endif

#endif /* GRPC_GRPC_SERVER_DISCONNECT_H */
