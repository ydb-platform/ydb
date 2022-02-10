#include "connection.h"

#include "remote_client_connection.h"

#include <util/generic/cast.h>

using namespace NBus;
using namespace NBus::NPrivate;

void TBusClientConnectionPtrOps::Ref(TBusClientConnection* c) {
    return CheckedCast<TRemoteClientConnection*>(c)->Ref();
}

void TBusClientConnectionPtrOps::UnRef(TBusClientConnection* c) {
    return CheckedCast<TRemoteClientConnection*>(c)->UnRef();
}
