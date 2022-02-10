#pragma once

// unique tag to fix pragma once gcc glueing: ./library/cpp/messagebus/defs.h

#include "codegen.h"
#include "netaddr.h"

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <util/generic/list.h>

#include <utility>

// For historical reasons TCrawlerModule need to access
// APIs that should be private.
class TCrawlerModule;

struct TDebugReceiverHandler;

namespace NBus {
    namespace NPrivate {
        class TAcceptor;
        struct TBusSessionImpl;
        class TRemoteServerSession;
        class TRemoteClientSession;
        class TRemoteConnection;
        class TRemoteServerConnection;
        class TRemoteClientConnection;
        class TBusSyncSourceSessionImpl;

        struct TBusMessagePtrAndHeader;

        struct TSessionDumpStatus;

        struct TClientRequestImpl;

    }

    class TBusSession;
    struct TBusServerSession;
    struct TBusClientSession;
    class TBusProtocol;
    class TBusMessage;
    class TBusMessageConnection;
    class TBusMessageQueue;
    class TBusLocator;
    struct TBusQueueConfig;
    struct TBusSessionConfig;
    struct TBusHeader;

    class IThreadHandler;

    using TBusKey = ui64;
    using TBusMessageList = TList<TBusMessage*>;
    using TBusKeyVec = TVector<std::pair<TBusKey, TBusKey>>;

    using TBusMessageQueuePtr = TIntrusivePtr<TBusMessageQueue>;

    class TBusModule;

    using TBusData = TString;
    using TBusService = const char*;

#define YBUS_KEYMIN TBusKey(0L)
#define YBUS_KEYMAX TBusKey(-1L)
#define YBUS_KEYLOCAL TBusKey(7L)
#define YBUS_KEYINVALID TBusKey(99999999L)

    // Check that generated id is valid for remote message
    inline bool IsBusKeyValid(TBusKey key) {
        return key != YBUS_KEYINVALID && key != YBUS_KEYMAX && key > YBUS_KEYLOCAL;
    }

#define YBUS_VERSION 0

#define YBUS_INFINITE (1u << 30u)

#define YBUS_STATUS_BASIC 0x0000
#define YBUS_STATUS_CONNS 0x0001
#define YBUS_STATUS_INFLIGHT 0x0002

}
