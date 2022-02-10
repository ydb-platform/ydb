#pragma once

#include "session_impl.h"

#include <util/generic/object_counter.h>

namespace NBus {
    namespace NPrivate {
        class TRemoteServerConnection: public TRemoteConnection {
            friend struct TBusSessionImpl;
            friend class TRemoteServerSession;

            TObjectCounter<TRemoteServerConnection> ObjectCounter;

        public:
            TRemoteServerConnection(TRemoteServerSessionPtr session, ui64 id, TNetAddr addr);

            void Init(SOCKET socket, TInstant now);

            inline TRemoteServerSession* GetSession();

            void HandleEvent(SOCKET socket, void* cookie) override;

            bool NeedInterruptRead() override;

            void MessageSent(TArrayRef<TBusMessagePtrAndHeader> messages) override;

            void ReaderProcessMessageUnknownVersion(TArrayRef<const char> dataRef) override;
        };

    }
}
