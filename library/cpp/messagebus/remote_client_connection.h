#pragma once

#include "connection.h"
#include "local_tasks.h"
#include "remote_client_session.h"
#include "remote_connection.h"

#include <util/generic/object_counter.h>

namespace NBus {
    namespace NPrivate {
        class TRemoteClientConnection: public TRemoteConnection, public TBusClientConnection {
            friend class TRemoteConnection;
            friend struct TBusSessionImpl;
            friend class TRemoteClientSession;

        private:
            TObjectCounter<TRemoteClientConnection> ObjectCounter;

            TSyncAckMessages AckMessages;

            TLocalTasks TimeToTimeoutMessages;

            IBusClientHandler* const ClientHandler;

        public:
            TRemoteClientConnection(TRemoteClientSessionPtr session, ui64 id, TNetAddr addr);

            inline TRemoteClientSession* GetSession();

            SOCKET CreateSocket(const TNetAddr& addr);

            void TryConnect() override;

            void HandleEvent(SOCKET socket, void* cookie) override;

            TBusMessage* PopAck(TBusKey id);

            void WriterFillStatus() override;

            void ClearOutgoingQueue(TMessagesPtrs& result, bool reconnect) override;

            void BeforeTryWrite() override;

            void ProcessReplyQueue();

            void MessageSent(TArrayRef<TBusMessagePtrAndHeader> messages) override;

            void TimeoutMessages();

            void ScheduleTimeoutMessages();

            void ReaderProcessMessageUnknownVersion(TArrayRef<const char> dataRef) override;

            EMessageStatus SendMessage(TBusMessage* pMes, bool wait) override;

            EMessageStatus SendMessageOneWay(TBusMessage* pMes, bool wait) override;

            EMessageStatus SendMessageImpl(TBusMessage*, bool wait, bool oneWay);

            void OpenConnection() override;
        };

    }
}
