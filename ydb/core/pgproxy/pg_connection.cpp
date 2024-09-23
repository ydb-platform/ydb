#include "pg_connection.h"
#include "pg_proxy_types.h"
#include "pg_proxy_events.h"
#include "pg_stream.h"
#include "pg_log_impl.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/raw_socket/sock_config.h>

namespace NPG {

using namespace NActors;

class TPGConnection : public TActorBootstrapped<TPGConnection>, public TNetworkConfig {
public:
    using TBase = TActorBootstrapped<TPGConnection>;
    const TActorId ListenerActorId;
    TIntrusivePtr<TSocketDescriptor> Socket;
    TSocketAddressType Address;
    THPTimer InactivityTimer;
    static constexpr TDuration InactivityTimeout = TDuration::Minutes(10);
    static constexpr uint32_t BACKEND_DATA_MASK = 0x55aa55aa;
    TEvPollerReady* InactivityEvent = nullptr;
    bool IsAuthRequired = true;
    bool IsSslSupported = true;
    bool ConnectionEstablished = false;
    bool CloseConnection = false;
    bool PasswordWasSupplied = false;
    TPollerToken::TPtr PollerToken;
    TSocketBuffer BufferInput;
    std::size_t MAX_BUFFER_SIZE = 2 * 1024 * 1024;
    std::unordered_map<TString, TString> ServerParams = {
        {"client_encoding", "UTF8"},
        {"server_encoding", "UTF8"},
        {"DateStyle", "ISO"},
        {"IntervalStyle", "postgres"},
        {"integer_datetimes", "on"},
        {"server_version", "14.5 (ydb stable-23-4)"},
    };
    TSocketBuffer BufferOutput;
    TActorId DatabaseProxy;
    std::shared_ptr<TPGInitial> InitialMessage;
    ui64 IncomingSequenceNumber = 1;
    ui64 OutgoingSequenceNumber = 1;
    ui64 SyncSequenceNumber = 1;
    char TransactionStatus = 'I'; // could be 'I' (idle), 'T' (transaction), 'E' (failed transaction)
    std::deque<TAutoPtr<IEventHandle>> PostponedEvents;

    TPGConnection(const TActorId& listenerActorId, TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address, const TActorId& databaseProxy)
        : ListenerActorId(listenerActorId)
        , Socket(std::move(socket))
        , Address(address)
        , DatabaseProxy(databaseProxy)
    {
        SetNonBlock();
        IsSslSupported = IsSslSupported && Socket->IsSslSupported();
    }

    void Bootstrap() {
        Become(&TPGConnection::StateAccepting);
        Schedule(InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        BLOG_D("incoming connection opened");
        OnAccept();
    }

    void PassAway() override {
        //ctx.Send(Endpoint->Owner, new TEvHttpProxy::TEvHttpConnectionClosed(ctx.SelfID, std::move(RecycledRequests)));
        if (ConnectionEstablished) {
            Send(DatabaseProxy, new TEvPGEvents::TEvConnectionClosed(), 0, IncomingSequenceNumber);
            ConnectionEstablished = false;
        }
        Send(ListenerActorId, new TEvents::TEvUnsubscribe());
        Shutdown();
        TBase::PassAway();
    }

protected:
    void SetNonBlock() noexcept {
        Socket->SetNonBlock();
    }

    void Shutdown() {
        if (Socket) {
            Socket->Shutdown();
        }
    }

    ssize_t SocketSend(const void* data, size_t size) {
        return Socket->Send(data, size);
    }

    ssize_t SocketReceive(void* data, size_t size) {
        return Socket->Receive(data, size);
    }

    void RequestPoller() {
        Socket->RequestPoller(PollerToken);
    }

    SOCKET GetRawSocket() const {
        return Socket->GetRawSocket();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "(#" << GetRawSocket() << "," << Address << ") ";
    }

    void OnAccept() {
        InactivityTimer.Reset();
        TBase::Become(&TPGConnection::StateConnected);
        BufferInput.Append('i'); // initial packet pseudo-message
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
    }

    void HandleAccepting(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        OnAccept();
    }

    void HandleAccepting(NActors::TEvPollerReady::TPtr) {
        OnAccept();
    }

    enum class EDirection {
        Incoming,
        Outgoing,
    };

    static constexpr TStringBuf GetClassName(TStringBuf name) {
        auto pos = name.find("TPG");
        if (pos == std::string_view::npos) {
            return name;
        }
        return name.substr(pos + 3, name.size() - pos - 4);
    }

    template<typename TPGMsg>
    static constexpr std::pair<char, TStringBuf> GetMessageCode() {
        return {TPGMsg::CODE, GetClassName(typeid(TPGMsg).name())};
    }

    TStringBuf GetMessageName(EDirection direction, const TPGMessage& message) const {
        static const std::unordered_map<char, TStringBuf> incomingMessageName = {
            GetMessageCode<TPGInitial>(),
            GetMessageCode<TPGPasswordMessage>(),
            GetMessageCode<TPGParse>(),
            GetMessageCode<TPGBind>(),
            GetMessageCode<TPGExecute>(),
            GetMessageCode<TPGDescribe>(),
            GetMessageCode<TPGSync>(),
            GetMessageCode<TPGClose>(),
            GetMessageCode<TPGQuery>(),
            GetMessageCode<TPGTerminate>(),
            GetMessageCode<TPGFlush>(),
        };
        static const std::unordered_map<char, TStringBuf> outgoingMessageName = {
            GetMessageCode<TPGAuth>(),
            GetMessageCode<TPGBackendKeyData>(),
            GetMessageCode<TPGErrorResponse>(),
            GetMessageCode<TPGDataRow>(),
            GetMessageCode<TPGParameterStatus>(),
            GetMessageCode<TPGCommandComplete>(),
            GetMessageCode<TPGParseComplete>(),
            GetMessageCode<TPGBindComplete>(),
            GetMessageCode<TPGCloseComplete>(),
            GetMessageCode<TPGParameterDescription>(),
            GetMessageCode<TPGNoData>(),
            GetMessageCode<TPGRowDescription>(),
            GetMessageCode<TPGEmptyQueryResponse>(),
            GetMessageCode<TPGReadyForQuery>(),
        };
        std::unordered_map<char, TStringBuf>::const_iterator itMessageName;
        switch (direction) {
            case EDirection::Incoming:
                itMessageName = incomingMessageName.find(message.Message);
                if (itMessageName != incomingMessageName.end()) {
                    return itMessageName->second;
                }
                break;
            case EDirection::Outgoing:
                itMessageName = outgoingMessageName.find(message.Message);
                if (itMessageName != outgoingMessageName.end()) {
                    return itMessageName->second;
                }
                break;
        }
        return "?";
    }

    TString GetMessageDump(EDirection direction, const TPGMessage& message) const {
        if (direction == EDirection::Incoming) {
            switch (message.Message) {
                case TPGInitial::CODE:
                    return ((const TPGInitial&)message).Dump();
                case TPGQuery::CODE:
                    return ((const TPGQuery&)message).Dump();
                case TPGSync::CODE:
                    return ((const TPGSync&)message).Dump();
                case TPGClose::CODE:
                    return ((const TPGClose&)message).Dump();
                case TPGDescribe::CODE:
                    return ((const TPGDescribe&)message).Dump();
                case TPGExecute::CODE:
                    return ((const TPGExecute&)message).Dump();
                case TPGBind::CODE:
                    return ((const TPGBind&)message).Dump();
                case TPGParse::CODE:
                    return ((const TPGParse&)message).Dump();

            }
        } else {
            switch (message.Message) {
                case TPGParameterStatus::CODE:
                    return ((const TPGParameterStatus&)message).Dump();
                case TPGBackendKeyData::CODE:
                    return ((const TPGBackendKeyData&)message).Dump();
                case TPGReadyForQuery::CODE:
                    return ((const TPGReadyForQuery&)message).Dump();
                case TPGCommandComplete::CODE:
                    return ((const TPGCommandComplete&)message).Dump();
                case TPGAuth::CODE:
                    return ((const TPGAuth&)message).Dump();
                case TPGDataRow::CODE:
                    return ((const TPGDataRow&)message).Dump();
                case TPGErrorResponse::CODE:
                    return ((const TPGErrorResponse&)message).Dump();
            }
        }
        return {};
    }

    void PrintMessage(EDirection direction, const TPGMessage& message) {
        TStringBuilder prefix;
        switch (direction) {
            case EDirection::Incoming:
                prefix << "-> [" << IncomingSequenceNumber << "] ";
                break;
            case EDirection::Outgoing:
                prefix << "<- [" << OutgoingSequenceNumber << "] ";
                break;
        }
        BLOG_D(prefix << "'" << message.Message << "' \"" << GetMessageName(direction, message) << "\" Size(" << message.GetDataSize() << ") " << GetMessageDump(direction, message));
    }

    template<typename TMessage>
    void SendMessage(const TMessage& message) {
        PrintMessage(EDirection::Outgoing, message);
        BufferOutput.Append(reinterpret_cast<const char*>(&message), sizeof(message));
    }

    template<typename TMessage>
    void SendStream(TPGStreamOutput<TMessage>& message) {
        message.UpdateLength();
        const TPGMessage& header = *reinterpret_cast<const TPGMessage*>(message.Data());
        PrintMessage(EDirection::Outgoing, header);
        BufferOutput.Append(message.Data(), message.Size());
    }

    void SendAuthOk() {
        TPGStreamOutput<TPGAuth> authOk;
        authOk << uint32_t(TPGAuth::EAuthCode::OK);
        SendStream(authOk);
    }

    void SendAuthClearText() {
        TPGStreamOutput<TPGAuth> authClearText;
        authClearText << uint32_t(TPGAuth::EAuthCode::ClearText);
        SendStream(authClearText);
    }
    /*
        0x0040:  70ac 68dd 70ac 68dc 5200 0000 0800 0000  p.h.p.h.R.......
        0x0050:  0053 0000 001a 6170 706c 6963 6174 696f  .S....applicatio
        0x0060:  6e5f 6e61 6d65 0070 7371 6c00 5300 0000  n_name.psql.S...
        0x0070:  1963 6c69 656e 745f 656e 636f 6469 6e67  .client_encoding
        0x0080:  0055 5446 3800 5300 0000 1744 6174 6553  .UTF8.S....DateS
        0x0090:  7479 6c65 0049 534f 2c20 4d44 5900 5300  tyle.ISO,.MDY.S.
        0x00a0:  0000 1969 6e74 6567 6572 5f64 6174 6574  ...integer_datet
        0x00b0:  696d 6573 006f 6e00 5300 0000 1b49 6e74  imes.on.S....Int
        0x00c0:  6572 7661 6c53 7479 6c65 0070 6f73 7467  ervalStyle.postg
        0x00d0:  7265 7300 5300 0000 1469 735f 7375 7065  res.S....is_supe
        0x00e0:  7275 7365 7200 6f6e 0053 0000 0019 7365  ruser.on.S....se
        0x00f0:  7276 6572 5f65 6e63 6f64 696e 6700 5554  rver_encoding.UT
        0x0100:  4638 0053 0000 0039 7365 7276 6572 5f76  F8.S...9server_v
        0x0110:  6572 7369 6f6e 0031 322e 3131 2028 5562  ersion.12.11.(Ub
        0x0120:  756e 7475 2031 322e 3131 2d30 7562 756e  untu.12.11-0ubun
        0x0130:  7475 302e 3230 2e30 342e 3129 0053 0000  tu0.20.04.1).S..
        0x0140:  0021 7365 7373 696f 6e5f 6175 7468 6f72  .!session_author
        0x0150:  697a 6174 696f 6e00 616c 6578 6579 0053  ization.alexey.S
        0x0160:  0000 0023 7374 616e 6461 7264 5f63 6f6e  ...#standard_con
        0x0170:  666f 726d 696e 675f 7374 7269 6e67 7300  forming_strings.
        0x0180:  6f6e 0053 0000 0015 5469 6d65 5a6f 6e65  on.S....TimeZone
        0x0190:  0045 7463 2f55 5443 004b 0000 000c 0004  .Etc/UTC.K......
    */
    void SendParameterStatus(TStringBuf name, TStringBuf value) {
        TPGStreamOutput<TPGParameterStatus> param;
        param << name << '\0' << value << '\0';
        SendStream(param);
    }

    void SendReadyForQuery() {
        TPGStreamOutput<TPGReadyForQuery> readyForQuery;
        readyForQuery << TransactionStatus;
        SendStream(readyForQuery);
    }

    void SendAuthError(const TString& error) {
        TPGStreamOutput<TPGErrorResponse> errorResponse;
        errorResponse
            << 'S' << "FATAL" << '\0'
            << 'V' << "FATAL" << '\0'
//            << 'C' << "28P01" << '\0'
            << 'M' << error << '\0'
            << 'R' << "auth_failed" << '\0'
            << '\0';
        SendStream(errorResponse);
    }

    void BecomeReadyForQuery() {
        if (OutgoingSequenceNumber == SyncSequenceNumber) {
            SendReadyForQuery();
            OutgoingSequenceNumber++;
        }
        ReplayPostponedEvents();
        FlushAndPoll();
    }

    bool IsValidBackendData(const TPGInitial::TPGBackendData& backendData) {
        Y_UNUSED(backendData);
        return true;
    }

    void HandleMessage(const TPGInitial* message) {
        uint32_t protocol = message->GetProtocol();
        if (protocol == 0x2f16d204) { // 790024708 SSL handshake
            if (IsSslSupported) {
                BufferOutput.Append('S');
                if (!FlushOutput()) {
                    return;
                }
                // TODO(xenoxeno): wait for reply to be sent
                if (!UpgradeToSecure()) {
                    return;
                }
                RequestPoller();
            } else {
                BLOG_D("<- 'N' \"Decline SSL\"");
                BufferOutput.Append('N');
                if (!FlushOutput()) {
                    return;
                }
                RequestPoller();
            }
            BufferInput.Append('i'); // initial packet pseudo-message
            return;
        }
        if (protocol == 0x2e16d204) { // 80877102 cancellation message
            BLOG_D("cancellation message");
            TPGInitial::TPGBackendData backendData = message->GetBackendData();
            if (IsValidBackendData(backendData)) {
                Send(DatabaseProxy, new TEvPGEvents::TEvCancelRequest(backendData.Pid ^ BACKEND_DATA_MASK, backendData.Key ^ BACKEND_DATA_MASK));
            }
            CloseConnection = true;
            return;
        }
        if (protocol != 0x300) {
            BLOG_W("invalid protocol version (" << Hex(protocol) << ")");
            CloseConnection = true;
            return;
        }
        InitialMessage = MakePGMessageCopy(message);
        if (IsAuthRequired) {
            Send(DatabaseProxy, new TEvPGEvents::TEvAuth(InitialMessage, Address), 0, IncomingSequenceNumber++);
        } else {
            SendAuthOk();
            BecomeConnected();
        }
    }

    void HandleMessage(const TPGPasswordMessage* message) {
        PasswordWasSupplied = true;
        Send(DatabaseProxy, new TEvPGEvents::TEvAuth(InitialMessage, Address, MakePGMessageCopy(message)), 0, IncomingSequenceNumber++);
        return;
    }

    void HandleMessage(const TPGQuery* message) {
        SyncSequenceNumber = IncomingSequenceNumber;
        Send(DatabaseProxy, new TEvPGEvents::TEvQuery(MakePGMessageCopy(message), TransactionStatus), 0, IncomingSequenceNumber++);
    }

    void HandleMessage(const TPGParse* message) {
        Send(DatabaseProxy, new TEvPGEvents::TEvParse(MakePGMessageCopy(message)), 0, IncomingSequenceNumber++);
    }

    void HandleMessage(const TPGSync*) {
        SyncSequenceNumber = IncomingSequenceNumber++;
        if (OutgoingSequenceNumber == SyncSequenceNumber) {
            SendReadyForQuery();
            OutgoingSequenceNumber++;
        }
    }

    void HandleMessage(const TPGBind* message) {
        Send(DatabaseProxy, new TEvPGEvents::TEvBind(MakePGMessageCopy(message)), 0, IncomingSequenceNumber++);
    }

    void HandleMessage(const TPGDescribe* message) {
        Send(DatabaseProxy, new TEvPGEvents::TEvDescribe(MakePGMessageCopy(message)), 0, IncomingSequenceNumber++);
    }

    void HandleMessage(const TPGExecute* message) {
        Send(DatabaseProxy, new TEvPGEvents::TEvExecute(MakePGMessageCopy(message), TransactionStatus), 0, IncomingSequenceNumber++);
    }

    void HandleMessage(const TPGClose* message) {
        Send(DatabaseProxy, new TEvPGEvents::TEvClose(MakePGMessageCopy(message)), 0, IncomingSequenceNumber++);
    }

    void HandleMessage(const TPGTerminate*) {
        CloseConnection = true;
    }

    void HandleMessage(const TPGFlush*) {
    }

    static void FillDataRow(TPGStreamOutput<TPGDataRow>& dataOut, const TEvPGEvents::TDataRow& dataIn) {
        dataOut << uint16_t(dataIn.size()); // number of fields
        for (const auto& item : dataIn) {
            if (item.Value) {
                const auto& value(item.Value.value());
                if (std::holds_alternative<TString>(value)) {
                    const auto& valueText(std::get<TString>(value));
                    dataOut << uint32_t(valueText.size()) << valueText;
                } else {
                    const auto& valueBinary(std::get<std::vector<uint8_t>>(value));
                    dataOut << uint32_t(valueBinary.size()) << valueBinary;
                }
            } else {
                dataOut << uint32_t(-1);
            }
        }
    }

    static void FillDataRowDescription(TPGStreamOutput<TPGRowDescription>& dataOut, const std::vector<TEvPGEvents::TRowDescriptionField>& dataIn) {
        dataOut << uint16_t(dataIn.size()); // number of fields
        for (const auto& field : dataIn) {
            dataOut
                << TStringBuf(field.Name) << '\0'
                << uint32_t(field.TableId)
                << uint16_t(field.ColumnId)
                << uint32_t(field.DataType)
                << uint16_t(field.DataTypeSize)
                << uint32_t(field.DataTypeModifier)
                << uint16_t(field.Format)
                ;
        }
    }

    template<typename TPGResponse>
    static void FillKeyValueResponse(TPGStreamOutput<TPGResponse>& dataOut, const std::vector<std::pair<char, TString>>& dataIn) {
        for (const auto& field : dataIn) {
            dataOut << field.first << field.second << '\0';
        }
        dataOut << '\0';
    }

    void SendNoticeResponse(const std::vector<std::pair<char, TString>>& noticeFields) {
        TPGStreamOutput<TPGNoticeResponse> noticeResponse;
        FillKeyValueResponse(noticeResponse, noticeFields);
        SendStream(noticeResponse);
    }

    void SendErrorResponse(const std::vector<std::pair<char, TString>>& errorFields) {
        TPGStreamOutput<TPGErrorResponse> errorResponse;
        FillKeyValueResponse(errorResponse, errorFields);
        SendStream(errorResponse);
    }

    bool FlushAndPoll() {
        if (FlushOutput()) {
            RequestPoller();
            return true;
        }
        return false;
    }

    void BecomeConnected() {
        SyncSequenceNumber = IncomingSequenceNumber;
        Send(DatabaseProxy, new TEvPGEvents::TEvConnectionOpened(InitialMessage, Address), 0, IncomingSequenceNumber++);
        ConnectionEstablished = true;
    }

    struct TEventsComparator {
        bool operator ()(const TAutoPtr<IEventHandle>& ev1, const TAutoPtr<IEventHandle>& ev2) const {
            return ev1->Cookie < ev2->Cookie;
        }
    };

    template<typename TEvent>
    bool IsEventExpected(const TAutoPtr<TEvent>& ev) {
        return (ev->Cookie == 0) || (ev->Cookie == OutgoingSequenceNumber);
    }

    template<typename TEvent>
    void PostponeEvent(TAutoPtr<TEvent>& ev) {
        BLOG_D("Postpone event " << ev->Cookie);
        TAutoPtr<IEventHandle> evb(ev.Release());
        auto it = std::upper_bound(PostponedEvents.begin(), PostponedEvents.end(), evb, TEventsComparator());
        PostponedEvents.insert(it, evb);
    }

    void ReplayPostponedEvents() {
        if (!PostponedEvents.empty()) {
            TAutoPtr<IEventHandle> event = std::move(PostponedEvents.front());
            PostponedEvents.pop_front();
            StateConnected(event);
        }
    }

    void HandleConnected(TEvPGEvents::TEvAuthResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->Error) {
                if (PasswordWasSupplied) {
                    SendAuthError(ev->Get()->Error);
                    CloseConnection = true;
                } else {
                    SendAuthClearText();
                }
            } else {
                SendAuthOk();
                BecomeConnected();
            }
            ++OutgoingSequenceNumber;
            ReplayPostponedEvents();
            FlushAndPoll();
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvFinishHandshake::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->ErrorFields.empty()) {
                const auto& clientParams(InitialMessage->GetClientParams());
                auto itOptions = clientParams.find("options");
                if (itOptions != clientParams.end()) {
                    TStringBuf options(itOptions->second);
                    TStringBuf token;
                    while (options.NextTok(' ', token) && token == "-c") {
                        TStringBuf option;
                        if (options.NextTok(' ', option)) {
                            TStringBuf name;
                            TStringBuf value;
                            if (option.NextTok('=', name)) {
                                value = option;
                                ServerParams[TString(name)] = TString(value);
                            }
                        }
                    }
                }

                TPGStreamOutput<TPGBackendKeyData> backendKeyData;
                backendKeyData << (ev->Get()->BackendData.Pid ^ BACKEND_DATA_MASK) << (ev->Get()->BackendData.Key ^ BACKEND_DATA_MASK);
                SendStream(backendKeyData);

                for (const auto& [name, value] : ServerParams) {
                    SendParameterStatus(name, value);
                }
                BecomeReadyForQuery();
            } else {
                SendErrorResponse(ev->Get()->ErrorFields);
                BLOG_ERROR("unable to create connection");
                CloseConnection = true;
                FlushAndPoll();
            }
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvQueryResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->TransactionStatus) {
                TransactionStatus = ev->Get()->TransactionStatus;
            }
            if (ev->Get()->ErrorFields.empty()) {
                if (ev->Get()->EmptyQuery) {
                    SendMessage(TPGEmptyQueryResponse());
                } else {
                    if (!ev->Get()->DataFields.empty()) { // rowDescription
                        TPGStreamOutput<TPGRowDescription> rowDescription;
                        FillDataRowDescription(rowDescription, ev->Get()->DataFields);
                        SendStream(rowDescription);
                    }
                    if (!ev->Get()->DataRows.empty()) { // dataFields
                        for (const auto& row : ev->Get()->DataRows) {
                            TPGStreamOutput<TPGDataRow> dataRow;
                            FillDataRow(dataRow, row);
                            SendStream(dataRow);
                        }
                    }
                    if (!ev->Get()->NoticeFields.empty()) { // notices
                        SendNoticeResponse(ev->Get()->NoticeFields);
                    }
                    if (ev->Get()->CommandCompleted) {
                        // commandComplete
                        TString tag = ev->Get()->Tag ? ev->Get()->Tag : "OK";
                        TPGStreamOutput<TPGCommandComplete> commandComplete;
                        commandComplete << tag << '\0';
                        SendStream(commandComplete);
                    }
                }
            } else {
                SendErrorResponse(ev->Get()->ErrorFields);
                if (ev->Get()->DropConnection) {
                    CloseConnection = true;
                    FlushAndPoll();
                    return;
                }
            }
            if (ev->Get()->ReadyForQuery) {
                BecomeReadyForQuery();
            }
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvDescribeResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->ErrorFields.empty()) {
                if (ev->Get()->ParameterTypes.size() > 0) {
                    // parameterDescription (statement only)
                    TPGStreamOutput<TPGParameterDescription> parameterDescription;
                    parameterDescription << uint16_t(ev->Get()->ParameterTypes.size()); // number of fields
                    for (auto type : ev->Get()->ParameterTypes) {
                        parameterDescription << type;
                    }
                    SendStream(parameterDescription);
                }
                if (ev->Get()->DataFields.size() > 0) {
                    // rowDescription
                    TPGStreamOutput<TPGRowDescription> rowDescription;
                    FillDataRowDescription(rowDescription, ev->Get()->DataFields);
                    SendStream(rowDescription);
                } else {
                    SendMessage(TPGNoData());
                }
            } else {
                SendErrorResponse(ev->Get()->ErrorFields);
                if (ev->Get()->DropConnection) {
                    CloseConnection = true;
                    FlushAndPoll();
                    return;
                }
            }
            ++OutgoingSequenceNumber;
            BecomeReadyForQuery();
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvExecuteResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->TransactionStatus) {
                TransactionStatus = ev->Get()->TransactionStatus;
            }
            if (ev->Get()->ErrorFields.empty()) {
                if (ev->Get()->EmptyQuery) {
                    SendMessage(TPGEmptyQueryResponse());
                } else {
                    if (!ev->Get()->DataRows.empty()) { // dataFields
                        for (const auto& row : ev->Get()->DataRows) {
                            TPGStreamOutput<TPGDataRow> dataRow;
                            FillDataRow(dataRow, row);
                            SendStream(dataRow);
                        }
                    }
                    if (!ev->Get()->NoticeFields.empty()) { // notices
                        SendNoticeResponse(ev->Get()->NoticeFields);
                    }
                    if (ev->Get()->CommandCompleted) {
                        // commandComplete
                        TString tag = ev->Get()->Tag ? ev->Get()->Tag : "OK";
                        TPGStreamOutput<TPGCommandComplete> commandComplete;
                        commandComplete << tag << '\0';
                        SendStream(commandComplete);
                    }
                }
            } else {
                SendErrorResponse(ev->Get()->ErrorFields);
                if (ev->Get()->DropConnection) {
                    CloseConnection = true;
                    FlushOutput();
                    return;
                }
            }
            if (ev->Get()->ReadyForQuery) {
                ++OutgoingSequenceNumber;
                BecomeReadyForQuery();
            }
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvParseResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->ErrorFields.empty()) {
                TPGStreamOutput<TPGParseComplete> parseComplete;
                SendStream(parseComplete);
            } else {
                SendErrorResponse(ev->Get()->ErrorFields);
                if (ev->Get()->DropConnection) {
                    CloseConnection = true;
                    FlushOutput();
                    return;
                }
            }
            ++OutgoingSequenceNumber;
            BecomeReadyForQuery();
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvBindResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            if (ev->Get()->ErrorFields.empty()) {
                TPGStreamOutput<TPGBindComplete> bindComplete;
                SendStream(bindComplete);
            } else {
                SendErrorResponse(ev->Get()->ErrorFields);
            }
            ++OutgoingSequenceNumber;
            BecomeReadyForQuery();
        } else {
            PostponeEvent(ev);
        }
    }

    void HandleConnected(TEvPGEvents::TEvCloseResponse::TPtr& ev) {
        if (IsEventExpected(ev)) {
            TPGStreamOutput<TPGCloseComplete> closeComplete;
            SendStream(closeComplete);
            ++OutgoingSequenceNumber;
            BecomeReadyForQuery();
        } else {
            PostponeEvent(ev);
        }
    }

    bool HasInputMessage() const {
        if (BufferInput.Size() >= sizeof(TPGMessage)) {
            const TPGMessage* message = reinterpret_cast<const TPGMessage*>(BufferInput.data());
            if (BufferInput.Size() >= message->GetMessageSize()) {
                return true;
            }
        }
        return false;
    }

    const TPGMessage* GetInputMessage() const {
        Y_DEBUG_ABORT_UNLESS(HasInputMessage());
        return reinterpret_cast<const TPGMessage*>(BufferInput.data());
    }

    size_t GetInputMessageSize() const {
        return GetInputMessage()->GetMessageSize();
    }

    size_t GetInputMessageDataSize() const {
        return GetInputMessage()->GetDataSize();
    }

    void HandleConnected(TEvPollerReady::TPtr event) {
        if (event->Get()->Read) {
            for (;;) {
                ssize_t need = BufferInput.Avail();
                if (need == 0) {
                    size_t capacity = BufferInput.Capacity() * 2;
                    if (capacity > MAX_BUFFER_SIZE) {
                        BLOG_ERROR("connection closed - not enough buffer size (" << capacity << " > " << MAX_BUFFER_SIZE << ")");
                        return PassAway();
                    }
                    BufferInput.Reserve(capacity);
                    need = BufferInput.Avail();
                }
                ssize_t res = SocketReceive(BufferInput.Pos(), need);
                if (res > 0) {
                    InactivityTimer.Reset();
                    BufferInput.Advance(res);
                    while (HasInputMessage()) {
                        const TPGMessage* message = GetInputMessage();
                        PrintMessage(EDirection::Incoming, *message);
                        switch (message->Message) {
                            case TPGInitial::CODE:
                                HandleMessage(static_cast<const TPGInitial*>(message));
                                break;
                            case TPGQuery::CODE:
                                HandleMessage(static_cast<const TPGQuery*>(message));
                                break;
                            case TPGTerminate::CODE:
                                HandleMessage(static_cast<const TPGTerminate*>(message));
                                break;
                            case TPGPasswordMessage::CODE:
                                HandleMessage(static_cast<const TPGPasswordMessage*>(message));
                                break;
                            case TPGParse::CODE:
                                HandleMessage(static_cast<const TPGParse*>(message));
                                break;
                            case TPGSync::CODE:
                                HandleMessage(static_cast<const TPGSync*>(message));
                                break;
                            case TPGBind::CODE:
                                HandleMessage(static_cast<const TPGBind*>(message));
                                break;
                            case TPGDescribe::CODE:
                                HandleMessage(static_cast<const TPGDescribe*>(message));
                                break;
                            case TPGExecute::CODE:
                                HandleMessage(static_cast<const TPGExecute*>(message));
                                break;
                            case TPGClose::CODE:
                                HandleMessage(static_cast<const TPGClose*>(message));
                                break;
                            case TPGFlush::CODE:
                                HandleMessage(static_cast<const TPGFlush*>(message));
                                break;
                            default:
                                BLOG_ERROR("invalid message (" << message->Message << ")");
                                CloseConnection = true;
                                break;
                        }
                        BufferInput.ChopHead(GetInputMessageSize());
                    }
                    if (!FlushOutput()) {
                        return;
                    }
                } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    break;
                } else if (-res == EINTR) {
                    continue;
                } else if (res == 0) {
                    // connection closed
                    BLOG_ERROR("connection was gracefully closed iSQ: " << IncomingSequenceNumber << " oSQ: " << OutgoingSequenceNumber << " sSQ: " << SyncSequenceNumber);
                    return PassAway();
                } else {
                    BLOG_ERROR("connection closed - error in recv: " << strerror(-res));
                    return PassAway();
                }
            }
            if (event->Get() == InactivityEvent) {
                const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
                if (passed >= InactivityTimeout) {
                    BLOG_ERROR("connection closed by inactivity timeout");
                    return PassAway(); // timeout
                } else {
                    Schedule(InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
                }
            }
        }
        if (event->Get()->Write) {
            if (!FlushOutput()) {
                return;
            }
        }
        RequestPoller();
    }

    void HandleConnected(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, true);
    }

    bool FlushOutput() {
        while (BufferOutput.Size() > 0) {
            ssize_t res = SocketSend(BufferOutput.Data(), BufferOutput.Size());
            if (res > 0) {
                BufferOutput.ChopHead(res);
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                break;
            } else {
                BLOG_ERROR("connection closed - error in FlushOutput: " << strerror(-res));
                PassAway();
                return false;
            }
        }
        if (CloseConnection && BufferOutput.Empty()) {
            BLOG_D("connection closed");
            PassAway();
            return false;
        }
        return true;
    }

    bool UpgradeToSecure() {
        int res = Socket->TryUpgradeToSecure();
        if (res < 0) {
            BLOG_ERROR("connection closed - error in UpgradeToSecure: " << strerror(-res));
            PassAway();
            return false;
        }
        return true;
    }

    STATEFN(StateAccepting) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleAccepting);
            hFunc(TEvPollerRegisterResult, HandleAccepting);
        }
    }

    STATEFN(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleConnected);
            hFunc(TEvPollerRegisterResult, HandleConnected);
            hFunc(TEvPGEvents::TEvAuthResponse, HandleConnected);
            hFunc(TEvPGEvents::TEvFinishHandshake, HandleConnected);
            hFunc(TEvPGEvents::TEvQueryResponse, HandleConnected);
            hFunc(TEvPGEvents::TEvParseResponse, HandleConnected);
            hFunc(TEvPGEvents::TEvBindResponse, HandleConnected);
            hFunc(TEvPGEvents::TEvDescribeResponse, HandleConnected);
            hFunc(TEvPGEvents::TEvExecuteResponse, HandleConnected);
            hFunc(TEvPGEvents::TEvCloseResponse, HandleConnected);
        }
    }
};

NActors::IActor* CreatePGConnection(const TActorId& listenerActorId, TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address, const TActorId& databaseProxy) {
    return new TPGConnection(listenerActorId, std::move(socket), std::move(address), databaseProxy);
}

}