#include <library/cpp/testing/unittest/registar.h>

#include "example.h"

#include <util/generic/cast.h>

using namespace NBus;
using namespace NBus::NTest;

static void FillWithJunk(TArrayRef<char> data) {
    TStringBuf junk =
        "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "01234567890123456789012345678901234567890123456789012345678901234567890123456789";

    for (size_t i = 0; i < data.size(); i += junk.size()) {
        memcpy(data.data() + i, junk.data(), Min(junk.size(), data.size() - i));
    }
}

static TString JunkString(size_t len) {
    TTempBuf temp(len);
    TArrayRef<char> tempArrayRef(temp.Data(), len);
    FillWithJunk(tempArrayRef);

    return TString(tempArrayRef.data(), tempArrayRef.size());
}

TExampleRequest::TExampleRequest(TAtomic* counterPtr, size_t payloadSize)
    : TBusMessage(77)
    , AllocCounter(counterPtr)
    , Data(JunkString(payloadSize))
{
}

TExampleRequest::TExampleRequest(ECreateUninitialized, TAtomic* counterPtr)
    : TBusMessage(MESSAGE_CREATE_UNINITIALIZED)
    , AllocCounter(counterPtr)
{
}

TExampleResponse::TExampleResponse(TAtomic* counterPtr, size_t payloadSize)
    : TBusMessage(79)
    , AllocCounter(counterPtr)
    , Data(JunkString(payloadSize))
{
}

TExampleResponse::TExampleResponse(ECreateUninitialized, TAtomic* counterPtr)
    : TBusMessage(MESSAGE_CREATE_UNINITIALIZED)
    , AllocCounter(counterPtr)
{
}

TExampleProtocol::TExampleProtocol(int port)
    : TBusProtocol("Example", port)
    , RequestCount(0)
    , ResponseCount(0)
    , RequestCountDeserialized(0)
    , ResponseCountDeserialized(0)
    , StartCount(0)
{
}

TExampleProtocol::~TExampleProtocol() {
    if (UncaughtException()) {
        // so it could be reported in test
        return;
    }
    Y_ABORT_UNLESS(0 == AtomicGet(RequestCount), "protocol %s: must be 0 requests allocated, actually %d", GetService(), int(RequestCount));
    Y_ABORT_UNLESS(0 == AtomicGet(ResponseCount), "protocol %s: must be 0 responses allocated, actually %d", GetService(), int(ResponseCount));
    Y_ABORT_UNLESS(0 == AtomicGet(RequestCountDeserialized), "protocol %s: must be 0 requests deserialized allocated, actually %d", GetService(), int(RequestCountDeserialized));
    Y_ABORT_UNLESS(0 == AtomicGet(ResponseCountDeserialized), "protocol %s: must be 0 responses deserialized allocated, actually %d", GetService(), int(ResponseCountDeserialized));
    Y_ABORT_UNLESS(0 == AtomicGet(StartCount), "protocol %s: must be 0 start objects allocated, actually %d", GetService(), int(StartCount));
}

void TExampleProtocol::Serialize(const TBusMessage* message, TBuffer& buffer) {
    // Messages have no data, we recreate them from scratch
    // instead of sending, so we don't need to serialize them.
    if (const TExampleRequest* exampleMessage = dynamic_cast<const TExampleRequest*>(message)) {
        buffer.Append(exampleMessage->Data.data(), exampleMessage->Data.size());
    } else if (const TExampleResponse* exampleReply = dynamic_cast<const TExampleResponse*>(message)) {
        buffer.Append(exampleReply->Data.data(), exampleReply->Data.size());
    } else {
        Y_ABORT("unknown message type");
    }
}

TAutoPtr<TBusMessage> TExampleProtocol::Deserialize(ui16 messageType, TArrayRef<const char> payload) {
    // TODO: check data
    Y_UNUSED(payload);

    if (messageType == 77) {
        TExampleRequest* exampleMessage = new TExampleRequest(MESSAGE_CREATE_UNINITIALIZED, &RequestCountDeserialized);
        exampleMessage->Data.append(payload.data(), payload.size());
        return exampleMessage;
    } else if (messageType == 79) {
        TExampleResponse* exampleReply = new TExampleResponse(MESSAGE_CREATE_UNINITIALIZED, &ResponseCountDeserialized);
        exampleReply->Data.append(payload.data(), payload.size());
        return exampleReply;
    } else {
        return nullptr;
    }
}

TExampleClient::TExampleClient(const TBusClientSessionConfig sessionConfig, int port)
    : Proto(port)
    , UseCompression(false)
    , CrashOnError(false)
    , DataSize(320)
    , MessageCount(0)
    , RepliesCount(0)
    , Errors(0)
    , LastError(MESSAGE_OK)
{
    Bus = CreateMessageQueue("TExampleClient");

    Session = TBusClientSession::Create(&Proto, this, sessionConfig, Bus);

    Session->RegisterService("localhost");
}

TExampleClient::~TExampleClient() {
}

EMessageStatus TExampleClient::SendMessage(const TNetAddr* addr) {
    TAutoPtr<TExampleRequest> message(new TExampleRequest(&Proto.RequestCount, DataSize));
    message->SetCompressed(UseCompression);
    return Session->SendMessageAutoPtr(message, addr);
}

void TExampleClient::SendMessages(size_t count, const TNetAddr* addr) {
    UNIT_ASSERT(MessageCount == 0);
    UNIT_ASSERT(RepliesCount == 0);
    UNIT_ASSERT(Errors == 0);

    WorkDone.Reset();
    MessageCount = count;
    for (ssize_t i = 0; i < MessageCount; ++i) {
        EMessageStatus s = SendMessage(addr);
        UNIT_ASSERT_EQUAL_C(s, MESSAGE_OK, "expecting OK, got " << s);
    }
}

void TExampleClient::SendMessages(size_t count, const TNetAddr& addr) {
    SendMessages(count, &addr);
}

void TExampleClient::ResetCounters() {
    MessageCount = 0;
    RepliesCount = 0;
    Errors = 0;
    LastError = MESSAGE_OK;

    WorkDone.Reset();
}

void TExampleClient::WaitReplies() {
    WorkDone.WaitT(TDuration::Seconds(60));

    UNIT_ASSERT_VALUES_EQUAL(AtomicGet(RepliesCount), MessageCount);
    UNIT_ASSERT_VALUES_EQUAL(AtomicGet(Errors), 0);
    UNIT_ASSERT_VALUES_EQUAL(Session->GetInFlight(), 0);

    ResetCounters();
}

EMessageStatus TExampleClient::WaitForError() {
    WorkDone.WaitT(TDuration::Seconds(60));

    UNIT_ASSERT_VALUES_EQUAL(1, MessageCount);
    UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(RepliesCount));
    UNIT_ASSERT_VALUES_EQUAL(0, Session->GetInFlight());
    UNIT_ASSERT_VALUES_EQUAL(1, Errors);
    EMessageStatus result = LastError;

    ResetCounters();
    return result;
}

void TExampleClient::WaitForError(EMessageStatus status) {
    EMessageStatus error = WaitForError();
    UNIT_ASSERT_VALUES_EQUAL(status, error);
}

void TExampleClient::SendMessagesWaitReplies(size_t count, const TNetAddr* addr) {
    SendMessages(count, addr);
    WaitReplies();
}

void TExampleClient::SendMessagesWaitReplies(size_t count, const TNetAddr& addr) {
    SendMessagesWaitReplies(count, &addr);
}

void TExampleClient::OnReply(TAutoPtr<TBusMessage> mess, TAutoPtr<TBusMessage> reply) {
    Y_UNUSED(mess);
    Y_UNUSED(reply);

    if (AtomicIncrement(RepliesCount) == MessageCount) {
        WorkDone.Signal();
    }
}

void TExampleClient::OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) {
    if (CrashOnError) {
        Y_ABORT("client failed: %s", ToCString(status));
    }

    Y_UNUSED(mess);

    AtomicIncrement(Errors);
    LastError = status;
    WorkDone.Signal();
}

TExampleServer::TExampleServer(
    const char* name,
    const TBusServerSessionConfig& sessionConfig)
    : UseCompression(false)
    , AckMessageBeforeSendReply(false)
    , ForgetRequest(false)
{
    Bus = CreateMessageQueue(name);
    Session = TBusServerSession::Create(&Proto, this, sessionConfig, Bus);
}

TExampleServer::TExampleServer(unsigned port, const char* name)
    : UseCompression(false)
    , AckMessageBeforeSendReply(false)
    , ForgetRequest(false)
{
    Bus = CreateMessageQueue(name);
    TBusServerSessionConfig sessionConfig;
    sessionConfig.ListenPort = port;
    Session = TBusServerSession::Create(&Proto, this, sessionConfig, Bus);
}

TExampleServer::~TExampleServer() {
}

size_t TExampleServer::GetInFlight() const {
    return Session->GetInFlight();
}

unsigned TExampleServer::GetActualListenPort() const {
    return Session->GetActualListenPort();
}

TNetAddr TExampleServer::GetActualListenAddr() const {
    return TNetAddr("127.0.0.1", GetActualListenPort());
}

void TExampleServer::WaitForOnMessageCount(unsigned n) {
    TestSync.WaitFor(n);
}

void TExampleServer::OnMessage(TOnMessageContext& mess) {
    TestSync.Inc();

    TExampleRequest* request = VerifyDynamicCast<TExampleRequest*>(mess.GetMessage());

    if (ForgetRequest) {
        mess.ForgetRequest();
        return;
    }

    TAutoPtr<TBusMessage> reply(new TExampleResponse(&Proto.ResponseCount, DataSize.GetOrElse(request->Data.size())));
    reply->SetCompressed(UseCompression);

    EMessageStatus status;
    if (AckMessageBeforeSendReply) {
        TBusIdentity ident;
        mess.AckMessage(ident);
        status = Session->SendReply(ident, reply.Release()); // TODO: leaks on error
    } else {
        status = mess.SendReplyMove(reply);
    }

    Y_ABORT_UNLESS(status == MESSAGE_OK, "failed to send reply: %s", ToString(status).data());
}
