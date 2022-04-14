#include "data_writer.h"

namespace NPersQueue::NTests {
using namespace NKikimr;

void TPQDataWriter::Read(
        const TString& topic, const TString& clientId, const TString& ticket, bool error, bool checkACL,
        bool useBatching, bool onlyCreate
) {

    grpc::ClientContext context;

    auto stream = StubP_->ReadSession(&context);
    UNIT_ASSERT(stream);

    // Send initial request.
    TReadRequest  req;
    TReadResponse resp;

    req.MutableInit()->AddTopics(topic);

    req.MutableInit()->SetClientId(clientId);
    req.MutableInit()->SetProxyCookie(ProxyCookie);
    if (!ticket.empty()) {
        req.MutableCredentials()->SetTvmServiceTicket(ticket);
    }

    if (useBatching) {
        req.MutableInit()->SetProtocolVersion(TReadRequest::Batching);
    }

    if (!stream->Write(req)) {
        ythrow yexception() << "write fail";
    }
    //TODO[komels]: why this leads to timeout?
    //Server.AnnoyingClient->GetClientInfo({topic}, clientId, true);
    UNIT_ASSERT(stream->Read(&resp));
    if (error) {
        UNIT_ASSERT(resp.HasError());
        return;
    }
    UNIT_ASSERT_C(resp.HasInit(), resp);

    if (onlyCreate)
        return;

    for (ui32 i = 0; i < 11; ++i) {
        TReadRequest req;

        req.MutableRead()->SetMaxCount(1);

        if (!stream->Write(req)) {
            ythrow yexception() << "write fail";
        }
        Server.AnnoyingClient->AlterTopic(FullTopicName, i < 10 ? 2 : 3);

    }

    if (checkACL) {
        NACLib::TDiffACL acl;
        acl.RemoveAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, clientId + "@" BUILTIN_ACL_DOMAIN);
        Server.AnnoyingClient->ModifyACL("/Root/PQ", FullTopicName, acl.SerializeAsString());

        TReadRequest req;
        req.MutableRead()->SetMaxCount(1);
        if (!stream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        UNIT_ASSERT(stream->Read(&resp));
        UNIT_ASSERT(resp.HasError() && resp.GetError().GetCode() == NPersQueue::NErrorCode::EErrorCode::ACCESS_DENIED);
        return;
    }
    Server.AnnoyingClient->GetClientInfo({FullTopicName}, clientId, true);
    for (ui32 i = 0; i < 11; ++i) {
        TReadResponse resp;

        UNIT_ASSERT(stream->Read(&resp));

        if (useBatching) {
            UNIT_ASSERT(resp.HasBatchedData());
            UNIT_ASSERT_VALUES_EQUAL(resp.GetBatchedData().PartitionDataSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.GetBatchedData().GetPartitionData(0).BatchSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.GetBatchedData().GetPartitionData(0).GetBatch(0).MessageDataSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.GetBatchedData().GetPartitionData(0).GetBatch(0).GetMessageData(0).GetOffset(), i);
        } else {
            UNIT_ASSERT(resp.HasData());
            UNIT_ASSERT_VALUES_EQUAL(resp.GetData().MessageBatchSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.GetData().GetMessageBatch(0).MessageSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.GetData().GetMessageBatch(0).GetMessage(0).GetOffset(), i);
        }
    }
    //TODO: check here that read will never done        UNIT_ASSERT(!stream->Read(&resp));
    {
        for (ui32 i = 1; i < 11; ++i) {
            TReadRequest req;

            req.MutableCommit()->AddCookie(i);

            if (!stream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }
        ui32 i = 1;
        while (i <= 10) {
            TReadResponse resp;

            UNIT_ASSERT(stream->Read(&resp));
            Cerr << resp << "\n";
            UNIT_ASSERT(resp.HasCommit());
            UNIT_ASSERT(resp.GetCommit().CookieSize() > 0);
            for (ui32 j = 0; j < resp.GetCommit().CookieSize(); ++j) {
                UNIT_ASSERT( resp.GetCommit().GetCookie(j) == i);
                ++i;
                UNIT_ASSERT(i <= 11);
            }
        }
        Server.AnnoyingClient->GetClientInfo({FullTopicName}, clientId, true);
    }
}

void TPQDataWriter::WaitWritePQServiceInitialization() {
    TWriteRequest  req;
    TWriteResponse resp;
    while (true) {
        grpc::ClientContext context;

        auto stream = StubP_->WriteSession(&context);
        UNIT_ASSERT(stream);
        req.MutableInit()->SetTopic(ShortTopicName);
        req.MutableInit()->SetSourceId("12345678");
        req.MutableInit()->SetProxyCookie(ProxyCookie);

        if (!stream->Write(req)) {
            continue;
        }
        UNIT_ASSERT(stream->Read(&resp));
        if (resp.GetError().GetCode() == NPersQueue::NErrorCode::INITIALIZING) {
            Sleep(TDuration::MilliSeconds(50));
        } else {
            break;
        }
    }
}

ui32 TPQDataWriter::InitSession(const TString& sourceId, ui32 pg, bool success, ui32 step) {
    TWriteRequest  req;
    TWriteResponse resp;

    grpc::ClientContext context;

    auto stream = StubP_->WriteSession(&context);

    UNIT_ASSERT(stream);
    req.MutableInit()->SetTopic(ShortTopicName);
    req.MutableInit()->SetSourceId(sourceId);
    req.MutableInit()->SetPartitionGroup(pg);
    req.MutableInit()->SetProxyCookie(ProxyCookie);

    UNIT_ASSERT(stream->Write(req));
    UNIT_ASSERT(stream->Read(&resp));
    Cerr << "Init result: " << resp << "\n";
    //TODO: ensure topic creation - proxy already knows about new partitions, but tablet - no!
    if (!success) {
        UNIT_ASSERT(resp.HasError());
        return 0;
    } else {
        if (!resp.HasInit() && step < 5) {
            Sleep(TDuration::MilliSeconds(100));
            return InitSession(sourceId, pg, success, step + 1);
        }
        UNIT_ASSERT(resp.HasInit());
        return resp.GetInit().GetPartition();
    }
    return 0;
}

ui32 TPQDataWriter::WriteImpl(
        const TString& topic, const TVector<TString>& data, bool error, const TString& ticket, bool batch
) {
    grpc::ClientContext context;

    auto stream = StubP_->WriteSession(&context);
    UNIT_ASSERT(stream);

    // Send initial request.
    TWriteRequest  req;
    TWriteResponse resp;

    req.MutableInit()->SetTopic(topic);
    req.MutableInit()->SetSourceId(SourceId_);
    req.MutableInit()->SetProxyCookie(ProxyCookie);
    if (!ticket.empty())
        req.MutableCredentials()->SetTvmServiceTicket(ticket);
    auto item = req.MutableInit()->MutableExtraFields()->AddItems();
    item->SetKey("key");
    item->SetValue("value");

    if (!stream->Write(req)) {
        ythrow yexception() << "write fail";
    }

    ui32 part = 0;

    UNIT_ASSERT(stream->Read(&resp));

    if (!error) {
        UNIT_ASSERT_C(resp.HasInit(), resp);
        UNIT_ASSERT_C(!resp.GetInit().GetSessionId().empty(), resp);
        part = resp.GetInit().GetPartition();
    } else {
        Cerr << resp << "\n";
        UNIT_ASSERT(resp.HasError());
        return 0;
    }

    // Send data requests.
    Flush(data, stream, ticket, batch);

    Flush(data, stream, ticket, batch);

    Flush(data, stream, ticket, batch);

    Flush(data, stream, ticket, batch);

    //will cause only 4 answers in stream->Read - third call will fail, not blocks
    stream->WritesDone();

    UNIT_ASSERT(!stream->Read(&resp));

    auto status = stream->Finish();
    UNIT_ASSERT(status.ok());
    return part;
}

ui64 TPQDataWriter::ReadCookieFromMetadata(const std::multimap<grpc::string_ref, grpc::string_ref>& meta) const {
    auto ci = meta.find("cookie");
    if (ci == meta.end()) {
        ythrow yexception() << "server didn't provide the cookie";
    } else {
        return FromString<ui64>(TStringBuf(ci->second.data(), ci->second.size()));
    }
}

void TPQDataWriter::InitializeChannel() {
    Channel_ = grpc::CreateChannel("[::1]:" + ToString(Server.GrpcPort), grpc::InsecureChannelCredentials());
    Stub_ = NKikimrClient::TGRpcServer::NewStub(Channel_);

    grpc::ClientContext context;
    NKikimrClient::TChooseProxyRequest request;
    NKikimrClient::TResponse response;
    auto status = Stub_->ChooseProxy(&context, request, &response);
    UNIT_ASSERT(status.ok());
    Cerr << response << "\n";
    UNIT_ASSERT(response.GetStatus() == NMsgBusProxy::MSTATUS_OK);
    ProxyCookie = response.GetProxyCookie();
    Channel_ = grpc::CreateChannel(
            "[" + response.GetProxyName() + "]:" + ToString(Server.GrpcPort),
            grpc::InsecureChannelCredentials()
    );
    StubP_ = NPersQueue::PersQueueService::NewStub(Channel_);
}
} // namespace NKikimr::NPersQueueTests
