#pragma once

#include "test_server.h"

#include <kikimr/yndx/api/grpc/persqueue.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>

namespace NPersQueue::NTests {

class TPQDataWriter {
public:
    TPQDataWriter(
            const TString& defaultTopicFullName, const TString& defaultShortTopicName,
            const TString& sourceId, TTestServer& server
    )
        : FullTopicName(defaultTopicFullName)
        , ShortTopicName(defaultShortTopicName)
        , SourceId_(sourceId)
        , Server(server)
    {
        InitializeChannel();
        WaitWritePQServiceInitialization();
    }

    void Read(const TString& topic, const TString& clientId, const TString& ticket = "", bool error = false,
              bool checkACL = false, bool useBatching = false, bool onlyCreate = false);

    void WaitWritePQServiceInitialization();

    ui32 InitSession(const TString& sourceId, ui32 pg, bool success, ui32 step = 0);

    ui32 Write(const TString& topic, const TString& data, bool error = false, const TString& ticket = "") {
        return WriteImpl(topic, {data}, error, ticket, false);
    }

    ui32 WriteBatch(const TString& topic, const TVector<TString>& data, bool error = false,
                    const TString& ticket = "") {
        return WriteImpl(topic, data, error, ticket, true);
    }

private:
    ui32 WriteImpl(const TString& topic, const TVector<TString>& data, bool error, const TString& ticket,
                   bool batch);

    template<typename S>
    void Flush(const TVector<TString>& data, S& stream, const TString& ticket, bool batch) {
        TWriteRequest request;
        TWriteResponse response;

        TVector<ui64> allSeqNo;
        if (batch) {
            for (const TString& d: data) {
                ui64 seqNo = AtomicIncrement(SeqNo_);
                allSeqNo.push_back(seqNo);
                auto *mutableData = request.MutableDataBatch()->AddData();
                mutableData->SetSeqNo(seqNo);
                mutableData->SetData(d);
            }
        } else {
            ui64 seqNo = AtomicIncrement(SeqNo_);
            allSeqNo.push_back(seqNo);
            request.MutableData()->SetSeqNo(seqNo);
            request.MutableData()->SetData(JoinSeq("\n", data));
        }
        if (!ticket.empty()) {
            request.MutableCredentials()->SetTvmServiceTicket(ticket);
        }

        Cerr << "request: " << request << Endl;
        if (!stream->Write(request)) {
            ythrow yexception() << "write fail";
        }

        UNIT_ASSERT(stream->Read(&response));
        if (batch) {
            UNIT_ASSERT_C(response.HasAckBatch(), response);
            UNIT_ASSERT_VALUES_EQUAL(data.size(), response.GetAckBatch().AckSize());
            for (size_t i = 0; i < data.size(); ++i) {
                const auto& ack = response.GetAckBatch().GetAck(i);
                UNIT_ASSERT(!ack.GetAlreadyWritten());
                UNIT_ASSERT(!ack.HasStat());
                UNIT_ASSERT_VALUES_EQUAL(ack.GetSeqNo(), allSeqNo[i]);
            }
            UNIT_ASSERT(response.GetAckBatch().HasStat());
        } else {
            const auto& ack = response.GetAck();
            UNIT_ASSERT(!ack.GetAlreadyWritten());
            UNIT_ASSERT_VALUES_EQUAL(ack.GetSeqNo(), allSeqNo[0]);
            UNIT_ASSERT(ack.HasStat());
        }
    }

    ui64 ReadCookieFromMetadata(const std::multimap<grpc::string_ref, grpc::string_ref>& meta) const;

    void InitializeChannel();

private:
    TString FullTopicName;
    TString ShortTopicName;
    const TString SourceId_;

    TTestServer& Server;

    TAtomic SeqNo_ = 1;

    //! Сетевой канал взаимодействия с proxy-сервером.
    std::shared_ptr<grpc::Channel> Channel_;
    std::unique_ptr<NKikimrClient::TGRpcServer::Stub> Stub_;
    std::unique_ptr<PersQueueService::Stub> StubP_;

    ui64 ProxyCookie = 0;
};
} // NKikimr::NPersQueueTests
