#pragma once
#include "test_utils.h"
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NKikimr::NPersQueueTests {

class TPQDataWriter {
public:
    TPQDataWriter(const TString& sourceId, NPersQueue::TTestServer& server, const TString& testTopicPath = "topic1")
        : SourceId_(sourceId)
        , Port_(server.GrpcPort)
        , Client(*server.AnnoyingClient)
        , Runtime(server.CleverServer->GetRuntime())
    {
        InitializeChannel();
        WaitWritePQServiceInitialization(testTopicPath);
    }

    void Read(const TString& topic, const TString& clientId, const TString& ticket = "", bool error = false, bool checkACL = false, bool onlyCreate = false) {
        Y_UNUSED(Client);
        Y_UNUSED(Runtime); //TODO: use them to restart PERSQUEUE tablets

        grpc::ClientContext context;

        if (!ticket.empty())
            context.AddMetadata("x-ydb-auth-ticket", ticket);


        auto stream = StubP_->MigrationStreamingRead(&context);
        UNIT_ASSERT(stream);

        // Send initial request.
        Ydb::PersQueue::V1::MigrationStreamingReadClientMessage  req;
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage resp;

        req.mutable_init_request()->add_topics_read_settings()->set_topic(topic);
        req.mutable_init_request()->mutable_read_params()->set_max_read_messages_count(1);
        req.mutable_init_request()->set_read_only_original(true);

        req.mutable_init_request()->set_consumer(clientId);

        if (!stream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        Client.GetClientInfo({"rt3.dc1--topic1"}, clientId, true);

        if (!stream->Read(&resp)) {
            auto status = stream->Finish();
            Cerr << (int)status.error_code() << " " << status.error_message() << "\n";
            Y_ABORT("");
            UNIT_ASSERT(error);
        }
        Cerr << "=== Got response: " << resp.ShortDebugString() << Endl;
        if (error) {
            UNIT_ASSERT(resp.response_case() == Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::RESPONSE_NOT_SET);
            return;
        }
        UNIT_ASSERT_C(resp.response_case() == Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kInitResponse, resp);


        if (onlyCreate)
            return;

        for (ui32 i = 0; i < 11; ++i) {
            Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
            req.mutable_read();

            if (!stream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            Client.AlterTopic("rt3.dc1--topic1", i < 10 ? 2 : 3);
        }

        if (checkACL) {
            NACLib::TDiffACL acl;
            acl.RemoveAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, clientId + "@" BUILTIN_ACL_DOMAIN);
            Client.ModifyACL("/Root/PQ", "rt3.dc1--topic1", acl.SerializeAsString());

            Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
            req.mutable_read();
            if (!stream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            UNIT_ASSERT(stream->Read(&resp));
            UNIT_ASSERT(resp.response_case() == Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::RESPONSE_NOT_SET && resp.issues(0).issue_code() == (ui32)Ydb::PersQueue::ErrorCode::ErrorCode::ACCESS_DENIED);
            return;
        }
        Client.GetClientInfo({"rt3.dc1--topic1"}, clientId, true);
        ui64 assignId = 0;
        for (ui32 i = 0; i < 11;) {
            Ydb::PersQueue::V1::MigrationStreamingReadServerMessage resp;

            UNIT_ASSERT(stream->Read(&resp));

            if (resp.response_case() == Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kAssigned) {
                auto assignId = resp.assigned().assign_id();
                Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
                req.mutable_start_read()->mutable_topic()->set_path("topic1");
                req.mutable_start_read()->set_cluster("dc1");
                req.mutable_start_read()->set_assign_id(assignId);
                UNIT_ASSERT(stream->Write(req));
                continue;
            }

            UNIT_ASSERT(resp.response_case() == Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kDataBatch);
            Cerr << resp << "\n";

            UNIT_ASSERT_VALUES_EQUAL(resp.data_batch().partition_data_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.data_batch().partition_data(0).batches_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.data_batch().partition_data(0).batches(0).message_data_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resp.data_batch().partition_data(0).batches(0).message_data(0).offset(), i);

            assignId = resp.data_batch().partition_data(0).cookie().assign_id();
            ++i;
        }
        //TODO: check here that read will never done        UNIT_ASSERT(!stream->Read(&resp));
        {
            for (ui32 i = 1; i < 11; ++i) {
                Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;

                auto cookie = req.mutable_commit()->add_cookies();
                cookie->set_assign_id(assignId);
                cookie->set_partition_cookie(i);

                if (!stream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
            ui32 i = 1;
            while (i <= 10) {
                Ydb::PersQueue::V1::MigrationStreamingReadServerMessage resp;

                UNIT_ASSERT(stream->Read(&resp));
                Cerr << resp << "\n";
                UNIT_ASSERT(resp.response_case() == Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kCommitted);
                UNIT_ASSERT(resp.committed().cookies_size() > 0);
                for (const auto& c : resp.committed().cookies()) {
                    UNIT_ASSERT(c.partition_cookie() == i);
                    ++i;
                    UNIT_ASSERT(i <= 11);
                }
            }
            Client.GetClientInfo({"rt3.dc1--topic1"}, clientId, true);
        }
    }

    void WaitWritePQServiceInitialization(const TString& testTopicPath = "topic1") {
        bool tried = false;
        while (true) {
            if (tried) {
                Sleep(TDuration::MilliSeconds(100));
            } else {
                tried = true;
            }

            Ydb::PersQueue::V1::StreamingWriteClientMessage  req;
            Ydb::PersQueue::V1::StreamingWriteServerMessage resp;
            grpc::ClientContext context;

            auto stream = StubP_->StreamingWrite(&context);
            UNIT_ASSERT(stream);

            req.mutable_init_request()->set_topic(testTopicPath);
            req.mutable_init_request()->set_message_group_id("12345678");

            if (!stream->Write(req)) {
                UNIT_ASSERT_C(stream->Read(&resp), "Context error: " << context.debug_error_string());
                UNIT_ASSERT_C(resp.status() == Ydb::StatusIds::UNAVAILABLE, "Response: " << resp << ", Context error: " << context.debug_error_string());
                continue;
            }

            AssertSuccessfullStreamingOperation(stream->Read(&resp), stream);
            if (resp.status() == Ydb::StatusIds::UNAVAILABLE) {
                continue;
            }

            if (stream->WritesDone()) {
                auto status = stream->Finish();
                Cerr << "Finish: " << (int)status.error_code() << " " << status.error_message() << "\n";
            }

            break;
        }
    }

    ui32 InitSession(const TString& sourceId, ui32 pg, bool success, ui32 step = 0) {
        Ydb::PersQueue::V1::StreamingWriteClientMessage  req;
        Ydb::PersQueue::V1::StreamingWriteServerMessage resp;

        grpc::ClientContext context;

        auto stream = StubP_->StreamingWrite(&context);

        UNIT_ASSERT(stream);
        req.mutable_init_request()->set_topic("topic1");
        req.mutable_init_request()->set_message_group_id(sourceId);
        req.mutable_init_request()->set_partition_group_id(pg);

        UNIT_ASSERT(stream->Write(req));
        UNIT_ASSERT(stream->Read(&resp));
        Cerr << "Init result: " << resp << "\n";
        if (!success) {
            UNIT_ASSERT(resp.status() != Ydb::StatusIds::SUCCESS);
            return 0;
        } else {
            if (resp.status() != Ydb::StatusIds::SUCCESS && step < 5) {
                Sleep(TDuration::MilliSeconds(100));
                return InitSession(sourceId, pg, success, step + 1);
            }
            UNIT_ASSERT(resp.status() == Ydb::StatusIds::SUCCESS);

            return resp.init_response().partition_id();
        }
        return 0;
    }

    ui32 Write(const TString& topic, const TVector<TString>& data, bool error = false, const TMaybe<TString>& ticket = {}) {
        return WriteImpl(topic, {data}, error, ticket);
    }

private:
    ui32 WriteImpl(const TString& topic, const TVector<TString>& data, bool error, const TMaybe<TString>& ticket) {
        grpc::ClientContext context;

        if (ticket)
            context.AddMetadata("x-ydb-auth-ticket", *ticket);


        auto stream = StubP_->StreamingWrite(&context);
        UNIT_ASSERT(stream);

        // Send initial request.
        Ydb::PersQueue::V1::StreamingWriteClientMessage  req;
        Ydb::PersQueue::V1::StreamingWriteServerMessage resp;

        req.mutable_init_request()->set_topic(topic);
        req.mutable_init_request()->set_message_group_id(SourceId_);
        req.mutable_init_request()->set_max_supported_format_version(0);

        (*req.mutable_init_request()->mutable_session_meta())["key"] = "value";

        if (!stream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        ui32 part = 0;

        if (!stream->Read(&resp)) {
            auto status = stream->Finish();
            if (error) {
                Cerr << (int)status.error_code() << " " << status.error_message() << "\n";
                UNIT_ASSERT(status.error_code() == grpc::StatusCode::UNAUTHENTICATED);

            } else {
                UNIT_ASSERT(false);
            }
        }

        if (!error) {
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::PersQueue::V1::StreamingWriteServerMessage::kInitResponse, resp);
            UNIT_ASSERT_C(!resp.init_response().session_id().empty(), resp);
            if (resp.init_response().block_format_version() > 0) {
                UNIT_ASSERT_C(resp.init_response().max_block_size() > 0, resp);
                UNIT_ASSERT_C(resp.init_response().max_flush_window_size() > 0, resp);
            }
            part = resp.init_response().partition_id();
        } else {
            Cerr << resp << "\n";
            UNIT_ASSERT(resp.server_message_case() == Ydb::PersQueue::V1::StreamingWriteServerMessage::SERVER_MESSAGE_NOT_SET);
            return 0;
        }

        // Send data requests.
        Flush(data, stream, ticket);

        Flush(data, stream, ticket);

        Flush(data, stream, ticket);

        Flush(data, stream, ticket);

        //will cause only 4 answers in stream->Read - third call will fail, not blocks
        stream->WritesDone();

        UNIT_ASSERT(!stream->Read(&resp));

        auto status = stream->Finish();
        Cerr << status.ok() << " " << (int)status.error_code() << " " << status.error_message() << "\n";
        UNIT_ASSERT(status.ok());
        return part;
    }

    template <typename S>
    void Flush(const TVector<TString>& data, S& stream, const TMaybe<TString>& ticket) {
        Ydb::PersQueue::V1::StreamingWriteClientMessage  request;
        Ydb::PersQueue::V1::StreamingWriteServerMessage response;

        if (ticket) {
            request.mutable_update_token_request()->set_token(*ticket);
            Cerr << "update user token request: " << request << Endl;
            if (!stream->Write(request)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(stream->Read(&response));
            UNIT_ASSERT_C(response.server_message_case() == Ydb::PersQueue::V1::StreamingWriteServerMessage::kUpdateTokenResponse, response);
        }

        TVector<ui64> allSeqNo;
        auto* mutableData = request.mutable_write_request();
        ui32 offset = 0;
        for (const TString& d : data) {
            ui64 seqNo = AtomicIncrement(SeqNo_);
            allSeqNo.push_back(seqNo);
            mutableData->add_sequence_numbers(seqNo);
            mutableData->add_message_sizes(d.size());
            mutableData->add_created_at_ms(TInstant::Now().MilliSeconds());
            mutableData->add_sent_at_ms(TInstant::Now().MilliSeconds());

            mutableData->add_blocks_offsets(offset++);
            mutableData->add_blocks_part_numbers(0);
            mutableData->add_blocks_message_counts(1);
            mutableData->add_blocks_uncompressed_sizes(d.size());
            mutableData->add_blocks_headers(TString(1, '\0') /* RAW codec ID */);
            mutableData->add_blocks_data(d);
        }

        Cerr << "request: " << request << Endl;
        if (!stream->Write(request)) {
            ythrow yexception() << "write fail";
        }

        UNIT_ASSERT(stream->Read(&response));
        UNIT_ASSERT_C(response.server_message_case() == Ydb::PersQueue::V1::StreamingWriteServerMessage::kBatchWriteResponse, response);
        UNIT_ASSERT_VALUES_EQUAL(data.size(), response.batch_write_response().sequence_numbers_size());
        for (size_t i = 0; i < data.size(); ++i) {
            UNIT_ASSERT(!response.batch_write_response().already_written(i));
            UNIT_ASSERT_VALUES_EQUAL(response.batch_write_response().sequence_numbers(i), allSeqNo[i]);
        }
        UNIT_ASSERT(response.batch_write_response().has_write_statistics());
    }

    ui64 ReadCookieFromMetadata(const std::multimap<grpc::string_ref, grpc::string_ref>& meta) const {
        auto ci = meta.find("cookie");
        if (ci == meta.end()) {
            ythrow yexception() << "server didn't provide the cookie";
        } else {
            return FromString<ui64>(TStringBuf(ci->second.data(), ci->second.size()));
        }
    }

    void InitializeChannel() {
        Channel_ = grpc::CreateChannel("localhost:" + ToString(Port_), grpc::InsecureChannelCredentials());
        StubP_ = Ydb::PersQueue::V1::PersQueueService::NewStub(Channel_);
    }

private:
    const TString SourceId_;
    const ui16 Port_;

    TFlatMsgBusPQClient& Client;
    TTestActorRuntime *Runtime;

    TAtomic SeqNo_ = 1;

    //! Сетевой канал взаимодействия с proxy-сервером.
    std::shared_ptr<grpc::Channel> Channel_;
    std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> StubP_;

};

} // namespace NKikimr::NPersQueueTests
