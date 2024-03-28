#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

namespace NYdb::NPersQueue::NTests {

Y_UNIT_TEST_SUITE(Compression) {
    TVector<TString> GetTestMessages(ECodec codec = ECodec::RAW) {
        static const TVector<THashMap<ECodec, TString>> TEST_MESSAGES = {
            {
                {ECodec::RAW, "Alice and Bob"},
                {ECodec::GZIP, TString("\x1F\x8B\x08\x0\x0\x0\x0\x0\x0\x3\x73\xCC\xC9\x4C\x4E\x55\x48\xCC\x4B\x51\x70\xCA\x4F\x2\x0\x2C\xE7\x84\x5D\x0D\x0\x0\x0", 33)},
                {ECodec::ZSTD, TString("\x28\xB5\x2F\xFD\x0\x58\x69\x0\x0\x41\x6C\x69\x63\x65\x20\x61\x6E\x64\x20\x42\x6F\x62", 22)}
            },
            {
                {ECodec::RAW, "Yandex.Cloud"},
                {ECodec::GZIP, TString("\x1F\x8B\x8\x0\x0\x0\x0\x0\x0\x3\x8B\x4C\xCC\x4B\x49\xAD\xD0\x73\xCE\xC9\x2F\x4D\x1\x0\x79\x91\x69\xCA\xC\x0\x0\x0", 32)},
                {ECodec::ZSTD, TString("\x28\xB5\x2F\xFD\x0\x58\x61\x0\x0\x59\x61\x6E\x64\x65\x78\x2E\x43\x6C\x6F\x75\x64", 21)}
            }
        };
        TVector<TString> messages;
        for (auto& m : TEST_MESSAGES) {
            messages.push_back(m.at(codec));
        }
        return messages;
    }

    void AlterTopic(TPersQueueYdbSdkTestSetup& setup) {
        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> stub;

        {
            channel = grpc::CreateChannel("localhost:" + ToString(setup.GetGrpcPort()), grpc::InsecureChannelCredentials());
            stub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
        }

        Ydb::PersQueue::V1::AlterTopicRequest request;
        request.set_path(TStringBuilder() << "/Root/PQ/rt3.dc1--" << setup.GetTestTopic());
        auto props = request.mutable_settings();
        props->set_partitions_count(1);
        props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
        props->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        props->add_supported_codecs(Ydb::PersQueue::V1::CODEC_GZIP);
        props->add_supported_codecs(Ydb::PersQueue::V1::CODEC_ZSTD);
        auto rr = props->add_read_rules();
        rr->set_consumer_name(setup.GetTestConsumer());
        rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
        rr->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        rr->add_supported_codecs(Ydb::PersQueue::V1::CODEC_GZIP);
        rr->add_supported_codecs(Ydb::PersQueue::V1::CODEC_ZSTD);
        rr->set_version(1);

        Ydb::PersQueue::V1::AlterTopicResponse response;
        grpc::ClientContext rcontext;
        auto status = stub->AlterTopic(&rcontext, request, &response);
        UNIT_ASSERT(status.ok());
        Ydb::PersQueue::V1::AlterTopicResult result;
        response.operation().result().UnpackTo(&result);
        Cerr << "Alter topic response: " << response << "\nAlter result: " << result << "\n";
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    void Write(TPersQueueYdbSdkTestSetup& setup, const TVector<TString>& messages, ECodec codec) {
        auto& client = setup.GetPersQueueClient();
        TWriteSessionSettings writeSettings = setup.GetWriteSessionSettings();
        writeSettings.Codec(codec);
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        for (auto& message : messages) {
            auto result = session->Write(message);
            UNIT_ASSERT(result);
        }
        session->Close();
    }

    void Read(
        TPersQueueYdbSdkTestSetup& setup,
        const TVector<TString> messages,
        const TVector<ECodec> codecs,
        bool decompress
    ) {
        Cerr << Endl << "Start read" << Endl << Endl;
        auto readSettings = setup.GetReadSessionSettings();
        readSettings.Decompress(decompress);
        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        auto totalReceived = 0u;
        readSettings.EventHandlers_.SimpleDataHandlers(
            [&](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& ev) {
                Cerr << Endl << Endl << Endl << "Got messages" << Endl << Endl;
                if (decompress) {
                    UNIT_ASSERT_NO_EXCEPTION(ev.GetMessages());
                    UNIT_ASSERT_EXCEPTION(ev.GetCompressedMessages(), yexception);
                    for (auto& message : ev.GetMessages()) {
                        UNIT_ASSERT_VALUES_EQUAL(message.GetData(), messages[totalReceived]);
                        ++totalReceived;
                    }
                } else {
                    UNIT_ASSERT_EXCEPTION(ev.GetMessages(), yexception);
                    UNIT_ASSERT_NO_EXCEPTION(ev.GetCompressedMessages());
                    for (auto& message : ev.GetCompressedMessages()) {
                        UNIT_ASSERT_VALUES_EQUAL(message.GetCodec(), codecs[totalReceived]);
                        UNIT_ASSERT_VALUES_EQUAL(message.GetData(), messages[totalReceived]);
                        ++totalReceived;
                    }
                }
                Cerr << Endl << "totalReceived: " << totalReceived << " wait: " << messages.size() << Endl << Endl;
                if (totalReceived == messages.size())
                    checkedPromise.SetValue();
            }
        );
        auto& client = setup.GetPersQueueClient();
        auto readSession = client.CreateReadSession(readSettings);
        checkedPromise.GetFuture().GetValueSync();
    }

    void WriteWithOneCodec(TPersQueueYdbSdkTestSetup& setup, ECodec codec) {
        AlterTopic(setup); // add zstd support

        auto messages = GetTestMessages();
        Write(setup, messages, codec);
        Read(setup, messages, TVector<ECodec>(messages.size(), ECodec::RAW), true);
        Read(setup, GetTestMessages(codec), TVector<ECodec>(messages.size(), codec), false);
    }

    Y_UNIT_TEST(WriteRAW) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        WriteWithOneCodec(setup, ECodec::RAW);
    }

    Y_UNIT_TEST(WriteGZIP) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        WriteWithOneCodec(setup, ECodec::GZIP);
    }

    Y_UNIT_TEST(WriteZSTD) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        WriteWithOneCodec(setup, ECodec::ZSTD);
    }

    Y_UNIT_TEST(WriteWithMixedCodecs) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        AlterTopic(setup); // add zstd support

        auto messages = GetTestMessages();
        TVector<TString> originalMessages;
        TVector<TString> targetMessages;
        TVector<ECodec> targetCodecs;

        auto addToTarget = [&](ECodec codec) {
            originalMessages.insert(originalMessages.end(), messages.begin(), messages.end());
            for (auto& m : GetTestMessages(codec)) {
                targetMessages.push_back(m);
                targetCodecs.push_back(codec);
            }
            Write(setup, messages, codec);
        };

        addToTarget(ECodec::RAW);
        addToTarget(ECodec::GZIP);
        addToTarget(ECodec::ZSTD);

        Read(setup, originalMessages, TVector<ECodec>(originalMessages.size(), ECodec::RAW), true);
        Read(setup, targetMessages, targetCodecs, false);
    }
}
};
