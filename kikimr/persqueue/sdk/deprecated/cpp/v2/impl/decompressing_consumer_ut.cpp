#include "decompressing_consumer.h"
#include "local_caller.h"
#include "persqueue_p.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace testing;

namespace NPersQueue {

class TMockConsumer: public IConsumerImpl {
public:
    TMockConsumer()
        : IConsumerImpl(nullptr, nullptr)
    {
    }

    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant) noexcept override {
        return NThreading::MakeFuture<TConsumerCreateResponse>(MockStart());
    }

    NThreading::TFuture<TError> IsDead() noexcept override {
        return MockIsDead();
    }

    NThreading::TFuture<TConsumerMessage> GetNextMessage() noexcept override {
        return NThreading::MakeFuture<TConsumerMessage>(MockGetNextMessage());
    }

    void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept override {
        promise.SetValue(MockGetNextMessage());
    }

    void Commit(const TVector<ui64>&) noexcept override {
    }

    void RequestPartitionStatus(const TString&, ui64, ui64) noexcept override {
    }

    void Cancel() override {
    }

    MOCK_METHOD(TConsumerCreateResponse, MockStart, (), ());
    MOCK_METHOD(NThreading::TFuture<TError>, MockIsDead, (), ());
    MOCK_METHOD(TConsumerMessage, MockGetNextMessage, (), ());
};

template <class TMock = TMockConsumer>
struct TDecompressingConsumerBootstrap {
    ~TDecompressingConsumerBootstrap() {
        Lib->CancelObjectsAndWait();
    }

    void Create() {
        Lib = new TPQLibPrivate(PQLibSettings);
        MockConsumer = std::make_shared<TMock>();
        DecompressingConsumer = std::make_shared<TLocalConsumerImplCaller<TDecompressingConsumer>>(MockConsumer, Settings, Lib->GetSelfRefsAreDeadPtr(), Lib, Logger);
        Lib->AddToDestroySet(DecompressingConsumer);
    }

    void MakeOKStartResponse() {
        TReadResponse rresp;
        rresp.MutableInit()->SetSessionId("test-session");
        TConsumerCreateResponse resp(std::move(rresp));
        EXPECT_CALL(*MockConsumer, MockStart())
            .WillOnce(Return(resp));
    }

    void ExpectIsDeadCall() {
        EXPECT_CALL(*MockConsumer, MockIsDead())
            .WillOnce(Return(DeadPromise.GetFuture()));
    }

    void Start() {
        MakeOKStartResponse();
        UNIT_ASSERT_STRINGS_EQUAL(DecompressingConsumer->Start().GetValueSync().Response.GetInit().GetSessionId(), "test-session");
    }

    TConsumerSettings Settings;
    TPQLibSettings PQLibSettings;
    TIntrusivePtr<TCerrLogger> Logger = new TCerrLogger(TLOG_DEBUG);
    TIntrusivePtr<TPQLibPrivate> Lib;
    std::shared_ptr<TMock> MockConsumer;
    std::shared_ptr<IConsumerImpl> DecompressingConsumer;
    NThreading::TPromise<TError> DeadPromise = NThreading::NewPromise<TError>();
};

Y_UNIT_TEST_SUITE(TDecompressingConsumerTest) {
    Y_UNIT_TEST(DiesOnDeadSubconsumer) {
        TDecompressingConsumerBootstrap<> bootstrap;
        bootstrap.Create();
        bootstrap.ExpectIsDeadCall();
        bootstrap.Start();

        auto isDead = bootstrap.DecompressingConsumer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        bootstrap.DeadPromise.SetValue(TError());

        isDead.GetValueSync(); // doesn't hang on
    }

    Y_UNIT_TEST(PassesNonData) {
        TDecompressingConsumerBootstrap<> bootstrap;
        bootstrap.Create();
        bootstrap.ExpectIsDeadCall();
        bootstrap.Start();

        InSequence sequence;
        {
            TReadResponse resp;
            resp.MutableCommit()->AddCookie(42);

            TConsumerMessage ret(std::move(resp));

            EXPECT_CALL(*bootstrap.MockConsumer, MockGetNextMessage())
                .WillOnce(Return(ret));
        }

        {
            TReadResponse resp;
            resp.MutableRelease()->SetTopic("topic!");

            TConsumerMessage ret(std::move(resp));

            EXPECT_CALL(*bootstrap.MockConsumer, MockGetNextMessage())
                .WillOnce(Return(ret));
        }

        auto passed1 = bootstrap.DecompressingConsumer->GetNextMessage().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(passed1.Response.GetCommit().CookieSize(), 1, passed1.Response);
        UNIT_ASSERT_VALUES_EQUAL_C(passed1.Response.GetCommit().GetCookie(0), 42, passed1.Response);

        auto passed2 = bootstrap.DecompressingConsumer->GetNextMessage().GetValueSync();
        UNIT_ASSERT_STRINGS_EQUAL_C(passed2.Response.GetRelease().GetTopic(), "topic!", passed2.Response);
    }

    void ProcessesBrokenChunks(bool skip) {
        TDecompressingConsumerBootstrap<> bootstrap;
        bootstrap.Settings.SkipBrokenChunks = skip;
        bootstrap.Create();
        bootstrap.ExpectIsDeadCall();
        bootstrap.Start();

        TReadResponse resp;
        auto* msg = resp.MutableData()->AddMessageBatch()->AddMessage();
        msg->SetData("hjdhkjhkjhshqsiuhqisuqihsi;");
        msg->MutableMeta()->SetCodec(ECodec::LZOP);

        EXPECT_CALL(*bootstrap.MockConsumer, MockGetNextMessage())
            .WillOnce(Return(TConsumerMessage(std::move(resp))));

        auto isDead = bootstrap.DecompressingConsumer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        auto passed = bootstrap.DecompressingConsumer->GetNextMessage().GetValueSync();

        if (skip) {
            UNIT_ASSERT(!passed.Response.HasError());
            isDead.HasValue();
            UNIT_ASSERT_VALUES_EQUAL(passed.Response.GetData().MessageBatchSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(passed.Response.GetData().GetMessageBatch(0).MessageSize(), 1);
            UNIT_ASSERT(passed.Response.GetData().GetMessageBatch(0).GetMessage(0).GetData().empty());
        } else {
            UNIT_ASSERT(passed.Response.HasError());
            isDead.GetValueSync();

            UNIT_ASSERT(bootstrap.DecompressingConsumer->GetNextMessage().GetValueSync().Response.HasError());
        }

        //DestroyAndWait(bootstrap.DecompressingConsumer);
    }

    Y_UNIT_TEST(DiesOnBrokenChunks) {
        ProcessesBrokenChunks(false);
    }

    Y_UNIT_TEST(SkipsBrokenChunks) {
        ProcessesBrokenChunks(true);
    }

    static void AddMessage(TReadResponse::TData::TMessageBatch* batch, const TString& sourceData, ECodec codec = ECodec::RAW) {
        auto* msg = batch->AddMessage();
        msg->MutableMeta()->SetCodec(codec);
        if (codec == ECodec::RAW) {
            msg->SetData(sourceData);
        } else {
            msg->SetData(TData::Encode(sourceData, codec, -1).GetEncodedData());
        }
    }

    Y_UNIT_TEST(DecompessesDataInProperChuncksOrder) {
        TDecompressingConsumerBootstrap<> bootstrap;
        bootstrap.Create();
        bootstrap.ExpectIsDeadCall();
        bootstrap.Start();

        InSequence sequence;
        {
            TReadResponse resp;
            auto* batch = resp.MutableData()->AddMessageBatch();
            AddMessage(batch, "message1", ECodec::LZOP);
            AddMessage(batch, "message2");
            AddMessage(batch, "message3", ECodec::GZIP);

            resp.MutableData()->AddMessageBatch();

            batch = resp.MutableData()->AddMessageBatch();
            AddMessage(batch, "messageA", ECodec::LZOP);
            AddMessage(batch, "messageB", ECodec::ZSTD);
            AddMessage(batch, "messageC");

            EXPECT_CALL(*bootstrap.MockConsumer, MockGetNextMessage())
                .WillOnce(Return(TConsumerMessage(std::move(resp))));
        }

        {
            TReadResponse resp;
            AddMessage(resp.MutableData()->AddMessageBatch(), "trololo", ECodec::LZOP);

            EXPECT_CALL(*bootstrap.MockConsumer, MockGetNextMessage())
                .WillOnce(Return(TConsumerMessage(std::move(resp))));
        }

        auto isDead = bootstrap.DecompressingConsumer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        auto f1 = bootstrap.DecompressingConsumer->GetNextMessage();
        auto f2 = bootstrap.DecompressingConsumer->GetNextMessage();

        auto data1 = f1.GetValueSync().Response.GetData();
        auto data2 = f2.GetValueSync().Response.GetData();

        UNIT_ASSERT_VALUES_EQUAL(data1.MessageBatchSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(0).MessageSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(1).MessageSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(2).MessageSize(), 3);

        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(0).GetMessage(0).GetData(), "message1");
        UNIT_ASSERT_EQUAL(data1.GetMessageBatch(0).GetMessage(0).GetMeta().GetCodec(), ECodec::RAW);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(0).GetMessage(1).GetData(), "message2");
        UNIT_ASSERT_EQUAL(data1.GetMessageBatch(0).GetMessage(1).GetMeta().GetCodec(), ECodec::RAW);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(0).GetMessage(2).GetData(), "message3");
        UNIT_ASSERT_EQUAL(data1.GetMessageBatch(0).GetMessage(2).GetMeta().GetCodec(), ECodec::RAW);

        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(2).GetMessage(0).GetData(), "messageA");
        UNIT_ASSERT_EQUAL(data1.GetMessageBatch(2).GetMessage(0).GetMeta().GetCodec(), ECodec::RAW);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(2).GetMessage(1).GetData(), "messageB");
        UNIT_ASSERT_EQUAL(data1.GetMessageBatch(2).GetMessage(1).GetMeta().GetCodec(), ECodec::RAW);
        UNIT_ASSERT_VALUES_EQUAL(data1.GetMessageBatch(2).GetMessage(2).GetData(), "messageC");
        UNIT_ASSERT_EQUAL(data1.GetMessageBatch(2).GetMessage(2).GetMeta().GetCodec(), ECodec::RAW);


        UNIT_ASSERT_VALUES_EQUAL(data2.MessageBatchSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data2.GetMessageBatch(0).MessageSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data2.GetMessageBatch(0).GetMessage(0).GetData(), "trololo");
        UNIT_ASSERT_EQUAL(data2.GetMessageBatch(0).GetMessage(0).GetMeta().GetCodec(), ECodec::RAW);


        UNIT_ASSERT(!isDead.HasValue());
    }
}
} // namespace NPersQueue
