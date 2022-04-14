#include "compressing_producer.h"
#include "local_caller.h"
#include "persqueue_p.h"

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/thread.h>

using namespace testing;

namespace NPersQueue {

class TMockProducer: public IProducerImpl {
public:
    TMockProducer()
        : IProducerImpl(nullptr, nullptr)
    {
    }

    NThreading::TFuture<TProducerCreateResponse> Start(TInstant) noexcept override {
        return NThreading::MakeFuture<TProducerCreateResponse>(MockStart());
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TProducerSeqNo seqNo, TData data) noexcept override {
        return NThreading::MakeFuture<TProducerCommitResponse>(MockWriteWithSeqNo(seqNo, data));
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TData data) noexcept override {
        return NThreading::MakeFuture<TProducerCommitResponse>(MockWrite(data));
    }

    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept override {
        promise.SetValue(MockWriteWithSeqNo(seqNo, data));
    }

    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept override {
        promise.SetValue(MockWrite(data));
    }

    NThreading::TFuture<TError> IsDead() noexcept override {
        return MockIsDead();
    }

    void Cancel() override {
    }

    MOCK_METHOD(TProducerCreateResponse, MockStart, (), ());
    MOCK_METHOD(NThreading::TFuture<TError>, MockIsDead, (), ());
    MOCK_METHOD(TProducerCommitResponse, MockWriteWithSeqNo, (TProducerSeqNo, TData), ());
    MOCK_METHOD(TProducerCommitResponse, MockWrite, (TData), ());
};

template <class TMock = TMockProducer>
struct TCompressingProducerBootstrap {
    ~TCompressingProducerBootstrap() {
        Lib->CancelObjectsAndWait();
    }

    void Create(ECodec codec = ECodec::GZIP) {
        Lib = new TPQLibPrivate(Settings);
        MockProducer = std::make_shared<TMock>();
        CompressingProducer = std::make_shared<TLocalProducerImplCaller<TCompressingProducer>>(MockProducer, codec, 3, Lib->GetSelfRefsAreDeadPtr(), Lib, Logger);
        Lib->AddToDestroySet(CompressingProducer);
    }

    void MakeOKStartResponse() {
        TWriteResponse wresp;
        wresp.MutableInit()->SetTopic("trololo");
        TProducerCreateResponse resp(std::move(wresp));
        EXPECT_CALL(*MockProducer, MockStart())
            .WillOnce(Return(resp));
    }

    void ExpectIsDeadCall() {
        EXPECT_CALL(*MockProducer, MockIsDead())
            .WillOnce(Return(DeadPromise.GetFuture()));
    }

    void Start() {
        MakeOKStartResponse();
        UNIT_ASSERT_STRINGS_EQUAL(CompressingProducer->Start().GetValueSync().Response.GetInit().GetTopic(), "trololo");
    }

    TPQLibSettings Settings;
    TIntrusivePtr<TCerrLogger> Logger = new TCerrLogger(TLOG_DEBUG);
    TIntrusivePtr<TPQLibPrivate> Lib;
    std::shared_ptr<TMock> MockProducer;
    std::shared_ptr<IProducerImpl> CompressingProducer;
    NThreading::TPromise<TError> DeadPromise = NThreading::NewPromise<TError>();
};

Y_UNIT_TEST_SUITE(TCompressingProducerTest) {
    Y_UNIT_TEST(PassesStartAndDead) {
        TCompressingProducerBootstrap<> bootstrap;
        bootstrap.Create();
        bootstrap.ExpectIsDeadCall();

        TWriteResponse wresp;
        wresp.MutableError()->SetDescription("trololo");
        TProducerCreateResponse resp(std::move(wresp));
        EXPECT_CALL(*bootstrap.MockProducer, MockStart())
            .WillOnce(Return(resp));

        auto startFuture = bootstrap.CompressingProducer->Start();
        UNIT_ASSERT_STRINGS_EQUAL(startFuture.GetValueSync().Response.GetError().GetDescription(), "trololo");

        auto isDeadFuture = bootstrap.CompressingProducer->IsDead();
        UNIT_ASSERT(isDeadFuture.Initialized());
        UNIT_ASSERT(!isDeadFuture.HasValue());

        TError err;
        err.SetDescription("42");
        bootstrap.DeadPromise.SetValue(err);

        UNIT_ASSERT_STRINGS_EQUAL(isDeadFuture.GetValueSync().GetDescription(), "42");
    }

    Y_UNIT_TEST(CallsWritesInProperOrder) {
        TCompressingProducerBootstrap<> bootstrap;
        bootstrap.Create();
        bootstrap.Start();

        TData data1 = TData("data1");
        TData expectedData1 = TData::Encode(data1, ECodec::GZIP, 3);

        TData data100 = TData::Raw("data100");
        TData expectedData100 = data100;

        TData data101 = TData("data101", ECodec::LZOP);
        TData expectedData101 = TData::Encode(data101, ECodec::GZIP, 3);

        TWriteResponse wresp;
        wresp.MutableAck();

        InSequence seq;
        EXPECT_CALL(*bootstrap.MockProducer, MockWriteWithSeqNo(1, expectedData1))
            .WillOnce(Return(TProducerCommitResponse(1, expectedData1, TWriteResponse(wresp))));

        EXPECT_CALL(*bootstrap.MockProducer, MockWriteWithSeqNo(100, expectedData100))
            .WillOnce(Return(TProducerCommitResponse(100, expectedData100, TWriteResponse(wresp))));

        EXPECT_CALL(*bootstrap.MockProducer, MockWrite(expectedData101))
            .WillOnce(Return(TProducerCommitResponse(101, expectedData101, TWriteResponse(wresp))));

        auto write1 = bootstrap.CompressingProducer->Write(1, data1);
        auto write100 = bootstrap.CompressingProducer->Write(100, data100);
        auto write101 = bootstrap.CompressingProducer->Write(data101);

        write1.GetValueSync();
        write100.GetValueSync();
        write101.GetValueSync();
    }

    class TMockProducerWithReorder: public TMockProducer {
    public:
        using TMockProducer::TMockProducer;
        using TMockProducer::Write;

        NThreading::TFuture<TProducerCommitResponse> Write(TProducerSeqNo seqNo, TData data) noexcept override {
            return PromiseWriteWithSeqNo(seqNo, data);
        }

        MOCK_METHOD(NThreading::TFuture<TProducerCommitResponse>, PromiseWriteWithSeqNo, (TProducerSeqNo, TData), ());
    };

    void SignalsAllFuturesInProperOrderImpl(bool death) {
        size_t lastWritten = 0;

        TCompressingProducerBootstrap<TMockProducerWithReorder> bootstrap;
        bootstrap.Create();
        bootstrap.Start();

        const size_t count = 200;
        NThreading::TFuture<TProducerCommitResponse> responses[count];
        TData datas[count];

        TWriteResponse wresp;
        wresp.MutableAck();
        EXPECT_CALL(*bootstrap.MockProducer, PromiseWriteWithSeqNo(_, _))
            .Times(AtLeast(0))
            .WillRepeatedly(Return(NThreading::MakeFuture<TProducerCommitResponse>(TProducerCommitResponse(1, TData("data"), TWriteResponse(wresp)))));

        NThreading::TPromise<TProducerCommitResponse> promise = NThreading::NewPromise<TProducerCommitResponse>();

        if (death) {
            EXPECT_CALL(*bootstrap.MockProducer, PromiseWriteWithSeqNo(50, _))
                .Times(AtLeast(0))
                .WillRepeatedly(Return(promise.GetFuture()));
        } else {
            EXPECT_CALL(*bootstrap.MockProducer, PromiseWriteWithSeqNo(50, _))
                .Times(1)
                .WillRepeatedly(Return(promise.GetFuture()));
        }

        TString bigString(10000, 'A');
        for (size_t i = 0; i < count; ++i) {
            TData data(TStringBuilder() << bigString << i, ECodec::LZOP);
            datas[i] = data;
        }

        auto testThreadId = TThread::CurrentThreadId(), prevThreadId = 0ul;
        for (size_t i = 0; i < count; ++i) {
            size_t seqNo = i + 1;
            responses[i] = bootstrap.CompressingProducer->Write(i + 1, datas[i]);
            auto onSignal = [&lastWritten, &prevThreadId, seqNo, testThreadId](const auto&) {
                // proper thread
                auto curId = TThread::CurrentThreadId();
                if (prevThreadId != 0) {
                    UNIT_ASSERT_C(curId == prevThreadId || curId == testThreadId, // could be executed in unittest thread if future ia already signalled or in object thread
                                  "prevThreadId: " << Hex(prevThreadId) << ", curId: " << Hex(curId) << ", testThreadId: " << Hex(testThreadId));
                } else if (curId != testThreadId) {
                    prevThreadId = curId;
                }

                // proper order
                UNIT_ASSERT_VALUES_EQUAL(seqNo, lastWritten + 1);
                lastWritten = seqNo;
            };
            responses[i].Subscribe(onSignal);
        }

        if (death) {
            bootstrap.CompressingProducer = nullptr;
        }

        responses[45].GetValueSync();
        promise.SetValue(TProducerCommitResponse(49, TData("data"), TWriteResponse(wresp)));
        responses[count - 1].GetValueSync();
        for (size_t i = count - 1; i > 0; --i) {
            UNIT_ASSERT(responses[i - 1].HasValue());
        }
    }

    Y_UNIT_TEST(SignalsAllFuturesInProperOrderDuringDeath) {
        SignalsAllFuturesInProperOrderImpl(true);
    }

    Y_UNIT_TEST(SignalsAllFuturesInProperOrder) {
        SignalsAllFuturesInProperOrderImpl(false);
    }
}
} // namespace NPersQueue
