#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/cast.h>

using namespace NYql::NPureCalc;

namespace {
    class TStringMessageStreamImpl: public IStream<NPureCalcProto::TStringMessage*> {
    private:
        ui32 I_ = 0;
        NPureCalcProto::TStringMessage Message_{};

    public:
        NPureCalcProto::TStringMessage* Fetch() override {
            if (I_ >= 3) {
                return nullptr;
            } else {
                Message_.SetX(ToString(I_));
                ++I_;
                return &Message_;
            }
        }
    };

    class TStringMessageConsumerImpl: public IConsumer<NPureCalcProto::TStringMessage*> {
    private:
        TVector<TString>* Buf_;

    public:
        TStringMessageConsumerImpl(TVector<TString>* buf)
            : Buf_(buf)
        {
        }

    public:
        void OnObject(NPureCalcProto::TStringMessage* t) override {
            Buf_->push_back(t->GetX());
        }

        void OnFinish() override {
        }
    };

}

Y_UNIT_TEST_SUITE(TestWorkerPool) {
    static TString sql = "SELECT 'abc'u || X AS X FROM Input";

    static TVector<TString> expected{"abc0", "abc1", "abc2"};

    void TestPullStreamImpl(bool useWorkerPool) {
        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseWorkerPool(useWorkerPool));

        auto program = factory->MakePullStreamProgram(
            TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
            TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
            sql,
            ETranslationMode::SQL
        );

        auto check = [](IStream<NPureCalcProto::TStringMessage*>* output) {
            TVector<TString> actual;
            while (auto *x = output->Fetch()) {
                actual.push_back(x->GetX());
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, actual);
        };

        // Sequential use
        for (size_t i = 0; i < 2; ++i) {
            auto output = program->Apply(MakeHolder<TStringMessageStreamImpl>());
            check(output.Get());
        }
        // Parallel use
        {
            auto output1 = program->Apply(MakeHolder<TStringMessageStreamImpl>());
            auto output2 = program->Apply(MakeHolder<TStringMessageStreamImpl>());
            check(output1.Get());
            check(output2.Get());
        }
    }

    Y_UNIT_TEST(TestPullStreamUseWorkerPool) {
        TestPullStreamImpl(true);
    }

    Y_UNIT_TEST(TestPullStreamNoWorkerPool) {
        TestPullStreamImpl(false);
    }

    void TestPullListImpl(bool useWorkerPool) {
        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseWorkerPool(useWorkerPool));

        auto program = factory->MakePullListProgram(
            TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
            TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
            sql,
            ETranslationMode::SQL
        );

        auto check = [](IStream<NPureCalcProto::TStringMessage*>* output) {
            TVector<TString> actual;
            while (auto *x = output->Fetch()) {
                actual.push_back(x->GetX());
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, actual);
        };

        // Sequential use
        for (size_t i = 0; i < 2; ++i) {
            auto output = program->Apply(MakeHolder<TStringMessageStreamImpl>());
            check(output.Get());
        }
        // Parallel use
        {
            auto output1 = program->Apply(MakeHolder<TStringMessageStreamImpl>());
            auto output2 = program->Apply(MakeHolder<TStringMessageStreamImpl>());
            check(output1.Get());
            check(output2.Get());
        }
    }

    Y_UNIT_TEST(TestPullListUseWorkerPool) {
        TestPullListImpl(true);
    }

    Y_UNIT_TEST(TestPullListNoWorkerPool) {
        TestPullListImpl(false);
    }

    void TestPushStreamImpl(bool useWorkerPool) {
        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseWorkerPool(useWorkerPool));

        auto program = factory->MakePushStreamProgram(
            TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
            TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
            sql,
            ETranslationMode::SQL
        );

        auto check = [](IConsumer<NPureCalcProto::TStringMessage*>* input, const TVector<TString>& result) {
            NPureCalcProto::TStringMessage message;
            for (auto s: {"0", "1", "2"}) {
                message.SetX(s);
                input->OnObject(&message);
            }
            input->OnFinish();

            UNIT_ASSERT_VALUES_EQUAL(expected, result);
        };

        // Sequential use
        for (size_t i = 0; i < 2; ++i) {
            TVector<TString> actual;
            auto input = program->Apply(MakeHolder<TStringMessageConsumerImpl>(&actual));
            check(input.Get(), actual);
        }

        // Parallel use
        {
            TVector<TString> actual1;
            auto input1 = program->Apply(MakeHolder<TStringMessageConsumerImpl>(&actual1));
            TVector<TString> actual2;
            auto input2 = program->Apply(MakeHolder<TStringMessageConsumerImpl>(&actual2));
            check(input1.Get(), actual1);
            check(input2.Get(), actual2);
        }
    }

    Y_UNIT_TEST(TestPushStreamUseWorkerPool) {
        TestPushStreamImpl(true);
    }

    Y_UNIT_TEST(TestPushStreamNoWorkerPool) {
        TestPushStreamImpl(false);
    }
}
