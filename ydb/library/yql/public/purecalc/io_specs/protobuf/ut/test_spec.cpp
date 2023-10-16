#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <util/generic/xrange.h>

namespace {
    TMaybe<NPureCalcProto::TAllTypes> allTypesMessage;

    NPureCalcProto::TAllTypes& GetCanonicalMessage() {
        if (!allTypesMessage) {
            allTypesMessage = NPureCalcProto::TAllTypes();

            allTypesMessage->SetFDouble(1);
            allTypesMessage->SetFFloat(2);
            allTypesMessage->SetFInt64(3);
            allTypesMessage->SetFSfixed64(4);
            allTypesMessage->SetFSint64(5);
            allTypesMessage->SetFUint64(6);
            allTypesMessage->SetFFixed64(7);
            allTypesMessage->SetFInt32(8);
            allTypesMessage->SetFSfixed32(9);
            allTypesMessage->SetFSint32(10);
            allTypesMessage->SetFUint32(11);
            allTypesMessage->SetFFixed32(12);
            allTypesMessage->SetFBool(true);
            allTypesMessage->SetFString("asd");
            allTypesMessage->SetFBytes("dsa");
        }

        return allTypesMessage.GetRef();
    }

    template <typename T1, typename T2>
    void AssertEqualToCanonical(const T1& got, const T2& expected) {
        UNIT_ASSERT_EQUAL(expected.GetFDouble(), got.GetFDouble());
        UNIT_ASSERT_EQUAL(expected.GetFFloat(), got.GetFFloat());
        UNIT_ASSERT_EQUAL(expected.GetFInt64(), got.GetFInt64());
        UNIT_ASSERT_EQUAL(expected.GetFSfixed64(), got.GetFSfixed64());
        UNIT_ASSERT_EQUAL(expected.GetFSint64(), got.GetFSint64());
        UNIT_ASSERT_EQUAL(expected.GetFUint64(), got.GetFUint64());
        UNIT_ASSERT_EQUAL(expected.GetFFixed64(), got.GetFFixed64());
        UNIT_ASSERT_EQUAL(expected.GetFInt32(), got.GetFInt32());
        UNIT_ASSERT_EQUAL(expected.GetFSfixed32(), got.GetFSfixed32());
        UNIT_ASSERT_EQUAL(expected.GetFSint32(), got.GetFSint32());
        UNIT_ASSERT_EQUAL(expected.GetFUint32(), got.GetFUint32());
        UNIT_ASSERT_EQUAL(expected.GetFFixed32(), got.GetFFixed32());
        UNIT_ASSERT_EQUAL(expected.GetFBool(), got.GetFBool());
        UNIT_ASSERT_EQUAL(expected.GetFString(), got.GetFString());
        UNIT_ASSERT_EQUAL(expected.GetFBytes(), got.GetFBytes());
    }

    template <typename T>
    void AssertEqualToCanonical(const T& got) {
        AssertEqualToCanonical(got, GetCanonicalMessage());
    }

    TString SerializeToTextFormatAsString(const google::protobuf::Message& message) {
        TString result;
        {
            TStringOutput output(result);
            SerializeToTextFormat(message, output);
        }
        return result;
    }

    template <typename T>
    void AssertProtoEqual(const T& actual, const T& expected) {
        UNIT_ASSERT_VALUES_EQUAL(SerializeToTextFormatAsString(actual), SerializeToTextFormatAsString(expected));
    }
}

class TAllTypesStreamImpl: public NYql::NPureCalc::IStream<NPureCalcProto::TAllTypes*> {
private:
    int I_ = 0;
    NPureCalcProto::TAllTypes Message_ = GetCanonicalMessage();

public:
    NPureCalcProto::TAllTypes* Fetch() override {
        if (I_ > 0) {
            return nullptr;
        } else {
            I_ += 1;
            return &Message_;
        }
    }
};

class TSimpleMessageStreamImpl: public NYql::NPureCalc::IStream<NPureCalcProto::TSimpleMessage*> {
public:
    TSimpleMessageStreamImpl(i32 value)
    {
        Message_.SetX(value);
    }

    NPureCalcProto::TSimpleMessage* Fetch() override {
        if (Exhausted_) {
            return nullptr;
        } else {
            Exhausted_ = true;
            return &Message_;
        }
    }

private:
    NPureCalcProto::TSimpleMessage Message_;
    bool Exhausted_ = false;
};

class TAllTypesConsumerImpl: public NYql::NPureCalc::IConsumer<NPureCalcProto::TAllTypes*> {
private:
    int I_ = 0;

public:
    void OnObject(NPureCalcProto::TAllTypes* t) override {
        I_ += 1;
        AssertEqualToCanonical(*t);
    }

    void OnFinish() override {
        UNIT_ASSERT(I_ > 0);
    }
};

class TStringMessageStreamImpl: public NYql::NPureCalc::IStream<NPureCalcProto::TStringMessage*> {
private:
    int I_ = 0;
    NPureCalcProto::TStringMessage Message_{};

public:
    NPureCalcProto::TStringMessage* Fetch() override {
        if (I_ >= 3) {
            return nullptr;
        } else {
            Message_.SetX(TString("-") * I_);
            I_ += 1;
            return &Message_;
        }
    }
};

class TSimpleMessageConsumerImpl: public NYql::NPureCalc::IConsumer<NPureCalcProto::TSimpleMessage*> {
private:
    TVector<int>* Buf_;

public:
    TSimpleMessageConsumerImpl(TVector<int>* buf)
        : Buf_(buf)
    {
    }

public:
    void OnObject(NPureCalcProto::TSimpleMessage* t) override {
        Buf_->push_back(t->GetX());
    }

    void OnFinish() override {
        Buf_->push_back(-100);
    }
};

using TMessagesVariant = std::variant<NPureCalcProto::TSplitted1*, NPureCalcProto::TSplitted2*, NPureCalcProto::TStringMessage*>;

class TVariantConsumerImpl: public NYql::NPureCalc::IConsumer<TMessagesVariant> {
public:
    using TType0 = TVector<std::pair<i32, TString>>;
    using TType1 = TVector<std::pair<ui32, TString>>;
    using TType2 = TVector<TString>;

public:
    TVariantConsumerImpl(TType0* q0, TType1* q1, TType2* q2, int* v)
        : Queue0_(q0)
        , Queue1_(q1)
        , Queue2_(q2)
        , Value_(v)
    {
    }

    void OnObject(TMessagesVariant value) override {
        if (auto* p = std::get_if<0>(&value)) {
            Queue0_->push_back({(*p)->GetBInt(), std::move(*(*p)->MutableBString())});
        } else if (auto* p = std::get_if<1>(&value)) {
            Queue1_->push_back({(*p)->GetCUint(), std::move(*(*p)->MutableCString())});
        } else if (auto* p = std::get_if<2>(&value)) {
            Queue2_->push_back(std::move(*(*p)->MutableX()));
        } else {
            Y_ABORT("invalid variant alternative");
        }
    }

    void OnFinish() override {
        *Value_ = 42;
    }

private:
    TType0* Queue0_;
    TType1* Queue1_;
    TType2* Queue2_;
    int* Value_;
};

class TUnsplittedStreamImpl: public NYql::NPureCalc::IStream<NPureCalcProto::TUnsplitted*> {
public:
    TUnsplittedStreamImpl()
    {
        Message_.SetAInt(-23);
        Message_.SetAUint(111);
        Message_.SetAString("Hello!");
    }

public:
    NPureCalcProto::TUnsplitted* Fetch() override {
        switch (I_) {
            case 0:
                ++I_;
                return &Message_;
            case 1:
                ++I_;
                Message_.SetABool(false);
                return &Message_;
            case 2:
                ++I_;
                Message_.SetABool(true);
                return &Message_;
            default:
                return nullptr;
        }
    }

private:
    NPureCalcProto::TUnsplitted Message_;
    ui32 I_ = 0;
};

template<typename T>
struct TVectorConsumer: public NYql::NPureCalc::IConsumer<T*> {
    TVector<T> Data;

    void OnObject(T* t) override {
        Data.push_back(*t);
    }

    void OnFinish() override {
    }
};

template <typename T>
struct TVectorStream: public NYql::NPureCalc::IStream<T*> {
    TVector<T> Data;
    size_t Index = 0;

public:
    T* Fetch() override {
        return Index < Data.size() ? &Data[Index++] : nullptr;
    }
};

Y_UNIT_TEST_SUITE(TestProtoIO) {
    Y_UNIT_TEST(TestAllTypes) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        {
            auto program = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TAllTypes>(),
                TProtobufOutputSpec<NPureCalcProto::TAllTypes>(),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            auto stream = program->Apply(MakeHolder<TAllTypesStreamImpl>());

            NPureCalcProto::TAllTypes* message;

            UNIT_ASSERT(message = stream->Fetch());
            AssertEqualToCanonical(*message);
            UNIT_ASSERT(!stream->Fetch());
        }

        {
            auto program = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TAllTypes>(),
                TProtobufOutputSpec<NPureCalcProto::TAllTypes>(),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            auto stream = program->Apply(MakeHolder<TAllTypesStreamImpl>());

            NPureCalcProto::TAllTypes* message;

            UNIT_ASSERT(message = stream->Fetch());
            AssertEqualToCanonical(*message);
            UNIT_ASSERT(!stream->Fetch());
        }

        {
            auto program = factory->MakePushStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TAllTypes>(),
                TProtobufOutputSpec<NPureCalcProto::TAllTypes>(),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            auto consumer = program->Apply(MakeHolder<TAllTypesConsumerImpl>());

            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnObject(&GetCanonicalMessage()); }());
            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnFinish(); }());
        }
    }

    template <typename T>
    void CheckPassThroughYql(T& testInput, google::protobuf::Arena* arena = nullptr) {
        using namespace NYql::NPureCalc;

        auto resetArena = [arena]() {
            if (arena != nullptr) {
                arena->Reset();
            }
        };

        auto factory = MakeProgramFactory();

        {
            auto program = factory->MakePushStreamProgram(
                TProtobufInputSpec<T>(),
                TProtobufOutputSpec<T>({}, arena),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            auto resultConsumer = MakeHolder<TVectorConsumer<T>>();
            auto* resultConsumerPtr = resultConsumer.Get();
            auto sourceConsumer = program->Apply(std::move(resultConsumer));

            sourceConsumer->OnObject(&testInput);
            UNIT_ASSERT_VALUES_EQUAL(1, resultConsumerPtr->Data.size());
            AssertProtoEqual(resultConsumerPtr->Data[0], testInput);

            resultConsumerPtr->Data.clear();
            sourceConsumer->OnObject(&testInput);
            UNIT_ASSERT_VALUES_EQUAL(1, resultConsumerPtr->Data.size());
            AssertProtoEqual(resultConsumerPtr->Data[0], testInput);
        }
        resetArena();

        {
            auto program = factory->MakePullStreamProgram(
                TProtobufInputSpec<T>(),
                TProtobufOutputSpec<T>({}, arena),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            auto sourceStream = MakeHolder<TVectorStream<T>>();
            auto* sourceStreamPtr = sourceStream.Get();
            auto resultStream = program->Apply(std::move(sourceStream));

            sourceStreamPtr->Data.push_back(testInput);
            T* resultMessage;
            UNIT_ASSERT(resultMessage = resultStream->Fetch());
            AssertProtoEqual(*resultMessage, testInput);
            UNIT_ASSERT(!resultStream->Fetch());

            UNIT_ASSERT_VALUES_EQUAL(resultMessage->GetArena(), arena);
        }
        resetArena();

        {
            auto program = factory->MakePullListProgram(
                TProtobufInputSpec<T>(),
                TProtobufOutputSpec<T>({}, arena),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            auto sourceStream = MakeHolder<TVectorStream<T>>();
            auto* sourceStreamPtr = sourceStream.Get();
            auto resultStream = program->Apply(std::move(sourceStream));

            sourceStreamPtr->Data.push_back(testInput);
            T* resultMessage;
            UNIT_ASSERT(resultMessage = resultStream->Fetch());
            AssertProtoEqual(*resultMessage, testInput);
            UNIT_ASSERT(!resultStream->Fetch());

            UNIT_ASSERT_VALUES_EQUAL(resultMessage->GetArena(), arena);
        }
        resetArena();
    }

    template <typename T>
    void CheckMessageIsInvalid(const TString& expectedExceptionMessage) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        UNIT_ASSERT_EXCEPTION_CONTAINS([&]() {
            factory->MakePushStreamProgram(TProtobufInputSpec<T>(), TProtobufOutputSpec<T>(), "SELECT * FROM Input", ETranslationMode::SQL);
        }(), yexception, expectedExceptionMessage);

        UNIT_ASSERT_EXCEPTION_CONTAINS([&]() {
            factory->MakePullStreamProgram(TProtobufInputSpec<T>(), TProtobufOutputSpec<T>(), "SELECT * FROM Input", ETranslationMode::SQL);
        }(), yexception, expectedExceptionMessage);

        UNIT_ASSERT_EXCEPTION_CONTAINS([&]() {
            factory->MakePullListProgram(TProtobufInputSpec<T>(), TProtobufOutputSpec<T>(), "SELECT * FROM Input", ETranslationMode::SQL);
        }(), yexception, expectedExceptionMessage);
    }

    Y_UNIT_TEST(TestSimpleNested) {
        NPureCalcProto::TSimpleNested input;
        input.SetX(10);
        {
            auto* item = input.MutableY();
            *item = GetCanonicalMessage();
            item->SetFUint64(100);
        }
        CheckPassThroughYql(input);
    }

    Y_UNIT_TEST(TestOptionalNested) {
        NPureCalcProto::TOptionalNested input;
        {
            auto* item = input.MutableX();
            *item = GetCanonicalMessage();
            item->SetFUint64(100);
        }
        CheckPassThroughYql(input);
    }

    Y_UNIT_TEST(TestSimpleRepeated) {
        NPureCalcProto::TSimpleRepeated input;
        input.SetX(20);
        input.AddY(100);
        input.AddY(200);
        input.AddY(300);
        CheckPassThroughYql(input);
    }

    Y_UNIT_TEST(TestNestedRepeated) {
        NPureCalcProto::TNestedRepeated input;
        input.SetX(20);
        {
            auto* item = input.MutableY()->Add();
            item->SetX(100);
            {
                auto* y = item->MutableY();
                *y = GetCanonicalMessage();
                y->SetFUint64(1000);
            }
        }
        {
            auto* item = input.MutableY()->Add();
            item->SetX(200);
            {
                auto* y = item->MutableY();
                *y = GetCanonicalMessage();
                y->SetFUint64(2000);
            }
        }
        CheckPassThroughYql(input);
    }

    Y_UNIT_TEST(TestMessageWithEnum) {
        NPureCalcProto::TMessageWithEnum input;
        input.AddEnumValue(NPureCalcProto::TMessageWithEnum::VALUE1);
        input.AddEnumValue(NPureCalcProto::TMessageWithEnum::VALUE2);
        CheckPassThroughYql(input);
    }

    Y_UNIT_TEST(TestRecursive) {
        CheckMessageIsInvalid<NPureCalcProto::TRecursive>("NPureCalcProto.TRecursive->NPureCalcProto.TRecursive");
    }

    Y_UNIT_TEST(TestRecursiveIndirectly) {
        CheckMessageIsInvalid<NPureCalcProto::TRecursiveIndirectly>(
                "NPureCalcProto.TRecursiveIndirectly->NPureCalcProto.TRecursiveIndirectly.TNested->NPureCalcProto.TRecursiveIndirectly");
    }

    Y_UNIT_TEST(TestColumnsFilter) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        auto filter = THashSet<TString>({"FFixed64", "FBool", "FBytes"});

        NPureCalcProto::TOptionalAllTypes canonicalMessage;
        canonicalMessage.SetFFixed64(GetCanonicalMessage().GetFFixed64());
        canonicalMessage.SetFBool(GetCanonicalMessage().GetFBool());
        canonicalMessage.SetFBytes(GetCanonicalMessage().GetFBytes());

        {
            auto inputSpec = TProtobufInputSpec<NPureCalcProto::TAllTypes>();
            auto outputSpec = TProtobufOutputSpec<NPureCalcProto::TOptionalAllTypes>();
            outputSpec.SetOutputColumnsFilter(filter);

            auto program = factory->MakePullStreamProgram(
                inputSpec,
                outputSpec,
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            UNIT_ASSERT_EQUAL(program->GetUsedColumns(), filter);

            auto stream = program->Apply(MakeHolder<TAllTypesStreamImpl>());

            NPureCalcProto::TOptionalAllTypes* message;

            UNIT_ASSERT(message = stream->Fetch());
            AssertEqualToCanonical(*message, canonicalMessage);
            UNIT_ASSERT(!stream->Fetch());
        }
    }

    Y_UNIT_TEST(TestColumnsFilterWithOptionalFields) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        auto fields = THashSet<TString>({"FFixed64", "FBool", "FBytes"});

        NPureCalcProto::TOptionalAllTypes canonicalMessage;
        canonicalMessage.SetFFixed64(GetCanonicalMessage().GetFFixed64());
        canonicalMessage.SetFBool(GetCanonicalMessage().GetFBool());
        canonicalMessage.SetFBytes(GetCanonicalMessage().GetFBytes());

        {
            auto program = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TAllTypes>(),
                TProtobufOutputSpec<NPureCalcProto::TOptionalAllTypes>(),
                "SELECT FFixed64, FBool, FBytes FROM Input",
                ETranslationMode::SQL
            );

            UNIT_ASSERT_EQUAL(program->GetUsedColumns(), fields);

            auto stream = program->Apply(MakeHolder<TAllTypesStreamImpl>());

            NPureCalcProto::TOptionalAllTypes* message;

            UNIT_ASSERT(message = stream->Fetch());
            AssertEqualToCanonical(*message, canonicalMessage);
            UNIT_ASSERT(!stream->Fetch());
        }

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TAllTypes>(),
                TProtobufOutputSpec<NPureCalcProto::TAllTypes>(),
                "SELECT FFixed64, FBool, FBytes FROM Input",
                ETranslationMode::SQL
            );
        }(), TCompileError, "Failed to optimize");
    }

    Y_UNIT_TEST(TestUsedColumns) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        auto allFields = THashSet<TString>();

        for (auto i: xrange(NPureCalcProto::TOptionalAllTypes::descriptor()->field_count())) {
            allFields.emplace(NPureCalcProto::TOptionalAllTypes::descriptor()->field(i)->name());
        }

        {
            auto program = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TAllTypes>(),
                TProtobufOutputSpec<NPureCalcProto::TOptionalAllTypes>(),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            UNIT_ASSERT_EQUAL(program->GetUsedColumns(), allFields);
        }
    }

    Y_UNIT_TEST(TestChaining) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        TString sql1 = "SELECT UNWRAP(X || CAST(\"HI\" AS Utf8)) AS X FROM Input";
        TString sql2 = "SELECT LENGTH(X) AS X FROM Input";

        {
            auto program1 = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                sql1,
                ETranslationMode::SQL
            );

            auto program2 = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TSimpleMessage>(),
                sql2,
                ETranslationMode::SQL
            );

            auto input = MakeHolder<TStringMessageStreamImpl>();
            auto intermediate = program1->Apply(std::move(input));
            auto output = program2->Apply(std::move(intermediate));

            TVector<int> expected = {2, 3, 4};
            TVector<int> actual{};

            while (auto *x = output->Fetch()) {
                actual.push_back(x->GetX());
            }

            UNIT_ASSERT_EQUAL(expected, actual);
        }

        {
            auto program1 = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                sql1,
                ETranslationMode::SQL
            );

            auto program2 = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TSimpleMessage>(),
                sql2,
                ETranslationMode::SQL
            );

            auto input = MakeHolder<TStringMessageStreamImpl>();
            auto intermediate = program1->Apply(std::move(input));
            auto output = program2->Apply(std::move(intermediate));

            TVector<int> expected = {2, 3, 4};
            TVector<int> actual{};

            while (auto *x = output->Fetch()) {
                actual.push_back(x->GetX());
            }

            UNIT_ASSERT_EQUAL(expected, actual);
        }

        {
            auto program1 = factory->MakePushStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                sql1,
                ETranslationMode::SQL
            );

            auto program2 = factory->MakePushStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TSimpleMessage>(),
                sql2,
                ETranslationMode::SQL
            );

            TVector<int> expected = {2, 3, 4, -100};
            TVector<int> actual{};

            auto consumer = MakeHolder<TSimpleMessageConsumerImpl>(&actual);
            auto intermediate = program2->Apply(std::move(consumer));
            auto input = program1->Apply(std::move(intermediate));

            NPureCalcProto::TStringMessage Message;

            Message.SetX("");
            input->OnObject(&Message);

            Message.SetX("1");
            input->OnObject(&Message);

            Message.SetX("22");
            input->OnObject(&Message);

            input->OnFinish();

            UNIT_ASSERT_EQUAL(expected, actual);
        }
    }

    Y_UNIT_TEST(TestTimestampColumn) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory(TProgramFactoryOptions()
            .SetDeterministicTimeProviderSeed(1)); // seconds

        NPureCalcProto::TOptionalAllTypes canonicalMessage;

        {
            auto inputSpec = TProtobufInputSpec<NPureCalcProto::TAllTypes>("MyTimestamp");
            auto outputSpec = TProtobufOutputSpec<NPureCalcProto::TOptionalAllTypes>();

            auto program = factory->MakePullStreamProgram(
                inputSpec,
                outputSpec,
                "SELECT MyTimestamp AS FFixed64 FROM Input",
                ETranslationMode::SQL
            );

            auto stream = program->Apply(MakeHolder<TAllTypesStreamImpl>());

            NPureCalcProto::TOptionalAllTypes* message;

            UNIT_ASSERT(message = stream->Fetch());
            UNIT_ASSERT_VALUES_EQUAL(message->GetFFixed64(), 1000000); // microseconds
            UNIT_ASSERT(!stream->Fetch());
        }
    }

    Y_UNIT_TEST(TestTableNames) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseSystemColumns(true));

        auto runTest = [&](TStringBuf tableName, i32 value) {
            auto program = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TSimpleMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TNamedSimpleMessage>(),
                TString::Join("SELECT TableName() AS Name, X FROM ", tableName),
                ETranslationMode::SQL
            );

            auto stream = program->Apply(MakeHolder<TSimpleMessageStreamImpl>(value));
            auto message = stream->Fetch();

            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->GetX(), value);
            UNIT_ASSERT_VALUES_EQUAL(message->GetName(), tableName);
            UNIT_ASSERT(!stream->Fetch());
        };

        runTest("Input", 37);
        runTest("Input0", -23);
    }

    void CheckMultiOutputs(TMaybe<TVector<google::protobuf::Arena*>> arenas) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();
        TString sExpr = R"(
(
    (let $type (ParseType '"Variant<Struct<BInt:Int32,BString:Utf8>, Struct<CUint:Uint32,CString:Utf8>, Struct<X:Utf8>>"))
    (let $stream (Self '0))
    (return (FlatMap (Self '0) (lambda '(x) (block '(
        (let $cond (Member x 'ABool))
        (let $item0 (Variant (AsStruct '('BInt (Member x 'AInt)) '('BString (Member x 'AString))) '0 $type))
        (let $item1 (Variant (AsStruct '('CUint (Member x 'AUint)) '('CString (Member x 'AString))) '1 $type))
        (let $item2 (Variant (AsStruct '('X (Utf8 'Error))) '2 $type))
        (return (If (Exists $cond) (If (Unwrap $cond) (AsList $item0) (AsList $item1)) (AsList $item2)))
    )))))
)
        )";

        {
            auto program = factory->MakePushStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TUnsplitted>(),
                TProtobufMultiOutputSpec<NPureCalcProto::TSplitted1, NPureCalcProto::TSplitted2, NPureCalcProto::TStringMessage>(
                    {}, arenas
                ),
                sExpr,
                ETranslationMode::SExpr
            );

            TVariantConsumerImpl::TType0 queue0;
            TVariantConsumerImpl::TType1 queue1;
            TVariantConsumerImpl::TType2 queue2;
            int finalValue = 0;

            auto consumer = MakeHolder<TVariantConsumerImpl>(&queue0, &queue1, &queue2, &finalValue);
            auto input = program->Apply(std::move(consumer));

            NPureCalcProto::TUnsplitted message;
            message.SetAInt(-13);
            message.SetAUint(47);
            message.SetAString("first message");
            message.SetABool(true);

            input->OnObject(&message);
            UNIT_ASSERT(queue0.size() == 1 && queue1.empty() && queue2.empty() && finalValue == 0);

            message.SetABool(false);
            message.SetAString("second message");

            input->OnObject(&message);
            UNIT_ASSERT(queue0.size() == 1 && queue1.size() == 1 && queue2.empty() && finalValue == 0);

            message.ClearABool();

            input->OnObject(&message);
            UNIT_ASSERT(queue0.size() == 1 && queue1.size() == 1 && queue2.size() == 1 && finalValue == 0);

            input->OnFinish();
            UNIT_ASSERT(queue0.size() == 1 && queue1.size() == 1 && queue2.size() == 1 && finalValue == 42);

            TVariantConsumerImpl::TType0 expected0 = {{-13, "first message"}};
            UNIT_ASSERT_EQUAL(queue0, expected0);

            TVariantConsumerImpl::TType1 expected1 = {{47, "second message"}};
            UNIT_ASSERT_EQUAL(queue1, expected1);

            TVariantConsumerImpl::TType2 expected2 = {{"Error"}};
            UNIT_ASSERT_EQUAL(queue2, expected2);
        }

        {
            auto program1 = factory->MakePullStreamProgram(
                TProtobufInputSpec<NPureCalcProto::TUnsplitted>(),
                TProtobufMultiOutputSpec<NPureCalcProto::TSplitted1, NPureCalcProto::TSplitted2, NPureCalcProto::TStringMessage>(
                    {}, arenas
                ),
                sExpr,
                ETranslationMode::SExpr
            );

            auto program2 = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TUnsplitted>(),
                TProtobufMultiOutputSpec<NPureCalcProto::TSplitted1, NPureCalcProto::TSplitted2, NPureCalcProto::TStringMessage>(
                    {}, arenas
                ),
                sExpr,
                ETranslationMode::SExpr
            );

            auto input1 = MakeHolder<TUnsplittedStreamImpl>();
            auto output1 = program1->Apply(std::move(input1));

            auto input2 = MakeHolder<TUnsplittedStreamImpl>();
            auto output2 = program2->Apply(std::move(input2));

            decltype(output1->Fetch()) variant1;
            decltype(output2->Fetch()) variant2;

#define ASSERT_EQUAL_FIELDS(X1, X2, I, F, E)            \
    UNIT_ASSERT_EQUAL(X1.index(), I);                   \
    UNIT_ASSERT_EQUAL(X2.index(), I);                   \
    UNIT_ASSERT_EQUAL(std::get<I>(X1)->Get##F(), E);    \
    UNIT_ASSERT_EQUAL(std::get<I>(X2)->Get##F(), E)

            variant1 = output1->Fetch();
            variant2 = output2->Fetch();
            ASSERT_EQUAL_FIELDS(variant1, variant2, 2, X, "Error");
            ASSERT_EQUAL_FIELDS(variant1, variant2, 2, Arena, (arenas.Defined() ? arenas->at(2) : nullptr));

            variant1 = output1->Fetch();
            variant2 = output2->Fetch();
            ASSERT_EQUAL_FIELDS(variant1, variant2, 1, CUint, 111);
            ASSERT_EQUAL_FIELDS(variant1, variant2, 1, CString, "Hello!");
            ASSERT_EQUAL_FIELDS(variant1, variant2, 1, Arena, (arenas.Defined() ? arenas->at(1) : nullptr));

            variant1 = output1->Fetch();
            variant2 = output2->Fetch();
            ASSERT_EQUAL_FIELDS(variant1, variant2, 0, BInt, -23);
            ASSERT_EQUAL_FIELDS(variant1, variant2, 0, BString, "Hello!");
            ASSERT_EQUAL_FIELDS(variant1, variant2, 0, Arena, (arenas.Defined() ? arenas->at(0) : nullptr));

            variant1 = output1->Fetch();
            variant2 = output2->Fetch();
            UNIT_ASSERT_EQUAL(variant1.index(), 0);
            UNIT_ASSERT_EQUAL(variant2.index(), 0);
            UNIT_ASSERT_EQUAL(std::get<0>(variant1), nullptr);
            UNIT_ASSERT_EQUAL(std::get<0>(variant1), nullptr);

#undef ASSERT_EQUAL_FIELDS
        }
    }

    Y_UNIT_TEST(TestMultiOutputs) {
        CheckMultiOutputs(Nothing());
    }

    Y_UNIT_TEST(TestSupportedTypes) {

    }

    Y_UNIT_TEST(TestProtobufArena) {
        {
            NPureCalcProto::TNestedRepeated input;
            input.SetX(20);
            {
                auto* item = input.MutableY()->Add();
                item->SetX(100);
                {
                    auto* y = item->MutableY();
                    *y = GetCanonicalMessage();
                    y->SetFUint64(1000);
                }
            }
            {
                auto* item = input.MutableY()->Add();
                item->SetX(200);
                {
                    auto* y = item->MutableY();
                    *y = GetCanonicalMessage();
                    y->SetFUint64(2000);
                }
            }

            google::protobuf::Arena arena;
            CheckPassThroughYql(input, &arena);
        }

        {
            google::protobuf::Arena arena1;
            google::protobuf::Arena arena2;
            TVector<google::protobuf::Arena*> arenas{&arena1, &arena2, &arena1};
            CheckMultiOutputs(arenas);
        }
    }

    Y_UNIT_TEST(TestFieldRenames) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        TString query = "SELECT InputAlias AS OutputAlias FROM Input";

        auto inputProtoOptions = TProtoSchemaOptions();
        inputProtoOptions.SetFieldRenames({{"X", "InputAlias"}});

        auto inputSpec = TProtobufInputSpec<NPureCalcProto::TSimpleMessage>(
            Nothing(), std::move(inputProtoOptions)
        );

        auto outputProtoOptions = TProtoSchemaOptions();
        outputProtoOptions.SetFieldRenames({{"X", "OutputAlias"}});

        auto outputSpec = TProtobufOutputSpec<NPureCalcProto::TSimpleMessage>(
            std::move(outputProtoOptions)
        );

        {
            auto program = factory->MakePullStreamProgram(
                inputSpec, outputSpec, query, ETranslationMode::SQL
            );

            auto input = MakeHolder<TSimpleMessageStreamImpl>(1);
            auto output = program->Apply(std::move(input));

            TVector<int> expected = {1};
            TVector<int> actual;

            while (auto* x = output->Fetch()) {
                actual.push_back(x->GetX());
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, actual);
        }

        {
            auto program = factory->MakePullListProgram(
                inputSpec, outputSpec, query, ETranslationMode::SQL
            );

            auto input = MakeHolder<TSimpleMessageStreamImpl>(1);
            auto output = program->Apply(std::move(input));

            TVector<int> expected = {1};
            TVector<int> actual;

            while (auto* x = output->Fetch()) {
                actual.push_back(x->GetX());
            }

            UNIT_ASSERT_VALUES_EQUAL(expected, actual);
        }

        {
            auto program = factory->MakePushStreamProgram(
                inputSpec, outputSpec, query, ETranslationMode::SQL
            );

            TVector<int> expected = {1, -100};
            TVector<int> actual;

            auto consumer = MakeHolder<TSimpleMessageConsumerImpl>(&actual);
            auto input = program->Apply(std::move(consumer));

            NPureCalcProto::TSimpleMessage Message;

            Message.SetX(1);
            input->OnObject(&Message);

            input->OnFinish();

            UNIT_ASSERT_VALUES_EQUAL(expected, actual);
        }
    }
}
