#include <ydb/library/yql/public/purecalc/examples/protobuf/main.pb.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/helpers/stream/stream_from_vector.h>

using namespace NYql::NPureCalc;
using namespace NExampleProtos;

void PullStreamExample(IProgramFactoryPtr);
void PushStreamExample(IProgramFactoryPtr);
void PrecompileExample(IProgramFactoryPtr factory);
THolder<IStream<TInput*>> MakeInput();

class TConsumer: public IConsumer<TOutput*> {
public:
    void OnObject(TOutput* message) override {
        Cout << "path = " << message->GetPath() << Endl;
        Cout << "host = " << message->GetHost() << Endl;
    }

    void OnFinish() override {
        Cout << "end" << Endl;
    }
};

const char* Query = R"(
    $a = (SELECT * FROM Input);
    $b = (SELECT CAST(Url::GetTail(Url) AS Utf8) AS Path, CAST(Url::GetHost(Url) AS Utf8) AS Host, Ip FROM $a);
    $c = (SELECT Path, Host FROM $b WHERE Path IS NOT NULL AND Host IS NOT NULL AND Ip::IsIPv4(Ip::FromString(Ip)));
    $d = (SELECT Unwrap(Path) AS Path, Unwrap(Host) AS Host FROM $c);
    SELECT * FROM $d;
)";

int main(int argc, char** argv) {
    try {
        auto factory = MakeProgramFactory(
            TProgramFactoryOptions().SetUDFsDir(argc > 1 ? argv[1] : "../../../../udfs"));

        Cout << "Pull stream:" << Endl;
        PullStreamExample(factory);

        Cout << Endl;
        Cout << "Push stream:" << Endl;
        PushStreamExample(factory);

        Cout << Endl;
        Cout << "Pull stream with pre-compilation:" << Endl;
        PrecompileExample(factory);
    } catch (const TCompileError& err) {
        Cerr << err.GetIssues() << Endl;
        Cerr << err.what() << Endl;
    }
}

void PullStreamExample(IProgramFactoryPtr factory) {
    auto program = factory->MakePullStreamProgram(
        TProtobufInputSpec<TInput>(),
        TProtobufOutputSpec<TOutput>(),
        Query,
        ETranslationMode::SQL);

    auto result = program->Apply(MakeInput());

    while (auto* message = result->Fetch()) {
        Cout << "path = " << message->GetPath() << Endl;
        Cout << "host = " << message->GetHost() << Endl;
    }
}

void PushStreamExample(IProgramFactoryPtr factory) {
    auto program = factory->MakePushStreamProgram(
        TProtobufInputSpec<TInput>(),
        TProtobufOutputSpec<TOutput>(),
        Query,
        ETranslationMode::SQL);

    auto consumer = program->Apply(MakeHolder<TConsumer>());

    auto input = MakeInput();
    while (auto* message = input->Fetch()) {
        consumer->OnObject(message);
    }
    consumer->OnFinish();
}

void PrecompileExample(IProgramFactoryPtr factory) {
    TString prg;
    {
        auto program = factory->MakePullStreamProgram(
            TProtobufInputSpec<TInput>(),
            TProtobufOutputSpec<TOutput>(),
            Query,
            ETranslationMode::SQL);

        prg = program->GetCompiledProgram();
    }

    auto program = factory->MakePullStreamProgram(
                TProtobufInputSpec<TInput>(),
                TProtobufOutputSpec<TOutput>(),
                prg,
                ETranslationMode::Mkql);

    auto result = program->Apply(MakeInput());

    while (auto* message = result->Fetch()) {
        Cout << "path = " << message->GetPath() << Endl;
        Cout << "host = " << message->GetHost() << Endl;
    }
}

THolder<IStream<TInput*>> MakeInput() {
    TVector<TInput> input;

    {
        auto& message = input.emplace_back();
        message.SetUrl("https://news.yandex.ru/Moscow/index.html?from=index");
        message.SetIp("83.220.231.160");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://music.yandex.ru/radio/");
        message.SetIp("83.220.231.161");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/maps/?ll=141.475401%2C11.581666&spn=1.757813%2C1.733096&z=7&l=map%2Cstv%2Csta&mode=search&panorama%5Bpoint%5D=141.476317%2C11.582710&panorama%5Bdirection%5D=177.241445%2C-15.219821&panorama%5Bspan%5D=107.410156%2C61.993317");
        message.SetIp("::ffff:77.75.155.3");
    }

    return StreamFromVector(std::move(input));
}
