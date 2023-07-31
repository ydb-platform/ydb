#include <ydb/library/yql/public/purecalc/examples/protobuf_pull_list/main.pb.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/helpers/stream/stream_from_vector.h>

using namespace NYql::NPureCalc;
using namespace NExampleProtos;

const char* Query = R"(
    SELECT
        Url,
        COUNT(*) AS Hits
    FROM
        Input
    GROUP BY
        Url
    ORDER BY
        Url
)";

THolder<IStream<TInput*>> MakeInput();

int main() {
    try {
        auto factory = MakeProgramFactory();

        auto program = factory->MakePullListProgram(
            TProtobufInputSpec<TInput>(),
            TProtobufOutputSpec<TOutput>(),
            Query,
            ETranslationMode::SQL
        );

        auto result = program->Apply(MakeInput());

        while (auto* message = result->Fetch()) {
            Cout << "url = " << message->GetUrl() << Endl;
            Cout << "hits = " << message->GetHits() << Endl;
        }
    } catch (TCompileError& e) {
        Cout << e.GetIssues();
    }
}

THolder<IStream<TInput*>> MakeInput() {
    TVector<TInput> input;

    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/a");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/a");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/b");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/c");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/b");
    }
    {
        auto& message = input.emplace_back();
        message.SetUrl("https://yandex.ru/b");
    }

    return StreamFromVector(std::move(input));
}
