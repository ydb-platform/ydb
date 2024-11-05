
#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>

#include <ydb/library/yql/core/user_data/yql_user_data.h>

#include <util/stream/file.h>
#include <util/datetime/base.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/skiff/skiff.h>

using namespace NYql::NUserData;
using namespace NYT;
using namespace NYql::NPureCalc;

const char* Query = R"(
    SELECT
        Url,
        COUNT(*) AS Hits
    FROM
        Input
    GROUP BY
        Url
    ORDER BY
        Hits desc
)";

int main() {
    auto addField = [&](NYT::TNode& members, const TString& name, const TString& type, const bool isOptional) {
        auto typeNode = NYT::TNode::CreateList()
                            .Add("DataType")
                            .Add(type);

        if (isOptional) {
            typeNode = NYT::TNode::CreateList()
                           .Add("OptionalType")
                           .Add(typeNode);
        }

        members.Add(NYT::TNode::CreateList()
                        .Add(name)
                        .Add(typeNode));
    };

    NYT::TNode members{NYT::TNode::CreateList()};
    addField(members, "Url", "String", false);
    NYT::TNode schema = NYT::TNode::CreateList()
                            .Add("StructType")
                            .Add(members);

    Cout << "InputSchema: " << NodeToYsonString(schema) << Endl;
    auto inputSpec = TSkiffInputSpec(TVector<NYT::TNode>{schema});
    auto outputSpec = TSkiffOutputSpec({NYT::TNode::CreateEntity()});
    auto factoryOptions = TProgramFactoryOptions();
    factoryOptions.SetNativeYtTypeFlags(0);
    factoryOptions.SetLLVMSettings("OFF");
    factoryOptions.SetBlockEngineSettings("disable");
    auto factory = MakeProgramFactory(factoryOptions);
    auto program = factory->MakePullListProgram(
        inputSpec,
        outputSpec,
        Query,
        ETranslationMode::SQL);
    Cout << "OutpSchema: " << NYT::NodeToCanonicalYsonString(program->MakeFullOutputSchema()) << Endl;
    TStringStream stream;
    NSkiff::TUncheckedSkiffWriter writer{&stream};
    writer.WriteVariant16Tag(0);
    writer.WriteString32("https://yandex.ru/a");
    writer.WriteVariant16Tag(0);
    writer.WriteString32("https://yandex.ru/a");
    writer.WriteVariant16Tag(0);
    writer.WriteString32("https://yandex.ru/b");
    writer.WriteVariant16Tag(0);
    writer.WriteString32("https://yandex.ru/c");
    writer.WriteVariant16Tag(0);
    writer.WriteString32("https://yandex.ru/b");
    writer.WriteVariant16Tag(0);
    writer.WriteString32("https://yandex.ru/b");
    writer.Finish();
    auto input = TStringStream(stream);
    auto handle = program->Apply(&input);
    TStringStream output;
    handle->Run(&output);
    auto parser = NSkiff::TUncheckedSkiffParser(&output);
    while (parser.HasMoreData()) {
        parser.ParseVariant16Tag();
        auto hits = parser.ParseInt64();
        auto url = parser.ParseString32();
        Cout << "URL: " << url << " Hits: " << hits << Endl;
    }
}
