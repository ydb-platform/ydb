#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>
#include <ydb/library/yql/public/purecalc/ut/empty_stream.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestUserData) {
    Y_UNIT_TEST(TestUserData) {
        using namespace NYql::NPureCalc;

        auto options = TProgramFactoryOptions()
            .AddFile(NYql::NUserData::EDisposition::INLINE, "my_file.txt", "my content!");

        auto factory = MakeProgramFactory(options);

        auto program = factory->MakePullListProgram(
            TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
            TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
            "SELECT UNWRAP(CAST(FileContent(\"my_file.txt\") AS Utf8)) AS X",
            ETranslationMode::SQL
        );

        auto stream = program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());

        NPureCalcProto::TStringMessage* message;

        UNIT_ASSERT(message = stream->Fetch());
        UNIT_ASSERT_EQUAL(message->GetX(), "my content!");
        UNIT_ASSERT(!stream->Fetch());
    }

    Y_UNIT_TEST(TestUserDataLibrary) {
        using namespace NYql::NPureCalc;

        try {
            auto options = TProgramFactoryOptions()
                .AddLibrary(NYql::NUserData::EDisposition::INLINE, "a.sql", "$x = 1; EXPORT $x;")
                .AddLibrary(NYql::NUserData::EDisposition::INLINE, "b.sql", "IMPORT a SYMBOLS $x; $y = CAST($x + 1 AS String); EXPORT $y;");

            auto factory = MakeProgramFactory(options);

            auto program = factory->MakePullListProgram(
                TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
                TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
                "IMPORT b SYMBOLS $y; SELECT CAST($y AS Utf8) ?? '' AS X;",
                ETranslationMode::SQL
            );

            auto stream = program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>());

            NPureCalcProto::TStringMessage* message;

            UNIT_ASSERT(message = stream->Fetch());
            UNIT_ASSERT_EQUAL(message->GetX(), "2");
            UNIT_ASSERT(!stream->Fetch());
        } catch (const TCompileError& e) {
            Cerr << e;
            throw e;
        }
    }
}
