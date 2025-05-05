#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>
#include <yt/yql/providers/yt/fmr/yt_service/file/yql_yt_file_yt_service.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h> // TODO - REMOVE

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(FileYtServiceTest) {
    Y_UNIT_TEST(CheckReaderAndWriter) {
        TString inputYsonContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                                   "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";

        TTempFileHandle file{};
        TYtTableRef ytTable{.Path = "test_path", .Cluster = "hahn", .FilePath = file.Name()};

        auto fileService = MakeFileYtSerivce();
        auto writer = fileService->MakeWriter(ytTable, TClusterConnection());
        writer->Write(inputYsonContent.data(), inputYsonContent.size());
        writer->Flush();

        TFileInput input(file.Name());

        auto reader = fileService->MakeReader(ytTable, TClusterConnection());
        TStringStream binaryYsonStream;
        TStringStream textYsonStream;
        binaryYsonStream << reader->ReadAll();
        NYson::ReformatYsonStream(&binaryYsonStream, &textYsonStream, NYson::EYsonFormat::Text, ::NYson::EYsonType::ListFragment);
        UNIT_ASSERT(textYsonStream.ReadAll().Contains(inputYsonContent));
    }
}

} // namespace NYql::NFmr
