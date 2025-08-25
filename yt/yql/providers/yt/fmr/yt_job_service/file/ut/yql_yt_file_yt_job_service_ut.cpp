#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(FileYtServiceTests) {
    Y_UNIT_TEST(CheckFileReaderAndWriter) {
        TString inputYsonContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                                   "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";

        TTempFileHandle file{};
        TYtTableRef ytTable{.RichPath = NYT::TRichYPath().Path("Path").Cluster("Cluster"), .FilePath = file.Name()};

        auto fileService = MakeFileYtJobSerivce();
        auto writer = fileService->MakeWriter(ytTable, TClusterConnection());
        writer->Write(inputYsonContent.data(), inputYsonContent.size());
        writer->Flush();

        TFileInput input(file.Name());

        auto reader = fileService->MakeReader(ytTable);
        TStringStream binaryYsonStream;
        TStringStream textYsonStream;
        binaryYsonStream << reader->ReadAll();
        NYson::ReformatYsonStream(&binaryYsonStream, &textYsonStream, NYson::EYsonFormat::Text, ::NYson::EYsonType::ListFragment);
        UNIT_ASSERT(textYsonStream.ReadAll().Contains(inputYsonContent));
    }
}

} // namespace NYql::NFmr
