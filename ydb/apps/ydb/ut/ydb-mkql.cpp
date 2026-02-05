#include "run_ydb.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/shellcommand.h>

Y_UNIT_TEST_SUITE(YdbMkql) {

Y_UNIT_TEST(Execute) {
    TShellCommand cmd(BinaryPath("ydb/apps/ydbd/ydbd"));

    const TString pgm = R"(
(
                        (let range '('('Id (Null) (Void))))
                        (let columns '('Id 'Name))
                        (let result (SelectRange 'Paths range columns '()))
                        (return (AsList (SetResult 'Result result)))
)
)";
    cmd << "--dump";
    cmd << "-s" << ("grpc://" + GetYdbEndpoint());
    cmd << "admin" << "tablet" <<  "72075186232723360" << "execute" << pgm;

    cmd.Run().Wait();

   if (cmd.GetExitCode() != 0) {
        TStringStream ss;
        ss <<
            "command: " << cmd.GetQuotedCommand() << Endl <<
            "exitcode: " << cmd.GetExitCode() << Endl <<
            "stdout: " << Endl << cmd.GetOutput() << Endl <<
            "stderr: " << Endl << cmd.GetError() << Endl;
        UNIT_ASSERT_C(false, ss.Str().data());
    }

    const TString res = cmd.GetOutput();
    UNIT_ASSERT(res.Contains("\"Name\": \"local\""));
}

}
