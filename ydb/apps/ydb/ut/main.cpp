#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

TString ExecYdbWorkloadTopic(TList<TString> args)
{
    //
    // ydb -e grpc://${YDB_ENDPOINT} -d /${YDB_DATABASE} workload topic ${args}
    //

    args.push_front("topic");
    args.push_front("workload");

    args.push_front("/" + GetEnv("YDB_DATABASE"));
    args.push_front("-d");

    args.push_front("grpc://" + GetEnv("YDB_ENDPOINT"));
    args.push_front("-e");

    TShellCommand command(BinaryPath("ydb/apps/ydb/ydb"), args);
    command.Run().Wait();

    UNIT_ASSERT_VALUES_EQUAL(command.GetExitCode(), 0);

    return command.GetOutput();
}

Y_UNIT_TEST_SUITE(YdbWorkloadTopic) {
Y_UNIT_TEST(RunFull) {
    ExecYdbWorkloadTopic({"init"});
    auto output = ExecYdbWorkloadTopic({"run", "full", "-s", "10"});
    ExecYdbWorkloadTopic({"clean"});

    TVector<TString> lines, columns;

    Split(output, "\n", lines);
    Split(lines.back(), "\t", columns);

    auto fullTime = FromString<ui64>(columns.back());

    UNIT_ASSERT_GE(fullTime, 0);
    UNIT_ASSERT_LT(fullTime, 10'000);
}
}
