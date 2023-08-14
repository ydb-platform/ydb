#include "run_ydb.h"

#include <util/generic/yexception.h>
#include <util/system/shellcommand.h>
#include <util/system/env.h>

#include <library/cpp/testing/common/env.h>

TString GetYdbEndpoint()
{
    return GetEnv("YDB_ENDPOINT");
}

TString GetYdbDatabase()
{
    return GetEnv("YDB_DATABASE");
}

TString RunYdb(const TList<TString>& args1, const TList<TString>& args2)
{
    TShellCommand command(BinaryPath(GetEnv("YDB_CLI_BINARY")));

    command << "-e" << ("grpc://" + GetYdbEndpoint());
    command << "-d" << ("/" + GetYdbDatabase());

    for (auto& arg : args1) {
        command << arg;
    }

    for (auto& arg : args2) {
        command << arg;
    }

    command.Run().Wait();

    if (command.GetExitCode() != 0) {
        ythrow yexception() << Endl <<
            "command: " << command.GetQuotedCommand() << Endl <<
            "exitcode: " << command.GetExitCode() << Endl <<
            "stdout: " << Endl << command.GetOutput() << Endl <<
            "stderr: " << Endl << command.GetError() << Endl;
    }

    return command.GetOutput();
}
