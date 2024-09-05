#include "run_ydb.h"

#include <util/generic/yexception.h>
#include <util/system/shellcommand.h>
#include <util/system/env.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

TString GetYdbEndpoint()
{
    return GetEnv("YDB_ENDPOINT");
}

TString GetYdbDatabase()
{
    return GetEnv("YDB_DATABASE");
}

TString RunYdb(const TList<TString>& args1, const TList<TString>& args2, bool checkExitCode)
{
    SetEnv("YDB_TOKEN", "root@builin");

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

    if (checkExitCode && (command.GetExitCode() != 0)) {
        ythrow yexception() << Endl <<
            "command: " << command.GetQuotedCommand() << Endl <<
            "exitcode: " << command.GetExitCode() << Endl <<
            "stdout: " << Endl << command.GetOutput() << Endl <<
            "stderr: " << Endl << command.GetError() << Endl;
    }

    return command.GetOutput();
}

ui64 GetMostRecentValue(const TString& output)
{
    TVector<TString> lines, columns;

    Split(output, "\n", lines);
    Split(lines.back(), "\t", columns);

    return FromString<ui64>(columns.back());
}

ui64 GetFullTimeValue(const TString& output)
{
    return GetMostRecentValue(output);
}

ui64 GetCommitTimeValue(const TString& output)
{
    return GetMostRecentValue(output);
}

THashSet<TString> GetCodecsList(const TString& output)
{
    THashSet<TString> result;

    TVector<TString> lines;
    Split(output, "\n", lines);

    for (auto& line : lines) {
        TVector<TString> fields;
        Split(line, ":", fields);

        if (fields[0] == "SupportedCodecs") {
            TVector<TString> codecs;
            Split(fields[1], ",", codecs);
            for (auto& codec : codecs) {
                result.insert(Strip(codec));
            }
        }
    }

    return result;
}

void UnitAssertColumnsOrder(TString line,
                            const TVector<TString>& columns)
{
    for (size_t i = 0; i < columns.size(); ++i) {
        auto& column = columns[i];

        UNIT_ASSERT_C(line.StartsWith(column),
                      Sprintf("In column %" PRISZT ", '%s' was expected, but '%s' was received",
                              i, column.data(), line.data()));

        line = line.substr(column.length());

        size_t pos = line.find_first_not_of(" \t");
        if (pos != TString::npos) {
            line = line.substr(pos);
        } else {
            line = "";
        }
    }

    UNIT_ASSERT_C(line.empty(),
                  Sprintf("Unexpected columns '%s'", line.data()));
}
