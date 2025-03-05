#include "yql_ut_common.h"

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/guid.h>
#include <util/system/user.h>
#include <util/stream/file.h>

namespace NYql {

TTestTablesMapping::TTestTablesMapping()
    : TmpInput()
    , TmpInputAttr(TmpInput.Name() + ".attr")
    , TmpOutput()
    , TmpOutputAttr(TmpOutput.Name() + ".attr")
{
    {
        TUnbufferedFileOutput tmpInput(TmpInput);
        tmpInput << "{\"key\"=\"\";\"subkey\"=\"\";\"value\"=\"\"}" << Endl;
        TUnbufferedFileOutput tmpInputAttr(TmpInputAttr);
        tmpInputAttr << "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
            << "[\"key\";[\"DataType\";\"String\"]];"
            << "[\"subkey\";[\"DataType\";\"String\"]];"
            << "[\"value\";[\"DataType\";\"String\"]]"
            << "]]}}" << Endl;
    }
    insert(std::make_pair("yt.plato.Input", TmpInput.Name()));

    {
        TUnbufferedFileOutput tmpOutput(TmpOutput);
        tmpOutput << "{\"key\"=\"\";\"subkey\"=\"\";\"value\"=\"\"}" << Endl;
        TUnbufferedFileOutput tmpOutputAttr(TmpOutputAttr);
        tmpOutputAttr << "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
            << "[\"key\";[\"DataType\";\"String\"]];"
            << "[\"subkey\";[\"DataType\";\"String\"]];"
            << "[\"value\";[\"DataType\";\"String\"]]"
            << "]]}}" << Endl;
    }
    insert(std::make_pair("yt.plato.Output", TmpOutput.Name()));
}

void InitializeYtGateway(IYtGateway::TPtr gateway, TYtState::TPtr ytState) {
    ytState->SessionId = CreateGuidAsString();
    gateway->OpenSession(
        IYtGateway::TOpenSessionOptions(ytState->SessionId)
            .UserName(GetUsername())
            .ProgressWriter(&NullProgressWriter)
            .OperationOptions(TYqlOperationOptions())
            .RandomProvider(CreateDeterministicRandomProvider(1))
            .TimeProvider(CreateDeterministicTimeProvider(10000000))
        );
}

}
