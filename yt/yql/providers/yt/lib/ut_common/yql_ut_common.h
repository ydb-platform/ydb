#pragma once

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

#include <util/system/tempfile.h>

namespace NYql {

struct TTestTablesMapping: public THashMap<TString, TString> {
    TTempFileHandle TmpInput;
    TTempFileHandle TmpInputAttr;
    TTempFileHandle TmpOutput;
    TTempFileHandle TmpOutputAttr;

    TTestTablesMapping();
};

void InitializeYtGateway(IYtGateway::TPtr gateway, TYtState::TPtr ytState);

}
