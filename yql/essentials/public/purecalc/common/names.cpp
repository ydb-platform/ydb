#include "names.h"

#include <util/generic/strbuf.h>

namespace NYql::NPureCalc {
    const TStringBuf PurecalcSysColumnsPrefix = "_yql_sys_";
    const TStringBuf PurecalcSysColumnTablePath = "_yql_sys_tablepath";
    const TStringBuf PurecalcBlockColumnLength = "_yql_block_length";

    const TStringBuf PurecalcDefaultCluster = "view";
    const TStringBuf PurecalcDefaultService = "data";

    const TStringBuf PurecalcInputCallableName = "Self";
    const TStringBuf PurecalcInputTablePrefix = "Input";

    const TStringBuf PurecalcBlockInputCallableName = "BlockSelf";

    const TStringBuf PurecalcUdfModulePrefix = "<purecalc>::";
}
