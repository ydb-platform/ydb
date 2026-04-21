#include "meta_cluster_info.h"

#include <ydb/mvp/core/core_ydb_impl.h>

namespace NMVP {

TString BuildClusterInfoQuery(TStringBuf rootDomain) {
    return TStringBuilder() << "DECLARE $name AS Utf8; SELECT * FROM `" << rootDomain << "/ydb/MasterClusterExt.db` WHERE name=$name";
}

NYdb::TParams BuildClusterInfoQueryParams(TStringBuf clusterName) {
    NYdb::TParamsBuilder params;
    params.AddParam("$name", NYdb::TValueBuilder().Utf8(TString(clusterName)).Build());
    return params.Build();
}

bool TryExtractClusterInfo(const NYdb::TResultSet& resultSet, THashMap<TString, TString>& clusterInfo) {
    const auto& columnsMeta = resultSet.GetColumnsMeta();
    NYdb::TResultSetParser rsParser(resultSet);
    if (!rsParser.TryNextRow()) {
        return false;
    }

    for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
        const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
        clusterInfo[columnMeta.Name] = THandlerActorYdb::ColumnValueToString(rsParser.ColumnParser(columnNum));
    }

    return true;
}

} // namespace NMVP
