#include "query_id.h"
#include "helpers.h"
#include <util/generic/yexception.h>

namespace NKikimr::NKqp {

TKqpQueryId::TKqpQueryId(const TString& cluster, const TString& database, const TString& text, NKikimrKqp::EQueryType type)
    : Cluster(cluster)
    , Database(database)
    , Text(text)
    , QueryType(type)
{
    switch (QueryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_DML:
        case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
        case NKikimrKqp::QUERY_TYPE_AST_DML:
        case NKikimrKqp::QUERY_TYPE_AST_SCAN:
        case NKikimrKqp::QUERY_TYPE_SQL_QUERY:
        case NKikimrKqp::QUERY_TYPE_FEDERATED_QUERY:
            break;

        default:
            Y_ENSURE(false, "Unsupported request type");
    }
}

bool TKqpQueryId::IsSql() const {
    return IsSqlQuery(QueryType);
}

} // namespace NKikimr::NKqp
