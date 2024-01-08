#include "query.h"

namespace NClickHouse {
    TQuery::TQuery() {
    }

    TQuery::TQuery(const char* query)
        : Query_(query)
    {
    }

    TQuery::TQuery(const TString& query)
        : Query_(query)
    {
    }

    TQuery::TQuery(const TString& query, const TString& query_id)
        : Query_(query)
        , QueryId_(query_id)
    {
    }

    TQuery::~TQuery() {
    }

}
