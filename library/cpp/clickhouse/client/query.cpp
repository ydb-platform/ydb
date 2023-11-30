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

    TQuery::~TQuery() {
    }

}
