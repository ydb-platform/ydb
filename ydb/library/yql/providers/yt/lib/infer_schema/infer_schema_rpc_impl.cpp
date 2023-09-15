#include "infer_schema_rpc.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {
TVector<TMaybe<NYT::TNode>> InferSchemaFromTablesContents(const TString&, const TString&, const NYT::TTransactionId&, const std::vector<TTableInferSchemaRequest>&) {
    YQL_ENSURE(false, "Not implemented");
}
}
