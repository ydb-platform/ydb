#pragma once

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NYql::NDqPredicate {

bool IsEmptyFilterPredicate(const NNodes::TCoLambda& lambda);
bool SerializeFilterPredicate(const NNodes::TCoLambda& predicate, NPq::NProto::TPredicate* proto, TStringBuilder& err);
TString FormatWhere(const NYql::NPq::NProto::TPredicate& predicate);

}
