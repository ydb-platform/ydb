#pragma once

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

namespace NFq {

    TString FormatWhere(const NYql::NPq::NProto::TPredicate& predicate);

} // namespace NFq
