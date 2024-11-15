#pragma once

#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYql {

class TTypeAnnotationNode;

THolder<TVisitorTransformerBase> CreateDqsDataSourceTypeAnnotationTransformer(bool annotateConfigure = true);

} // NYql
