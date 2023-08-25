#pragma once

#include "yql_data_provider.h"
#include "yql_type_annotation.h"

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NYql {

TAutoPtr<IGraphTransformer> CreateConfigureTransformer(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateIODiscoveryTransformer(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateEpochsTransformer(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateRecaptureDataProposalsInspector(const TTypeAnnotationContext& types, const TString& provider);
TAutoPtr<IGraphTransformer> CreateStatisticsProposalsInspector(const TTypeAnnotationContext& types, const TString& provider);
TAutoPtr<IGraphTransformer> CreateLogicalDataProposalsInspector(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreatePhysicalDataProposalsInspector(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreatePhysicalFinalizers(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateTableMetadataLoader(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateCompositeFinalizingTransformer(const TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreatePlanInfoTransformer(const TTypeAnnotationContext& types);

}
