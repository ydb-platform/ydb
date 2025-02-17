#pragma once
#include "yql_pg_provider.h"

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

#include <util/generic/ptr.h>

namespace NYql {

TIntrusivePtr<IDataProvider> CreatePgDataSource(TPgState::TPtr state);
TIntrusivePtr<IDataProvider> CreatePgDataSink(TPgState::TPtr state);

THolder<TVisitorTransformerBase> CreatePgDataSourceTypeAnnotationTransformer(TPgState::TPtr state);
THolder<TVisitorTransformerBase> CreatePgDataSinkTypeAnnotationTransformer(TPgState::TPtr state);

THolder<TExecTransformerBase> CreatePgDataSinkExecTransformer(TPgState::TPtr state);

THolder<IDqIntegration> CreatePgDqIntegration(TPgState::TPtr state);

}
