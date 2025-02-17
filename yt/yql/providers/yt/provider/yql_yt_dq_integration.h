#pragma once

#include "yql_yt_provider.h"

#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqIntegration> CreateYtDqIntegration(TYtState* state);

// TODO move to yql/core
bool CheckSupportedTypesOld(const TTypeAnnotationNode::TListType& typesToCheck, const TSet<TString>& supportedTypes, const TSet<NUdf::EDataSlot>& supportedDataTypes, std::function<void(const TString&)> unsupportedTypeHandler);

}
