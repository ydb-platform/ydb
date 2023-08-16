#pragma once

#include "external_source.h"

namespace NKikimr::NExternalSource {

IExternalSource::TPtr CreateExternalDataSource(const TString& name, const TVector<TString>& authMethods);

}
