#pragma once

#include <util/generic/string.h>

namespace NKikimr::NNaming {

    TString SnakeToCamelCase(const TString& name);
    TString CamelToSnakeCase(const TString& name);

}