#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {

struct TConfigExample {
    TString LoadName;
    TString Text;
    TString Escaped;
};

struct TConfigTemplate {
    TString LoadName;
    TString Template;
};

TConstArrayRef<TConfigTemplate> GetConfigTemplates();

TConfigExample ApplyTemplateParams(const TConfigTemplate& templ, const TString& tenantName);

}  // namespace NKikimr
