#include "named_span.h"

#include <ydb/library/actors/retro_tracing/universal_span.h>
#include "universal_span_wilson_api.h"

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr {

void TNamedSpan::SetName(const char* name) {
    NameSize = std::min(std::strlen(name), MaxNameSize);
    std::memcpy(reinterpret_cast<void*>(NameBuffer), reinterpret_cast<const void*>(name), NameSize);
}

TString TNamedSpan::GetName() const {
    TString res(NameBuffer, NameSize);
    return res;
}

std::unique_ptr<NWilson::TSpan> TNamedSpan::MakeWilsonSpan() {
    std::unique_ptr<NWilson::TSpan> res = TRetroSpan::MakeWilsonSpan();
    res->Attribute("database", AppData()->TenantName);
    res->Attribute("node", AppData()->NodeName);
    return res;
}

} // namespace NKikimr
