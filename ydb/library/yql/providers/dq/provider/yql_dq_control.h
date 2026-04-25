#pragma once

#include <util/generic/ptr.h>
#include <util/generic/map.h>

#include <yql/essentials/core/file_storage/file_storage.h>

namespace NYql {

namespace NProto {
class TDqConfig;
}

class IDqControl : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqControl>;

    virtual bool IsReady(const TMap<TString, TString>& udfs = TMap<TString, TString>())= 0;
};

using IDqControlPtr = TIntrusivePtr<IDqControl>;

class IDqControlFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqControlFactory>;

    virtual IDqControlPtr GetControl() = 0;
    virtual const THashSet<TString>& GetIndexedUdfFilter() = 0;
    virtual bool StripEnabled() const = 0;
};

using IDqControlFactoryPtr = TIntrusivePtr<IDqControlFactory>;

IDqControlFactoryPtr CreateDqControlFactory(const NProto::TDqConfig& config, const TMap<TString, TString>& udfs, const TFileStoragePtr& fileStorage);
IDqControlFactoryPtr CreateDqControlFactory(
    const uint32_t port,
    const TString& vanillaJobLite,
    const TString& vanillaJobLiteMd5,
    const bool enableStrip,
    const THashSet<TString> indexedUdfFilter,
    const TMap<TString, TString>& udfs,
    const TFileStoragePtr& fileStorage);

extern const TString DqStrippedSuffied;

} // namespace NYql
