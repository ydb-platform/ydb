#pragma once
#include "properties.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard {

class TMetadataObjectInfo final: public TSimpleRefCount<TMetadataObjectInfo> {
public:
    using TPtr = TIntrusivePtr<TMetadataObjectInfo>;

private:
    YDB_ACCESSOR(ui64, AlterVersion, 0);
    YDB_READONLY_DEF(IMetadataObjectProperties::TPtr, Properties);

public:
    template <typename T>
    const std::shared_ptr<T> GetPropertiesVerified() const {
        AFL_VERIFY(Properties);
        const std::shared_ptr<T> converted = std::dynamic_pointer_cast<T>(Properties);
        AFL_VERIFY(converted);
        return converted;
    }

    virtual ~TMetadataObjectInfo() = default;

    TMetadataObjectInfo() = default;
    TMetadataObjectInfo(ui64 alterVersion, IMetadataObjectProperties::TPtr properties)
        : AlterVersion(alterVersion)
        , Properties(properties) {
    }
};
}   // namespace NKikimr::NSchemeShard
