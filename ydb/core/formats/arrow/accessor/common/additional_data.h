#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimrTxColumnShard {
class TIndexColumnMeta;
}

namespace NKikimr::NArrow::NAccessor {

struct IAdditionalAccessorData {
    virtual ~IAdditionalAccessorData() = default;
    virtual bool HasDataToSerialize() const = 0;
    virtual void AddToProto(NKikimrTxColumnShard::TIndexColumnMeta& meta) const = 0;

    virtual NJson::TJsonValue DebugJson() const {
        return NJson::TJsonValue(NJson::JSON_MAP);
    }
};

struct TBlobWithAdditionalAccessorData {
    TString Blob;
    std::shared_ptr<IAdditionalAccessorData> Meta;
};

}
