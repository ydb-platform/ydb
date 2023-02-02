#include "object.h"
#include "behaviour.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NInitializer {

bool TDBInitialization::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetComponentIdIdx(), ComponentId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetModificationIdIdx(), ModificationId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetInstantIdx(), Instant, rawValue)) {
        return false;
    }
    return true;
}

NInternal::TTableRecord TDBInitialization::SerializeToRecord() const {
    NInternal::TTableRecord result;
    result.SetColumn(TDecoder::ComponentId, NInternal::TYDBValue::Utf8(ComponentId));
    result.SetColumn(TDecoder::ModificationId, NInternal::TYDBValue::Utf8(ModificationId));
    result.SetColumn(TDecoder::Instant, NInternal::TYDBValue::UInt32(Instant.Seconds()));
    return result;
}

IClassBehaviour::TPtr TDBInitialization::GetBehaviour() {
    static std::shared_ptr<TDBObjectBehaviour> result = std::make_shared<TDBObjectBehaviour>();
    return result;
}

}
