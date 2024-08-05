#include <util/generic/serialized_enum.h>
#include <util/generic/singleton.h>
#include <ydb/library/services/services.pb.h>

namespace NEnumSerializationRuntime {

using TType = NDetail::TSelectEnumRepresentationType<NKikimrServices::TActivity::EType>::TType;

class TStorageHolder {
public:
    TStorageHolder() {
        for (int i = 0; i < NKikimrServices::TActivity::EType_ARRAYSIZE; i++) {
            if (NKikimrServices::TActivity::EType_IsValid(i)) {
                NKikimrServices::TActivity::EType enumType = static_cast<NKikimrServices::TActivity::EType>(i);
                TString name = NKikimrServices::TActivity::EType_Name(enumType);
                Storage.insert({enumType, name});
            }
        }
    }


    TMap<TType, TString> Storage;
};

template<>
TMappedDictView<NKikimrServices::TActivity::EType, TString> GetEnumNamesImpl() {
    auto& storage = Singleton<TStorageHolder>()->Storage;
    return TMappedDictView<NKikimrServices::TActivity::EType, TString>(storage);
}

} // NEnumSerializationRuntime
