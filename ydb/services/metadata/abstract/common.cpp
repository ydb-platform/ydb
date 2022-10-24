#include "common.h"

namespace NKikimr::NMetadataProvider {

i32 ISnapshot::GetFieldIndex(const Ydb::ResultSet& rawData, const TString& columnId) const {
    i32 idx = 0;
    for (auto&& i : rawData.columns()) {
        if (i.name() == columnId) {
            return idx;
        }
        ++idx;
    }
    return -1;
}

}
