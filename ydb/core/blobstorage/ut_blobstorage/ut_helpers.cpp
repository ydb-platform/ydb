#include "ut_helpers.h"

namespace NKikimr {

TString MakeData(ui32 dataSize) {
    TString data(dataSize, '\0');
    for (ui32 i = 0; i < dataSize; ++i) {
        data[i] = 'A' + (i % 26);
    }
    return data;
}

ui64 TInflightActor::Cookie = 1;

} // namespace NKikimr
