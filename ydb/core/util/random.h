#include <util/generic/string.h>

#include <stddef.h>

namespace NKikimr {

void SafeEntropyPoolRead(void* buf, size_t len);

TString GenRandomBuffer(size_t len);

} // namespace NKikimr
