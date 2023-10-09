#include <library/cpp/string_utils/base32/base32.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
{
    const std::string_view example{reinterpret_cast<const char*>(data), size};
    const auto converted = Base32StrictDecode(Base32Encode(example));

    Y_ABORT_UNLESS(example == converted);

    return 0;
}
