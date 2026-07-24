#include "local_alpha_num_and_punctuation.h"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const TString input(reinterpret_cast<const char*>(data), size);

    try {
        const bool result = NFuzzAlphaNumAndPunctuation::IsAlphaNumAndPunctuation(input);
        (void)result;
    } catch (...) {
    }

    return 0;
}
