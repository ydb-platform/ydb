#include "local_alpha_num_and_punctuation.h"

#include <util/generic/array_size.h>

namespace NFuzzAlphaNumAndPunctuation {

namespace {

bool AlphaNumAndPunctuation[256] = {};

bool InitAlphaNumAndPunctuation() {
    char chars[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
    for (size_t i = 0; i + 1 < Y_ARRAY_SIZE(chars); ++i) {
        AlphaNumAndPunctuation[static_cast<unsigned char>(chars[i])] = true;
    }
    return true;
}

const bool AlphaNumAndPunctuationReady = InitAlphaNumAndPunctuation();

} // namespace

bool IsAlphaNumAndPunctuation(TStringBuf input) {
    (void)AlphaNumAndPunctuationReady;
    for (char c : input) {
        if (!AlphaNumAndPunctuation[static_cast<unsigned char>(c)]) {
            return false;
        }
    }
    return true;
}

} // namespace NFuzzAlphaNumAndPunctuation
