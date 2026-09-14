#include "radix.h"

#include <util/generic/yexception.h>

#include <algorithm>
#include <utility>

namespace NLsp::NYql {

namespace {

void Append(TString& alphabet, char a, char b) {
    Y_ENSURE(a < b);
    alphabet.reserve(alphabet.size() + (b - a + 1));
    for (char c = a; c <= b; ++c) {
        alphabet.push_back(c);
    }
}

} // namespace

TRadix::TRadix(TString alphabet)
    : Alphabet_(std::move(alphabet))
{
    if (Alphabet_.size() < 2) {
        throw yexception() << "Radix alphabet must contain at least two characters";
    }

    if (!std::ranges::is_sorted(Alphabet_)) {
        throw yexception() << "Radix alphabet must be strictly increasing";
    }

    const auto dups = std::ranges::unique(Alphabet_);
    if (!std::ranges::empty(dups)) {
        throw yexception() << "Radix alphabet must have unique characters";
    }
}

TString TRadix::Encode(size_t value) const {
    TString result(Reserve(EncodedLen(value)));
    do {
        result.push_back(Alphabet_[value % Alphabet_.size()]);
        value /= Alphabet_.size();
    } while (value > 0);

    std::ranges::reverse(result);
    return result;
}

TString TRadix::Encode(size_t value, size_t length) const {
    TString result = Encode(value);

    Y_ENSURE(0 < length);
    Y_ENSURE(result.size() <= length);

    result.insert(static_cast<size_t>(0), length - result.size(), Alphabet_.front());
    return result;
}

TString TRadix::SimpleAlphabet() {
    TString alphabet;
    Append(alphabet, '0', '9');
    Append(alphabet, 'A', 'Z');
    Append(alphabet, 'a', 'z');
    return alphabet;
}

size_t TRadix::EncodedLen(size_t value) const {
    size_t length = 1;
    while (value >= Alphabet_.size()) {
        value /= Alphabet_.size();
        ++length;
    }
    return length;
}

} // namespace NLsp::NYql
