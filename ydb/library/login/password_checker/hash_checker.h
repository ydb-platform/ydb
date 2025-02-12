#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <unordered_set>

namespace NLogin {

class THashChecker {
public:
    struct TResult {
        bool Success = true;
        TString Error;
    };

    static constexpr size_t SALT_SIZE = 16;
    static constexpr size_t HASH_SIZE = 32;

private:
    static bool IsBase64(const std::string& value);

public:
    static TResult Check(const TString& hash);
};

} // NLogin
