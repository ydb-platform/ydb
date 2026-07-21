// Fuzzer for the password complexity checker.
// TPasswordChecker::Check is called with attacker-controlled username and
// password strings during user creation and password change over gRPC.
// Uses utf-8 iteration and character-class logic on raw input.
#include <ydb/library/login/password_checker/password_checker.h>
#include <cstring>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) return 0;

    // Split fuzz bytes into username / password via first NUL byte
    const uint8_t* nul = static_cast<const uint8_t*>(std::memchr(data, 0, size));
    size_t usernameLen = nul ? static_cast<size_t>(nul - data) : size / 2;
    size_t passwordLen = size - usernameLen - (nul ? 1 : 0);
    const uint8_t* passwordData = data + usernameLen + (nul ? 1 : 0);

    std::string username(reinterpret_cast<const char*>(data), usernameLen);
    std::string password(reinterpret_cast<const char*>(passwordData), passwordLen);

    NLogin::TPasswordComplexity::TInitializer init;
    init.MinLength = 8;
    init.MinLowerCaseCount = 1;
    init.MinUpperCaseCount = 1;
    init.MinNumbersCount = 1;
    init.MinSpecialCharsCount = 1;
    init.SpecialChars = "!@#$%^&*";
    init.CanContainUsername = false;

    NLogin::TPasswordComplexity complexity(init);
    NLogin::TPasswordChecker checker(complexity);
    try {
        auto result = checker.Check(username, password);
        (void)result;
    } catch (...) {}
    return 0;
}
