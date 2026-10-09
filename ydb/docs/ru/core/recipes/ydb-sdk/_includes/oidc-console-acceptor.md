```cpp
#include <ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <iostream>
#include <mutex>

class TConsoleAcceptor final : public NYdb::NOidc::IAuthAcceptor {
public:
    void Accept(const NYdb::NOidc::TDeviceAuthInfo& info) override {
        static std::mutex outputMutex;
        std::lock_guard<std::mutex> guard(outputMutex);
        std::cerr << "Откройте " << Display(info.VerificationUrl)
                  << " и введите код " << Display(info.UserCode) << '\n';
        if (info.VerificationUrlComplete) {
            std::cerr << "Готовая ссылка: "
                      << Display(*info.VerificationUrlComplete) << '\n';
        }
        std::cerr << std::flush;
    }

private:
    static std::string Display(const std::string& value) {
        std::string result;
        for (unsigned char c : value) {
            result += c >= 0x20 && c < 0x7f ? static_cast<char>(c) : '?';
        }
        return result;
    }
};
```
