#pragma once

#include <ydb-cpp-sdk/client/types/fwd.h>

#include "credentials.h"

#include <ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

#include <util/datetime/base.h>

namespace NYdb::inline V3 {

constexpr TDuration DEFAULT_JWT_TOKEN_TTL = TDuration::Hours(1);

struct TJwtTokenSourceParams {
    using TSelf = TJwtTokenSourceParams;

    FLUENT_SETTING(std::string, KeyId);

    template <class TAlg, class... T>
    TSelf& SigningAlgorithm(T&&... args) {
        SigningAlgorithm_ = std::make_shared<TJwtSigningAlgorithm<TAlg>>(std::forward<T>(args)...);
        return *this;
    }

    // JWT Claims
    FLUENT_SETTING(std::string, Issuer);
    FLUENT_SETTING(std::string, Subject);
    FLUENT_SETTING(std::string, Id);
    FLUENT_SETTING_VECTOR_OR_SINGLE(std::string, Audience);

    FLUENT_SETTING_DEFAULT(TDuration, TokenTtl, DEFAULT_JWT_TOKEN_TTL);


    // Helpers
    class ISigningAlgorithm {
    public:
        virtual ~ISigningAlgorithm() = default;
#ifdef YDB_SDK_USE_NEW_JWT
        virtual std::string sign(const std::string& data, std::error_code& ec) const = 0;
#else
        virtual std::string sign(const std::string& data) const = 0;
#endif
        virtual std::string name() const = 0;
    };

    // Interface implementation for jwt-cpp algorithm classes
    template <class TAlg>
    class TJwtSigningAlgorithm: public ISigningAlgorithm {
    public:
        template <class... T>
        explicit TJwtSigningAlgorithm(T&&... args)
            : Alg(std::forward<T>(args)...)
        {
        }

#ifdef YDB_SDK_USE_NEW_JWT
        std::string sign(const std::string& data, std::error_code& ec) const override {
            return Alg.sign(data, ec);
        }
#else
        std::string sign(const std::string& data) const override {
            return Alg.sign(data);
        }
#endif

        std::string name() const override {
            return Alg.name();
        }

    private:
        TAlg Alg;
    };

    std::shared_ptr<ISigningAlgorithm> SigningAlgorithm_;
};

std::shared_ptr<ITokenSource> CreateJwtTokenSource(const TJwtTokenSourceParams& params);

} // namespace NYdb
