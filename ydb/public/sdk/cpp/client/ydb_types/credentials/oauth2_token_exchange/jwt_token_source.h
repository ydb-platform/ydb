#pragma once
#include "credentials.h"
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NYdb {

constexpr TDuration DEFAULT_JWT_TOKEN_TTL = TDuration::Hours(1);

struct TJwtTokenSourceParams {
    using TSelf = TJwtTokenSourceParams;

    FLUENT_SETTING(TString, KeyId);

    template <class TAlg, class... T>
    TSelf& SigningAlgorithm(T&&... args) {
        SigningAlgorithm_ = std::make_shared<TJwtSigningAlgorithm<TAlg>>(std::forward<T>(args)...);
        return *this;
    }

    // JWT Claims
    FLUENT_SETTING(TString, Issuer);
    FLUENT_SETTING(TString, Subject);
    FLUENT_SETTING(TString, Id);
    FLUENT_SETTING_VECTOR_OR_SINGLE(TString, Audience);

    FLUENT_SETTING_DEFAULT(TDuration, TokenTtl, DEFAULT_JWT_TOKEN_TTL);


    // Helpers
    class ISigningAlgorithm {
    public:
        virtual ~ISigningAlgorithm() = default;
        virtual std::string sign(const std::string& data) const = 0;
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

        std::string sign(const std::string& data) const override {
            return Alg.sign(data);
        }

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
