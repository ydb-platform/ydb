#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/jwt_token_source.h>
#include <library/cpp/testing/unittest/registar.h>

#include <jwt-cpp/jwt.h>

extern const std::string TestRSAPrivateKeyContent;
extern const std::string TestRSAPublicKeyContent;

struct TJwtCheck {
    using TSelf = TJwtCheck;
#ifdef YDB_SDK_USE_NEW_JWT
    using TDecodedJwt = jwt::decoded_jwt<jwt::traits::kazuho_picojson>;
#else
    using TDecodedJwt = jwt::decoded_jwt;
#endif

    struct IAlgCheck {
        virtual void Check(const TDecodedJwt& decoded) const = 0;
        virtual ~IAlgCheck() = default;
    };

    template <class TAlg>
    struct TAlgCheck : public IAlgCheck {
        TAlgCheck(const std::string& publicKey)
            : Alg(publicKey)
        {}

        void Check(const TDecodedJwt& decoded) const override {
            UNIT_ASSERT_VALUES_EQUAL(decoded.get_algorithm(), Alg.name());
            const std::string data = decoded.get_header_base64() + "." + decoded.get_payload_base64();
		    const std::string signature = decoded.get_signature();
#ifdef YDB_SDK_USE_NEW_JWT
            std::error_code ec;
            Alg.verify(data, signature, ec);
            if (ec) {
                throw std::runtime_error(ec.message());
            }
#else
            Alg.verify(data, signature); // Throws
#endif
        }

        TAlg Alg;
    };

    template <class TAlg>
    TSelf& Alg(const std::string& publicKey) {
        Alg_.reset(new TAlgCheck<TAlg>(publicKey));
        return *this;
    }
    std::unique_ptr<IAlgCheck> Alg_ = std::make_unique<TAlgCheck<jwt::algorithm::rs256>>(TestRSAPublicKeyContent);

    FLUENT_SETTING_OPTIONAL(std::string, KeyId);

    FLUENT_SETTING_OPTIONAL(std::string, Issuer);
    FLUENT_SETTING_OPTIONAL(std::string, Subject);
    FLUENT_SETTING_OPTIONAL(std::string, Id);
    FLUENT_SETTING_VECTOR_OR_SINGLE(std::string, Audience);
    FLUENT_SETTING_DEFAULT(TDuration, TokenTtl, TDuration::Hours(1));

    void Check(const std::string& token) {
        TDecodedJwt decoded(token);

        UNIT_ASSERT_VALUES_EQUAL(decoded.get_type(), "JWT");
        Alg_->Check(decoded);

        const auto now = std::chrono::system_clock::from_time_t(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
        UNIT_ASSERT(decoded.get_issued_at() >= now - std::chrono::minutes(10));
        UNIT_ASSERT(decoded.get_issued_at() <= now);
        UNIT_ASSERT_EQUAL(decoded.get_expires_at() - decoded.get_issued_at(), std::chrono::seconds(TokenTtl_.Seconds()));

#define CHECK_JWT_CLAIM(claim, option)                                  \
        if (option) {                                                   \
            UNIT_ASSERT(decoded.has_ ## claim());                       \
            UNIT_ASSERT_VALUES_EQUAL(decoded.get_ ## claim(), *option); \
        } else {                                                        \
            UNIT_ASSERT(!decoded.has_ ## claim());                      \
        }

        CHECK_JWT_CLAIM(key_id, KeyId_);
        CHECK_JWT_CLAIM(issuer, Issuer_);
        CHECK_JWT_CLAIM(subject, Subject_);
        CHECK_JWT_CLAIM(id, Id_);

#undef CHECK_JWT_CLAIM

        if (!Audience_.empty()) {
            UNIT_ASSERT(decoded.has_audience());
            const std::set<std::string> aud = decoded.get_audience();
            UNIT_ASSERT_VALUES_EQUAL(aud.size(), Audience_.size());
            for (const std::string& a : Audience_) {
                UNIT_ASSERT(aud.contains(a));
            }
        } else {
            UNIT_ASSERT(!decoded.has_audience());
        }
    }
};
