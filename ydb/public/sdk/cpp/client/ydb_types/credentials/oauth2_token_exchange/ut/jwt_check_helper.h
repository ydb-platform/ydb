#include "jwt_token_source.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

extern const TString TestRSAPrivateKeyContent;
extern const TString TestRSAPublicKeyContent;

struct TJwtCheck {
    using TSelf = TJwtCheck;

    struct IAlgCheck {
        virtual void Check(const jwt::decoded_jwt& decoded) const = 0;
        virtual ~IAlgCheck() = default;
    };

    template <class TAlg>
    struct TAlgCheck : public IAlgCheck {
        TAlgCheck(const TString& publicKey)
            : Alg(publicKey)
        {}

        void Check(const jwt::decoded_jwt& decoded) const override {
            UNIT_ASSERT_VALUES_EQUAL(decoded.get_algorithm(), Alg.name());
            const std::string data = decoded.get_header_base64() + "." + decoded.get_payload_base64();
		    const std::string signature = decoded.get_signature();
            Alg.verify(data, signature); // Throws
        }

        TAlg Alg;
    };

    template <class TAlg>
    TSelf& Alg(const TString& publicKey) {
        Alg_.Reset(new TAlgCheck<TAlg>(publicKey));
        return *this;
    }
    THolder<IAlgCheck> Alg_ = MakeHolder<TAlgCheck<jwt::algorithm::rs256>>(TestRSAPublicKeyContent);

    FLUENT_SETTING_OPTIONAL(TString, KeyId);

    FLUENT_SETTING_OPTIONAL(TString, Issuer);
    FLUENT_SETTING_OPTIONAL(TString, Subject);
    FLUENT_SETTING_OPTIONAL(TString, Id);
    FLUENT_SETTING_VECTOR_OR_SINGLE(TString, Audience);
    FLUENT_SETTING_DEFAULT(TDuration, TokenTtl, TDuration::Hours(1));

    void Check(const TString& token) {
        jwt::decoded_jwt decoded(token);
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
            for (const TString& a : Audience_) {
                UNIT_ASSERT(aud.contains(a));
            }
        } else {
            UNIT_ASSERT(!decoded.has_audience());
        }
    }
};
