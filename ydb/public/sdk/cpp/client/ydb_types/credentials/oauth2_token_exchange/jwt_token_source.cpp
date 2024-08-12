#include "jwt_token_source.h"

#include <jwt-cpp/jwt.h>

#include <chrono>
#include <set>

#define INV_ARG "Invalid argument for JWT token source: "

namespace NYdb {

static const TString TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";

class TJwtTokenSource: public ITokenSource {
public:
    TJwtTokenSource(const TJwtTokenSourceParams& params)
        : Params(params)
    {
        if (!Params.SigningAlgorithm_) {
            throw std::invalid_argument(INV_ARG "no signing algorithm");
        }

        if (!Params.TokenTtl_) {
            throw std::invalid_argument(INV_ARG "token TTL must be positive");
        }

        for (const auto& aud : Params.Audience_) {
            if (aud.empty()) {
                throw std::invalid_argument(INV_ARG "empty audience");
            }
        }
    }

    TToken GetToken() const override {
        TToken t;
        t.TokenType = TOKEN_TYPE;

        auto tokenBuilder = jwt::create();
        tokenBuilder.set_type("JWT");

        const auto now = std::chrono::system_clock::now();
        const auto expire = now + std::chrono::microseconds(Params.TokenTtl_.MicroSeconds());
        tokenBuilder.set_issued_at(now);
        tokenBuilder.set_expires_at(expire);

        if (Params.KeyId_) {
            tokenBuilder.set_key_id(Params.KeyId_);
        }

        if (Params.Issuer_) {
            tokenBuilder.set_issuer(Params.Issuer_);
        }

        if (Params.Subject_) {
            tokenBuilder.set_subject(Params.Subject_);
        }

        if (Params.Id_) {
            tokenBuilder.set_id(Params.Id_);
        }

        if (Params.Audience_.size() == 1) {
            tokenBuilder.set_audience(Params.Audience_[0]);
        } else if (Params.Audience_.size() > 1) {
            std::set<std::string> aud(Params.Audience_.begin(), Params.Audience_.end());
            tokenBuilder.set_audience(aud);
        }

        t.Token = tokenBuilder.sign(*Params.SigningAlgorithm_);
        return t;
    }

private:
    TJwtTokenSourceParams Params;
};

std::shared_ptr<ITokenSource> CreateJwtTokenSource(const TJwtTokenSourceParams& params) {
    return std::make_shared<TJwtTokenSource>(params);
}

} // namespace NYdb
