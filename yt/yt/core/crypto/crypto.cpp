#include "crypto.h"

#include <yt/yt/core/misc/error.h>

#include <util/string/hex.h>

#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/rand.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

TMD5Hash MD5FromString(TStringBuf data)
{
    TMD5Hash hash;
    if (data.size() != hash.size()) {
        THROW_ERROR_EXCEPTION("Invalid MD5 hash size")
            << TErrorAttribute("expected", hash.size())
            << TErrorAttribute("actual", data.size());
    }

    std::copy(data.begin(), data.end(), hash.begin());
    return hash;
}

static_assert(
    sizeof(MD5_CTX) == sizeof(TMD5Hasher),
    "TMD5Hasher size must be exactly equal to that of MD5_CTX");

TMD5Hasher::TMD5Hasher()
{
    MD5_Init(reinterpret_cast<MD5_CTX*>(State_.data()));
}

TMD5Hasher::TMD5Hasher(const TMD5State& data)
    : State_(data)
{ }

TMD5Hasher& TMD5Hasher::Append(TStringBuf data)
{
    MD5_Update(reinterpret_cast<MD5_CTX*>(State_.data()), data.data(), data.size());
    return *this;
}

TMD5Hasher& TMD5Hasher::Append(TRef data)
{
    MD5_Update(reinterpret_cast<MD5_CTX*>(State_.data()), data.Begin(), data.Size());
    return *this;
}

TMD5Hash TMD5Hasher::GetDigest() const
{
    TMD5Hash hash;
    auto stateCopy = State_;
    MD5_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<MD5_CTX*>(stateCopy.data()));
    return hash;
}

std::string TMD5Hasher::GetHexDigestUpperCase() const
{
    auto md5 = GetDigest();
    return HexEncode(md5.data(), md5.size());
}

std::string TMD5Hasher::GetHexDigestLowerCase() const
{
    // TODO(babenko): migrate to std::string
    return to_lower(TString(GetHexDigestUpperCase()));
}

void TMD5Hasher::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, State_);
}

const TMD5State& TMD5Hasher::GetState() const
{
    return State_;
}

////////////////////////////////////////////////////////////////////////////////

std::string GetMD5HexDigestUpperCase(TStringBuf data)
{
    TMD5Hasher hasher;
    hasher.Append(data);
    return hasher.GetHexDigestUpperCase();
}

std::string GetMD5HexDigestLowerCase(TStringBuf data)
{
    TMD5Hasher hasher;
    hasher.Append(data);
    return hasher.GetHexDigestLowerCase();
}

////////////////////////////////////////////////////////////////////////////////

TSha1Hash Sha1FromString(TStringBuf data)
{
    TSha1Hash hash;
    if (data.size() != hash.size()) {
        THROW_ERROR_EXCEPTION("Invalid Sha1 hash size")
            << TErrorAttribute("expected", hash.size())
            << TErrorAttribute("actual", data.size());
    }

    std::copy(data.begin(), data.end(), hash.begin());
    return hash;
}

static_assert(
    sizeof(SHA_CTX) == sizeof(TSha1Hasher),
    "TSha1Hasher size must be exactly equal to that of SHA1_CTX");

TSha1Hasher::TSha1Hasher()
{
    SHA1_Init(reinterpret_cast<SHA_CTX*>(CtxStorage_.data()));
}

TSha1Hasher& TSha1Hasher::Append(TStringBuf data)
{
    SHA1_Update(reinterpret_cast<SHA_CTX*>(CtxStorage_.data()), data.data(), data.size());
    return *this;
}

TSha1Hash TSha1Hasher::GetDigest() const
{
    TSha1Hash hash;
    auto stateCopy = CtxStorage_;
    SHA1_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<SHA_CTX*>(stateCopy.data()));
    return hash;
}

std::string TSha1Hasher::GetHexDigestUpperCase() const
{
    auto sha1 = GetDigest();
    return HexEncode(sha1.data(), sha1.size());
}

std::string TSha1Hasher::GetHexDigestLowerCase() const
{
    // TODO(babenko): migrate to std::string
    return to_lower(TString(GetHexDigestUpperCase()));
}

////////////////////////////////////////////////////////////////////////////////

TSha256Hasher::TSha256Hasher()
{
    static_assert(
        sizeof(CtxStorage_) == sizeof(SHA256_CTX),
        "Invalid ctx storage size");

    SHA256_Init(reinterpret_cast<SHA256_CTX*>(CtxStorage_.data()));
}

TSha256Hasher& TSha256Hasher::Append(TStringBuf data)
{
    SHA256_Update(
        reinterpret_cast<SHA256_CTX*>(CtxStorage_.data()),
        data.data(),
        data.size());
    return *this;
}

TSha256Hasher::TDigest TSha256Hasher::GetDigest() const
{
    TDigest digest;
    auto stateCopy = CtxStorage_;
    SHA256_Final(
        reinterpret_cast<unsigned char*>(digest.data()),
        reinterpret_cast<SHA256_CTX*>(stateCopy.data()));
    return digest;
}

std::string TSha256Hasher::GetHexDigestUpperCase() const
{
    auto digest = GetDigest();
    return HexEncode(digest.data(), digest.size());
}

std::string TSha256Hasher::GetHexDigestLowerCase() const
{
    // TODO(babenko): migrate to std::string
    return to_lower(TString(GetHexDigestUpperCase()));
}

////////////////////////////////////////////////////////////////////////////////

std::string GetSha256HexDigestUpperCase(TStringBuf data)
{
    TSha256Hasher hasher;
    hasher.Append(data);
    return hasher.GetHexDigestUpperCase();
}

std::string GetSha256HexDigestLowerCase(TStringBuf data)
{
    TSha256Hasher hasher;
    hasher.Append(data);
    return hasher.GetHexDigestLowerCase();
}

////////////////////////////////////////////////////////////////////////////////

using TSha256Hmac = std::array<char, 256 / 8>;

TSha256Hmac CreateSha256HmacImpl(TStringBuf key, TStringBuf message)
{
    TSha256Hmac hmac;
    unsigned int opensslIsInsane;
    auto* result = HMAC(
        EVP_sha256(),
        key.data(),
        key.size(),
        reinterpret_cast<const unsigned char*>(message.data()),
        message.size(),
        reinterpret_cast<unsigned char*>(hmac.data()),
        &opensslIsInsane);
    YT_VERIFY(nullptr != result);
    return hmac;
}

std::string CreateSha256Hmac(TStringBuf key, TStringBuf message)
{
    auto hmac = CreateSha256HmacImpl(key, message);
    return to_lower(HexEncode(hmac.data(), hmac.size()));
}

std::string CreateSha256HmacRaw(TStringBuf key, TStringBuf message)
{
    auto hmac = CreateSha256HmacImpl(key, message);
    return std::string(hmac.data(), hmac.size());
}

bool ConstantTimeCompare(TStringBuf trusted, TStringBuf untrusted)
{
    int total = trusted.size() != untrusted.size() ? 1 : 0;

    for (size_t i = 0; i < std::min(trusted.size(), untrusted.size()); ++i) {
        total |= trusted[i] ^ untrusted[i];
    }

    return total == 0;
}

////////////////////////////////////////////////////////////////////////////////

std::string HashPassword(const std::string& password, const std::string& salt)
{
    auto passwordSha256 = GetSha256HexDigestLowerCase(password);
    return HashPasswordSha256(passwordSha256, salt);
}

std::string HashPasswordSha256(const std::string& passwordSha256, const std::string& salt)
{
    auto saltedPassword = salt + passwordSha256;
    return GetSha256HexDigestLowerCase(saltedPassword);
}

////////////////////////////////////////////////////////////////////////////////

std::string GenerateCryptoStrongRandomString(int length)
{
    std::vector<unsigned char> bytes(length);
    if (RAND_bytes(bytes.data(), bytes.size())) {
        auto* data = reinterpret_cast<char*>(bytes.data());
        return std::string{data, static_cast<size_t>(length)};
    } else {
        THROW_ERROR_EXCEPTION("Failed to generate %v random bytes", length)
            << TErrorAttribute("openssl_error_code", ERR_get_error());
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NCrypto::NProto::TMD5Hasher* protoHasher, const std::optional<NYT::NCrypto::TMD5Hasher>& hasher)
{
    auto* outputBytes = protoHasher->mutable_state();
    outputBytes->clear();
    if (!hasher) {
        return;
    }

    const auto& state = hasher->GetState();
    outputBytes->assign(state.begin(), state.end());
}

void FromProto(std::optional<NYT::NCrypto::TMD5Hasher>* hasher, const NCrypto::NProto::TMD5Hasher& protoHasher)
{
    const auto& inputBytes = protoHasher.state();
    hasher->reset();
    if (inputBytes.empty()) {
        return;
    }

    TMD5State state;
    std::copy(inputBytes.begin(), inputBytes.end(), state.data());

    hasher->emplace(state);
}

void ToProto(NCrypto::NProto::TMD5Hash* protoHash, const std::optional<NYT::NCrypto::TMD5Hash>& hash)
{
    auto* outputBytes = protoHash->mutable_data();
    outputBytes->clear();
    if (!hash) {
        return;
    }

    outputBytes->assign(hash->begin(), hash->end());
}

void FromProto(std::optional<NYT::NCrypto::TMD5Hash>* hash, const NCrypto::NProto::TMD5Hash& protoHash)
{
    const auto& inputBytes = protoHash.data();
    hash->reset();
    if (inputBytes.empty()) {
        return;
    }

    hash->emplace(MD5FromString(inputBytes));
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
