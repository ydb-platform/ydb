#pragma once

#include <yt/yt_proto/yt/core/crypto/proto/crypto.pb.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/strbuf.h>

#include <array>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

using TMD5Hash = std::array<char, 16>;
using TMD5State = std::array<char, 92>;

TMD5Hash MD5FromString(TStringBuf data);

class TMD5Hasher
{
public:
    TMD5Hasher();
    explicit TMD5Hasher(const TMD5State& data);

    TMD5Hasher& Append(TStringBuf data);
    TMD5Hasher& Append(TRef data);

    TMD5Hash GetDigest() const;
    std::string GetHexDigestLowerCase() const;
    std::string GetHexDigestUpperCase() const;

    const TMD5State& GetState() const;

    void Persist(const TStreamPersistenceContext& context);

private:
    //! Erasing openssl struct type... brutally.
    TMD5State State_;
};

////////////////////////////////////////////////////////////////////////////////

std::string GetMD5HexDigestUpperCase(TStringBuf data);
std::string GetMD5HexDigestLowerCase(TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

using TSha1Hash = std::array<char, 20>;

TSha1Hash Sha1FromString(TStringBuf data);

class TSha1Hasher
{
public:
    TSha1Hasher();

    TSha1Hasher& Append(TStringBuf data);

    TSha1Hash GetDigest() const;
    std::string GetHexDigestLowerCase() const;
    std::string GetHexDigestUpperCase() const;

private:
    std::array<char, 96> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

class TSha256Hasher
{
public:
    TSha256Hasher();

    TSha256Hasher& Append(TStringBuf data);

    using TDigest = std::array<char, 32>;

    TDigest GetDigest() const;
    std::string GetHexDigestLowerCase() const;
    std::string GetHexDigestUpperCase() const;

private:
    static constexpr int CtxSize = 112;
    std::array<char, CtxSize> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

std::string GetSha256HexDigestUpperCase(TStringBuf data);
std::string GetSha256HexDigestLowerCase(TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

std::string CreateSha256Hmac(TStringBuf key, TStringBuf message);
std::string CreateSha256HmacRaw(TStringBuf key, TStringBuf message);

bool ConstantTimeCompare(TStringBuf trusted, TStringBuf untrusted);

////////////////////////////////////////////////////////////////////////////////

//! Hashes password with given (random) salt.
// BEWARE: Think twice before changing this function's semantics!
std::string HashPassword(const std::string& password, const std::string& salt);

//! Hashes SHA256-hashed password with given (random) salt.
std::string HashPasswordSha256(const std::string& passwordSha256, const std::string& salt);

////////////////////////////////////////////////////////////////////////////////

//! Returns string of given length filled with random bytes fetched
//! from cryptographically strong generator.
// NB: May throw on RNG failure.
std::string GenerateCryptoStrongRandomString(int length);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NCrypto::NProto::TMD5Hasher* protoHasher, const std::optional<NYT::NCrypto::TMD5Hasher>& hasher);
void FromProto(std::optional<NYT::NCrypto::TMD5Hasher>* hasher, const NCrypto::NProto::TMD5Hasher& protoHasher);

void ToProto(NCrypto::NProto::TMD5Hash* protoHash, const std::optional<NYT::NCrypto::TMD5Hash>& hash);
void FromProto(std::optional<NYT::NCrypto::TMD5Hash>* hash, const NCrypto::NProto::TMD5Hash& protoHash);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
