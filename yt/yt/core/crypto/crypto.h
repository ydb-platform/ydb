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
    TString GetHexDigestLowerCase() const;
    TString GetHexDigestUpperCase() const;

    const TMD5State& GetState() const;

    void Persist(const TStreamPersistenceContext& context);

private:
    //! Erasing openssl struct type... brutally.
    TMD5State State_;
};

////////////////////////////////////////////////////////////////////////////////

TString GetMD5HexDigestUpperCase(TStringBuf data);
TString GetMD5HexDigestLowerCase(TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

using TSha1Hash = std::array<char, 20>;

TSha1Hash Sha1FromString(TStringBuf data);

class TSha1Hasher
{
public:
    TSha1Hasher();

    TSha1Hasher& Append(TStringBuf data);

    TSha1Hash GetDigest() const;
    TString GetHexDigestLowerCase() const;
    TString GetHexDigestUpperCase() const;

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
    TString GetHexDigestLowerCase() const;
    TString GetHexDigestUpperCase() const;

private:
    static constexpr int CtxSize = 112;
    std::array<char, CtxSize> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

TString GetSha256HexDigestUpperCase(TStringBuf data);
TString GetSha256HexDigestLowerCase(TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

TString CreateSha256Hmac(TStringBuf key, TStringBuf message);
TString CreateSha256HmacRaw(TStringBuf key, TStringBuf message);

bool ConstantTimeCompare(const TString& trusted, const TString& untrusted);

////////////////////////////////////////////////////////////////////////////////

//! Hashes password with given (random) salt.
// BEWARE: Think twice before changing this function's semantics!
TString HashPassword(const TString& password, const TString& salt);

//! Hashes SHA256-hashed password with given (random) salt.
TString HashPasswordSha256(const TString& passwordSha256, const TString& salt);

////////////////////////////////////////////////////////////////////////////////

//! Returns string of given length filled with random bytes fetched
//! from cryptographically strong generator.
// NB: May throw on RNG failure.
TString GenerateCryptoStrongRandomString(int length);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NCrypto::NProto::TMD5Hasher* protoHasher, const std::optional<NYT::NCrypto::TMD5Hasher>& hasher);
void FromProto(std::optional<NYT::NCrypto::TMD5Hasher>* hasher, const NCrypto::NProto::TMD5Hasher& protoHasher);

void ToProto(NCrypto::NProto::TMD5Hash* protoHash, const std::optional<NYT::NCrypto::TMD5Hash>& hash);
void FromProto(std::optional<NYT::NCrypto::TMD5Hash>* hash, const NCrypto::NProto::TMD5Hash& protoHash);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
