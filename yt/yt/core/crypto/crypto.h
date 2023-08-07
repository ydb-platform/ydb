#pragma once

#include <yt/yt_proto/yt/core/crypto/proto/crypto.pb.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/strbuf.h>

#include <array>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

typedef std::array<char, 16> TMD5Hash;
typedef std::array<char, 92> TMD5State;

TMD5Hash MD5FromString(TStringBuf data);

class TMD5Hasher
{
public:
    TMD5Hasher();
    explicit TMD5Hasher(const TMD5State& data);

    TMD5Hasher& Append(TStringBuf data);
    TMD5Hasher& Append(TRef data);

    TMD5Hash GetDigest();
    TString GetHexDigestLowerCase();
    TString GetHexDigestUpperCase();

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

typedef std::array<char, 20> TSha1Hash;

TSha1Hash Sha1FromString(TStringBuf data);

class TSha1Hasher
{
public:
    TSha1Hasher();

    TSha1Hasher& Append(TStringBuf data);

    TSha1Hash GetDigest();
    TString GetHexDigestLowerCase();
    TString GetHexDigestUpperCase();

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
    TDigest GetDigest();

    TString GetHexDigestLowerCase();
    TString GetHexDigestUpperCase();

private:
    constexpr static int CtxSize = 112;
    std::array<char, CtxSize> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

TString GetSha256HexDigestUpperCase(TStringBuf data);
TString GetSha256HexDigestLowerCase(TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

TString CreateSha256Hmac(const TString& key, const TString& message);
TString CreateSha256HmacRaw(const TString& key, const TString& message);

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

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
