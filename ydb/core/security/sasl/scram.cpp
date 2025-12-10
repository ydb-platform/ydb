#include "scram.h"

#include <cstdlib>
#include <format>
#include <iostream>

#include <ydb/core/security/sasl/saslprep.h>
#include <openssl/hmac.h>

namespace {

#ifdef SCRAM_DEBUG
/* Useful for debugging interop issues */
void PrintHash(const char* func, const char * hash, size_t hash_size) {
    std::cout << std::format(" HASH in {}:", func);
    for (size_t i = 0; i < hash_size; ++i) {
	    std::cout << std::format(" {:02X}", static_cast<unsigned char>(hash[i]));
    }
    std::cout << std::endl;
}
#endif

#ifdef SCRAM_DEBUG
#define PRINT_HASH(func,hash,size)  print_hash(func,hash,size)
#else
#define PRINT_HASH(func,hash,size)
#endif

std::string Hi(const EVP_MD* md,
    const std::string& str, const std::string& salt, ui32 iterationCount)
{
    char* initialKey = nullptr;
    ui32 i;
    char* tempResult;
    ui32 hashLen = 0;
    size_t k;
    size_t hashSize = EVP_MD_size(md);
    std::string result;
    result.resize(hashSize);

    initialKey = static_cast<char*>(malloc(salt.size() + 4));
    memcpy(initialKey, salt.data(), salt.size());
    initialKey[salt.size()] = 0;
    initialKey[salt.size() + 1] = 0;
    initialKey[salt.size() + 2] = 0;
    initialKey[salt.size() + 3] = 1;

    tempResult = static_cast<char*>(malloc(hashSize));

    /* U1 := HMAC(str, salt || INT(1)) */
    if (HMAC(md, str.data(), str.size(),
            reinterpret_cast<unsigned char*>(initialKey), salt.size() + 4,
            reinterpret_cast<unsigned char*>(result.data()), &hashLen) == nullptr) {
    }

    memcpy(tempResult, result.data(), hashSize);

    PRINT_HASH ("first HMAC in Hi()", tempResult, hashSize);

    /* On each loop iteration j "temp_result" contains Uj,
       while "result" contains "U1 XOR ... XOR Uj" */
    for (i = 2; i <= iterationCount; ++i) {
        if (HMAC(md, str.data(), str.size(),
            reinterpret_cast<unsigned char*>(tempResult), hashSize,
            reinterpret_cast<unsigned char*>(tempResult), &hashLen) == nullptr) {
        }

	    PRINT_HASH ("Hi() HMAC inside loop", tempResult, hashSize);

        for (k = 0; k < hashSize; k++) {
            result[k] ^= tempResult[k];
        }

	    PRINT_HASH ("Hi() - accumulated result inside loop", result, hashSize);
    }

    free(initialKey);
    free(tempResult);

    return result;
}

std::string GetDigestNameFromHashType(const std::string& hashType) {
    if (hashType.size() < 6) {
        return "";
    }

    std::string result = hashType.substr(6);
    std::erase(result, '-');
    return result;
}

} // namespace

namespace NKikimr::NSasl {

constexpr std::string_view ClientKeyConstant = "Client Key";
constexpr std::string_view ServerKeyConstant = "Server Key";

bool GenerateScramSecrets(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& storedKey, std::string& serverKey, std::string& errorText)
{
    std::string digestName = GetDigestNameFromHashType(hashType);
    const EVP_MD* md = EVP_get_digestbyname(digestName.c_str());
    if (md == nullptr) {
        errorText = "Unsupported hash type: " + hashType;
        return false;
    }

    std::string prepPassword;
    auto saslPrepRC = SaslPrep(password, prepPassword);
    if (saslPrepRC != ESaslPrepReturnCodes::Success) {
        errorText = "Unsupported password format";
        return false;
    }

    /* SaltedPassword := Hi(Normalize(password), salt, i) */
    std::string saltedPassword = Hi(md, prepPassword, salt, iterationsCount);

    size_t hashSize = EVP_MD_size(md);
    std::string clientKey;
    clientKey.resize(hashSize);
    ui32 hashLen = 0;

    /* ClientKey := HMAC(SaltedPassword, "Client Key") */
    if (HMAC(md, saltedPassword.data(), saltedPassword.size(),
	    reinterpret_cast<const unsigned char*>(ClientKeyConstant.data()), ClientKeyConstant.size(),
	    reinterpret_cast<unsigned char*>(clientKey.data()), &hashLen) == nullptr)
    {
	    errorText = "HMAC call failed";
	    return false;
    }

    storedKey.resize(hashSize);
    /* StoredKey := H(ClientKey) */
    if (EVP_Digest(reinterpret_cast<const unsigned char*>(clientKey.data()), clientKey.size(),
                   reinterpret_cast<unsigned char*>(storedKey.data()), nullptr, md, nullptr) == 0)
    {
        errorText = "Digest call failed";
        return false;
    }

    serverKey.resize(hashSize);
    /* ServerKey := HMAC(SaltedPassword, "Server Key") */
    if (HMAC(md, saltedPassword.data(), saltedPassword.size(),
	    reinterpret_cast<const unsigned char*>(ServerKeyConstant.data()), ServerKeyConstant.size(),
	    reinterpret_cast<unsigned char*>(serverKey.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    return true;
}

} // namespace NKikimr::NSasl
