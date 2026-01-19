#include "scram.h"

#include <cstdlib>
#include <format>
#include <iostream>
#include <openssl/hmac.h>
#include <sstream>
#include <unordered_set>

#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/library/login/sasl/saslprep.h>

#include <util/charset/utf8.h>

namespace {

#ifdef SCRAM_DEBUG
// Useful for debugging interop issues
void PrintHash(const char* func, const char * hash, size_t hash_size) {
    std::cout << std::format(" HASH in {}:", func);
    for (size_t i = 0; i < hash_size; ++i) {
	    std::cout << std::format(" {:02X}", static_cast<unsigned char>(hash[i]));
    }
    std::cout << std::endl;
}
#endif

#ifdef SCRAM_DEBUG
#define PRINT_HASH(func,hash,size) print_hash(func,hash,size)
#else
#define PRINT_HASH(func,hash,size)
#endif

constexpr std::string_view CLIENT_KEY_CONSTANT = "Client Key";
constexpr std::string_view SERVER_KEY_CONSTANT = "Server Key";

const std::unordered_set<char> RESERVED_ATTRIBUTE_NAMES = {
    'a', 'n', 'm', 'r', 'c', 's', 'i', 'p', 'v', 'e',
};

bool IsBase64(const std::string_view value) {
    try {
        Base64StrictDecode(value);
        return true;
    } catch (...) {
        return false;
    }
}

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

    // U1 := HMAC(str, salt || INT(1))
    if (HMAC(md, str.data(), str.size(),
            reinterpret_cast<unsigned char*>(initialKey), salt.size() + 4,
            reinterpret_cast<unsigned char*>(result.data()), &hashLen) == nullptr) {
    }

    memcpy(tempResult, result.data(), hashSize);

    PRINT_HASH ("first HMAC in Hi()", tempResult, hashSize);

    // On each loop iteration j "temp_result" contains Uj,
    // while "result" contains "U1 XOR ... XOR Uj"
    for (i = 2; i <= iterationCount; ++i) {
        if (HMAC(md, str.data(), str.size(),
            reinterpret_cast<unsigned char*>(tempResult), hashSize,
            reinterpret_cast<unsigned char*>(tempResult), &hashLen) == nullptr) {
        }

	    PRINT_HASH ("Hi() HMAC inside loop", tempResult, hashSize);

        for (k = 0; k < hashSize; ++k) {
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

std::optional<std::pair<char, const std::string_view>> ReadAttribute(const std::string_view msg, size_t& curPos) {
    if (curPos == msg.size()) {
        return std::nullopt;
    }

	// attr-val = ALPHA "=" value
	// Generic syntax of any attribute sent by server or client
    if (!((msg[curPos] >= 'A' && msg[curPos] <= 'Z') || (msg[curPos] >= 'a' && msg[curPos] <= 'z'))) {
        return std::nullopt;
    }

    char attrName = msg[curPos++];
    if (curPos == msg.size() || msg[curPos++] != '=') {
        return std::nullopt;
    }

    size_t attrValueBegin = curPos;
    while (curPos < msg.size() && msg[curPos] != ',') {
        if (msg[curPos] == '\0') {
            return std::nullopt;
        }

        ++curPos;
    }

    std::string_view attrValue = msg.substr(attrValueBegin, curPos - attrValueBegin);
    if (attrValue.empty()) {
        return std::nullopt;
    } else {
        return std::make_pair(std::move(attrName), attrValue);
    }
}

bool IsValidChannelBindingName(const std::string_view cbName) {
    for (size_t i = 0; i < cbName.size(); ++i) {
        if (!((cbName[i] >= 'A' && cbName[i] <= 'Z') || (cbName[i] >= 'a' && cbName[i] <= 'z')
            || (cbName[i] >= '0' && cbName[i] <= '9') || cbName[i] == '.' || cbName[i] == '-')) {
            return false;
        }
    }

    return true;
}

// Convert saslname = 1*(value-safe-char / "=2C" / "=3D")
// ',' and '=' are escaped in authzid and authzid with "=2C" and "=3D" respectively
std::string DecodeSaslname(const std::string_view saslname) {
    std::string decodedSaslName(saslname);
    size_t newSize = 0;

    for (size_t i = 0; i < saslname.size();) {
        if (saslname[i] == '=') {
            ++i;
            if (i >= saslname.size() - 1) {
                return "";
            }

            if (saslname[i] == '2' && saslname[i + 1] == 'C') {
                decodedSaslName[newSize++] = ',';
            } else if (saslname[i] == '3' && saslname[i + 1] == 'D') {
                decodedSaslName[newSize++] = '=';
            } else {
                return "";
            }

            i += 2;
        } else if (saslname[i] == ',') {
                return "";
        } else {
            decodedSaslName[newSize++] = saslname[i];
            ++i;
        }
    }

    decodedSaslName.resize(newSize);
    return decodedSaslName;
}

// Does string consist of printable characters defined by SCRAM spec
bool IsPrintable(const std::string_view str) {
	for (size_t i = 0; i < str.size(); ++i) {
        // printable = %x21-2B / %x2D-7E; Printable ASCII except ","
		if (str[i] < 0x21 || str[i] > 0x7E || str[i] == ',') {
			return false;
        }
	}

	return true;
}

bool IsPositiveInt(const std::string_view str) {
    if (str.empty()) {
	    return false;
	}

	try {
        ui32 result = std::stoul(std::string(str));
        if (result == 0) {
            return false;
        } else {
            return true;
        }
    } catch (...) {
        return false;
    }
}

} // namespace

namespace NLogin::NSasl {

namespace {

EParseMsgReturnCodes ParseGS2Header(const std::string_view msgView, size_t& curPos, GS2Header& parsedHeader) {
    if (curPos == msgView.size()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    //read gs2-cbind-flag
    switch (msgView[curPos]) {
    case 'n': {
        parsedHeader.ChannelBindingFlag = EClientChannelBindingFlag::NotSupported;
        ++curPos;
        break;
    }
    case 'y': {
        parsedHeader.ChannelBindingFlag = EClientChannelBindingFlag::Supported;
        ++curPos;
        break;
    }
    default: {
        if (auto cbAttr = ReadAttribute(msgView, curPos); cbAttr.has_value()) {
            if (cbAttr->first == 'p') {
                parsedHeader.ChannelBindingFlag = EClientChannelBindingFlag::Required;
            } else {
                return EParseMsgReturnCodes::InvalidFormat;
            }

            const std::string_view requestedChannelBinding = cbAttr->second;
            if (!IsValidChannelBindingName(requestedChannelBinding)) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                parsedHeader.RequestedChannelBinding = requestedChannelBinding;
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }
        break;
    }
    }

    if (curPos == msgView.size() || msgView[curPos++] != ',') {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    if (curPos == msgView.size()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read authzid
    if (msgView[curPos] != ',') {
        if (auto authzIdAttr = ReadAttribute(msgView, curPos); authzIdAttr.has_value()) {
            if (authzIdAttr->first != 'a') {
                return EParseMsgReturnCodes::InvalidFormat;
            }

            const std::string_view authzId = authzIdAttr->second;
            std::string decodedAuthzId = DecodeSaslname(authzId);
            if (decodedAuthzId.empty() || !IsUtf(decodedAuthzId.data(), decodedAuthzId.size())) {
                return EParseMsgReturnCodes::InvalidUsernameEncoding;
            } else {
                parsedHeader.AuthzId = std::move(decodedAuthzId);
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;;
        }
    }

    if (curPos == msgView.size() || msgView[curPos++] != ',') {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    return EParseMsgReturnCodes::Success;
}

} // namespace

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

    // SaltedPassword := Hi(Normalize(password), salt, i)
    std::string saltedPassword = Hi(md, prepPassword, salt, iterationsCount);

    size_t hashSize = EVP_MD_size(md);
    std::string clientKey;
    clientKey.resize(hashSize);
    ui32 hashLen = 0;

    // ClientKey := HMAC(SaltedPassword, "Client Key")
    if (HMAC(md, saltedPassword.data(), saltedPassword.size(),
	    reinterpret_cast<const unsigned char*>(CLIENT_KEY_CONSTANT.data()), CLIENT_KEY_CONSTANT.size(),
	    reinterpret_cast<unsigned char*>(clientKey.data()), &hashLen) == nullptr)
    {
	    errorText = "HMAC call failed";
	    return false;
    }

    storedKey.resize(hashSize);
    // StoredKey := H(ClientKey)
    if (EVP_Digest(reinterpret_cast<const unsigned char*>(clientKey.data()), clientKey.size(),
                   reinterpret_cast<unsigned char*>(storedKey.data()), nullptr, md, nullptr) == 0)
    {
        errorText = "Digest call failed";
        return false;
    }

    serverKey.resize(hashSize);
    // ServerKey := HMAC(SaltedPassword, "Server Key")
    if (HMAC(md, saltedPassword.data(), saltedPassword.size(),
	    reinterpret_cast<const unsigned char*>(SERVER_KEY_CONSTANT.data()), SERVER_KEY_CONSTANT.size(),
	    reinterpret_cast<unsigned char*>(serverKey.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    return true;
}

bool ComputeServerKey(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& serverKey, std::string& errorText)
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

    // SaltedPassword := Hi(Normalize(password), salt, i)
    std::string saltedPassword = Hi(md, prepPassword, salt, iterationsCount);

    size_t hashSize = EVP_MD_size(md);
    ui32 hashLen = 0;

    serverKey.resize(hashSize);
    // ServerKey := HMAC(SaltedPassword, "Server Key")
    if (HMAC(md, saltedPassword.data(), saltedPassword.size(),
	    reinterpret_cast<const unsigned char*>(SERVER_KEY_CONSTANT.data()), SERVER_KEY_CONSTANT.size(),
	    reinterpret_cast<unsigned char*>(serverKey.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    return true;
}

bool ComputeServerSignature(const std::string& hashType, const std::string& serverKey, const std::string& authMessage,
    std::string& serverSignature, std::string& errorText)
{
    std::string digestName = GetDigestNameFromHashType(hashType);
    const EVP_MD* md = EVP_get_digestbyname(digestName.c_str());
    if (md == nullptr) {
        errorText = "Unsupported hash type: " + hashType;
        return false;
    }

    size_t hashSize = EVP_MD_size(md);
    ui32 hashLen = 0;

    serverSignature.resize(hashSize);
    // ServerSignature := HMAC(ServerKey, AuthMessage)
    if (HMAC(md, serverKey.data(), serverKey.size(),
	    reinterpret_cast<const unsigned char*>(authMessage.data()), authMessage.size(),
	    reinterpret_cast<unsigned char*>(serverSignature.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    return true;
}

bool ComputeClientProof(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount, const std::string& authMessage,
    std::string& clientProof, std::string& errorText)
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

    // SaltedPassword := Hi(Normalize(password), salt, i)
    std::string saltedPassword = Hi(md, prepPassword, salt, iterationsCount);

    size_t hashSize = EVP_MD_size(md);
    std::string clientKey;
    clientKey.resize(hashSize);
    ui32 hashLen = 0;

    // ClientKey := HMAC(SaltedPassword, "Client Key")
    if (HMAC(md, saltedPassword.data(), saltedPassword.size(),
        reinterpret_cast<const unsigned char*>(CLIENT_KEY_CONSTANT.data()), CLIENT_KEY_CONSTANT.size(),
        reinterpret_cast<unsigned char*>(clientKey.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    std::string storedKey;
    storedKey.resize(hashSize);
    // StoredKey := H(ClientKey)
    if (EVP_Digest(reinterpret_cast<const unsigned char*>(clientKey.data()), clientKey.size(),
                   reinterpret_cast<unsigned char*>(storedKey.data()), nullptr, md, nullptr) == 0)
    {
        errorText = "Digest call failed";
        return false;
    }

    std::string clientSignature;
    clientSignature.resize(hashSize);
    // ClientSignature := HMAC(StoredKey, AuthMessage)
    if (HMAC(md, storedKey.data(), storedKey.size(),
        reinterpret_cast<const unsigned char*>(authMessage.data()), authMessage.size(),
        reinterpret_cast<unsigned char*>(clientSignature.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    clientProof.resize(hashSize);
    // ClientProof := ClientKey XOR ClientSignature
    for (size_t k = 0; k < hashSize; ++k) {
        clientProof[k] = clientKey[k] ^ clientSignature[k];
    }

    return true;
}

bool VerifyClientProof(const std::string& hashType, const std::string& clientProof, const std::string& storedKey,
    const std::string& authMessage, std::string& errorText)
{
    std::string digestName = GetDigestNameFromHashType(hashType);
    const EVP_MD* md = EVP_get_digestbyname(digestName.c_str());
    if (md == nullptr) {
        errorText = "Unsupported hash type: " + hashType;
        return false;
    }

    size_t hashSize = EVP_MD_size(md);
    ui32 hashLen = 0;

    if (clientProof.size() != hashSize) {
        return false;
    }

    std::string clientSignature;
    clientSignature.resize(hashSize);
    // ClientSignature := HMAC(StoredKey, AuthMessage)
    if (HMAC(md, storedKey.data(), storedKey.size(),
	    reinterpret_cast<const unsigned char*>(authMessage.data()), authMessage.size(),
	    reinterpret_cast<unsigned char*>(clientSignature.data()), &hashLen) == nullptr)
    {
        errorText = "HMAC call failed";
        return false;
    }

    std::string clientKey;
    clientKey.resize(hashSize);
    // ClientKey := ClientSignature XOR ClientProof
    for (size_t k = 0; k < hashSize; ++k) {
        clientKey[k] = clientSignature[k] ^ clientProof[k];
    }

    std::string computedStoredKey;
    computedStoredKey.resize(hashSize);
    // StoredKey := H(ClientKey)
    if (EVP_Digest(reinterpret_cast<const unsigned char*>(clientKey.data()), clientKey.size(),
                   reinterpret_cast<unsigned char*>(computedStoredKey.data()), nullptr, md, nullptr) == 0)
    {
        errorText = "Digest call failed";
        return false;
    }

    return computedStoredKey == storedKey;
}

EParseMsgReturnCodes ParseFirstClientMsg(const std::string& msg, TFirstClientMsg& parsedMsg) {
    if (msg.empty()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    const std::string_view msgView = msg;
    size_t i = 0;

    // read GS2 header
    if (auto rc = ParseGS2Header(msgView, i, parsedMsg.GS2Header); rc != EParseMsgReturnCodes::Success) {
        return rc;
    }

    size_t firstClientMsgBareStart = i;

    if (i == msgView.size()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read mext
    if (msgView[i] == 'm') {
        if (auto mextAttr = ReadAttribute(msgView, i); mextAttr.has_value()) {
            const std::string_view mext = mextAttr->second;
            if (!IsUtf(mext.data(), mext.size())) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                parsedMsg.Mext = mext;
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        if (i == msgView.size() || msgView[i++] != ',') {
            return EParseMsgReturnCodes::InvalidFormat;
        }
    }

    //read username
    if (auto usernameAttr = ReadAttribute(msgView, i); usernameAttr.has_value()) {
        if (usernameAttr->first != 'n') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view username = usernameAttr->second;
        std::string decodedUsername = DecodeSaslname(username);
        if (decodedUsername.empty() || !IsUtf(decodedUsername.data(), decodedUsername.size())) {
            return EParseMsgReturnCodes::InvalidUsernameEncoding;
        } else {
            parsedMsg.AuthcId = std::move(decodedUsername);
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    if (i == msgView.size() || msgView[i++] != ',') {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    //read nonce
    if (auto nonceAttr = ReadAttribute(msgView, i); nonceAttr.has_value()) {
        if (nonceAttr->first != 'r') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view nonce = nonceAttr->second;
        if (!IsPrintable(nonce)) {
            return EParseMsgReturnCodes::InvalidEncoding;
        } else {
            parsedMsg.Nonce = nonce;
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read extensions
    while (i != msgView.size()) {
        if (msgView[i++] != ',') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        if (auto attr = ReadAttribute(msgView, i); attr.has_value()) {
            if (RESERVED_ATTRIBUTE_NAMES.contains(attr->first)) {
                return EParseMsgReturnCodes::InvalidFormat;
            }

            const std::string_view attrValue = attr->second;
            if (!IsUtf(attrValue.data(), attrValue.size())) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                if (!parsedMsg.Extensions.emplace(attr->first, attrValue).second){
                    return EParseMsgReturnCodes::InvalidFormat;
                }
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }

    }

    parsedMsg.ClientFirstMessageBare = msgView.substr(firstClientMsgBareStart);
    return EParseMsgReturnCodes::Success;
}

EParseMsgReturnCodes ParseFinalClientMsg(const std::string& msg, TFinalClientMsg& parsedMsg) {
    if (msg.empty()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    const std::string_view msgView = msg;
    size_t i = 0;

    // read channel binding
    if (auto cbindAttr = ReadAttribute(msgView, i); cbindAttr.has_value()) {
        if (cbindAttr->first != 'c') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view channelBinding = cbindAttr->second;
        if (!IsBase64(channelBinding)) {
            return EParseMsgReturnCodes::InvalidEncoding;
        } else {
            const std::string cbindInput = Base64StrictDecode(channelBinding);
            const std::string_view cbindInputView = cbindInput;
            size_t j = 0;

            // read GS2 header
            if (auto rc = ParseGS2Header(cbindInputView, j, parsedMsg.GS2Header); rc != EParseMsgReturnCodes::Success) {
                return rc;
            }

            switch (parsedMsg.GS2Header.ChannelBindingFlag) {
            case EClientChannelBindingFlag::Required: {
                if (j == cbindInputView.size()) {
                    return EParseMsgReturnCodes::InvalidFormat;
                }

                parsedMsg.ChannelBindingData = cbindInputView.substr(j);
                break;
            }
            case EClientChannelBindingFlag::NotSupported:
            case EClientChannelBindingFlag::Supported:
                if (j != cbindInputView.size()) {
                    return EParseMsgReturnCodes::InvalidFormat;
                }
                break;
            }
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    if (i == msgView.size() || msgView[i++] != ',') {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    //read nonce
    if (auto nonceAttr = ReadAttribute(msgView, i); nonceAttr.has_value()) {
        if (nonceAttr->first != 'r') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view nonce = nonceAttr->second;
        if (!IsPrintable(nonce)) {
            return EParseMsgReturnCodes::InvalidEncoding;
        } else {
            parsedMsg.Nonce = nonce;
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    size_t clientFinalMsgWithoutProofLength = i;

    // read extensions and proof
    while (i != msgView.size() && parsedMsg.Proof.empty()) {
        if (msgView[i++] != ',') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        if (auto attr = ReadAttribute(msgView, i); attr.has_value()) {
            if (attr->first == 'p') { //client proof
                const std::string_view clientProof = attr->second;
                if (!IsBase64(clientProof)) {
                    return EParseMsgReturnCodes::InvalidEncoding;
                } else {
                    parsedMsg.Proof = clientProof;
                }
            } else { // extensions
                if (RESERVED_ATTRIBUTE_NAMES.contains(attr->first)) {
                    return EParseMsgReturnCodes::InvalidFormat;
                }

                const std::string_view attrValue = attr->second;
                if (!IsUtf(attrValue.data(), attrValue.size())) {
                    return EParseMsgReturnCodes::InvalidEncoding;
                } else {
                    if (!parsedMsg.Extensions.emplace(attr->first, attrValue).second){
                        return EParseMsgReturnCodes::InvalidFormat;
                    }

                    clientFinalMsgWithoutProofLength = i;
                }
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }
    }

    if (i != msgView.size() || parsedMsg.Proof.empty()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    parsedMsg.ClientFinalMessageWithoutProof = msgView.substr(0, clientFinalMsgWithoutProofLength);
    return EParseMsgReturnCodes::Success;
}

EParseMsgReturnCodes ParseFirstServerMsg(const std::string& msg, TFirstServerMsg& parsedMsg) {
    if (msg.empty()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    const std::string_view msgView = msg;
    size_t i = 0;

    // read nonce
    if (auto nonceAttr = ReadAttribute(msgView, i); nonceAttr.has_value()) {
        if (nonceAttr->first != 'r') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view nonce = nonceAttr->second;
        if (!IsPrintable(nonce)) {
            return EParseMsgReturnCodes::InvalidEncoding;
        } else {
            parsedMsg.Nonce = nonce;
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    if (i == msgView.size() || msgView[i++] != ',') {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read salt
    if (auto saltAttr = ReadAttribute(msgView, i); saltAttr.has_value()) {
        if (saltAttr->first != 's') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view salt = saltAttr->second;
        if (!IsBase64(salt)) {
            return EParseMsgReturnCodes::InvalidEncoding;
        } else {
            parsedMsg.Salt = salt;
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    if (i == msgView.size() || msgView[i++] != ',') {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read iterations count
    if (auto iterAttr = ReadAttribute(msgView, i); iterAttr.has_value()) {
        if (iterAttr->first != 'i') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        const std::string_view iterStr = iterAttr->second;
        if (!IsPositiveInt(iterStr)) {
            return EParseMsgReturnCodes::InvalidFormat;
        } else {
            parsedMsg.IterationsCount = std::stoul(std::string(iterStr));
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read extensions
    while (i != msgView.size()) {
        if (msgView[i++] != ',') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        if (auto attr = ReadAttribute(msgView, i); attr.has_value()) {
            if (RESERVED_ATTRIBUTE_NAMES.contains(attr->first)) {
                return EParseMsgReturnCodes::InvalidFormat;
            }

            const std::string_view attrValue = attr->second;
            if (!IsUtf(attrValue.data(), attrValue.size())) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                if (!parsedMsg.Extensions.emplace(attr->first, attrValue).second){
                    return EParseMsgReturnCodes::InvalidFormat;
                }
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }
    }

    return EParseMsgReturnCodes::Success;
}

EParseMsgReturnCodes ParseFinalServerMsg(const std::string& msg, TFinalServerMsg& parsedMsg) {
    if (msg.empty()) {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    const std::string_view msgView = msg;
    size_t i = 0;

    // read first attribute (either 'v' for server signature or 'e' for error)
    if (auto firstAttr = ReadAttribute(msgView, i); firstAttr.has_value()) {
        if (firstAttr->first == 'v') {
            // server signature
            const std::string_view serverSignature = firstAttr->second;
            if (!IsBase64(serverSignature)) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                parsedMsg.ServerSignature = serverSignature;
            }
        } else if (firstAttr->first == 'e') {
            // error
            const std::string_view error = firstAttr->second;
            if (!IsPrintable(error)) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                parsedMsg.Error = error;
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }
    } else {
        return EParseMsgReturnCodes::InvalidFormat;
    }

    // read extensions
    while (i != msgView.size()) {
        if (msgView[i++] != ',') {
            return EParseMsgReturnCodes::InvalidFormat;
        }

        if (auto attr = ReadAttribute(msgView, i); attr.has_value()) {
            if (RESERVED_ATTRIBUTE_NAMES.contains(attr->first)) {
                return EParseMsgReturnCodes::InvalidFormat;
            }

            const std::string_view attrValue = attr->second;
            if (!IsUtf(attrValue.data(), attrValue.size())) {
                return EParseMsgReturnCodes::InvalidEncoding;
            } else {
                if (!parsedMsg.Extensions.emplace(attr->first, attrValue).second){
                    return EParseMsgReturnCodes::InvalidFormat;
                }
            }
        } else {
            return EParseMsgReturnCodes::InvalidFormat;
        }
    }

    return EParseMsgReturnCodes::Success;
}

std::string BuildFirstServerMsg(const std::string& nonce, const std::string& salt, const std::string& iterationsCount,
    const std::unordered_map<char, std::string>& extensions)
{
    std::stringstream msg;

    msg << "r=" << nonce << ",";
    msg << "s=" << salt << ",";
    msg << "i=" << iterationsCount;

    for (const auto& [attrName, attrValue] : extensions) {
        msg << "," << attrName << "=" << attrValue;
    }

    return msg.str();
}

std::string BuildFinalServerMsg(const std::string& serverSignature,
    const std::unordered_map<char, std::string>& extensions)
{
    std::stringstream msg;

    msg << "v=" << serverSignature;

    for (const auto& [attrName, attrValue] : extensions) {
        msg << "," << attrName << "=" << attrValue;
    }

    return msg.str();
}

} // namespace NLogin::NSasl
