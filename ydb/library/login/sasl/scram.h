#pragma once

#include <string>
#include <unordered_map>

#include <util/system/types.h>

namespace NLogin::NSasl {

enum class EClientChannelBindingFlag {
    NotSupported,
    Supported, // but thinks that the server does not
    Required,
};

struct GS2Header {
    EClientChannelBindingFlag ChannelBindingFlag;
    std::string RequestedChannelBinding;
    std::string AuthzId;
};

struct TFirstClientMsg {
    GS2Header GS2Header;
    std::string Mext;
    std::string AuthcId;
    std::string Nonce;
    std::unordered_map<char, std::string> Extensions;
    std::string ClientFirstMessageBare;
};

struct TFinalClientMsg {
    GS2Header GS2Header;
    std::string ChannelBindingData;
    std::string Nonce;
    std::string Proof;
    std::unordered_map<char, std::string> Extensions;
    std::string ClientFinalMessageWithoutProof;
};

struct TFirstServerMsg {
    std::string Nonce;
    std::string Salt;
    ui32 IterationsCount;
    std::unordered_map<char, std::string> Extensions;
};

struct TFinalServerMsg {
    std::string ServerSignature;
    std::string Error;
    std::unordered_map<char, std::string> Extensions;
};

enum class EParseMsgReturnCodes {
    Success,
    InvalidEncoding,
    InvalidUsernameEncoding,
    InvalidFormat,
};

enum class EScramServerError {
    InvalidEncoding,
    ExtensionsNoSupported,
    InvalidProof,
    ChannelBindingsDontMatch,
    ServerDoesSupportChannelBinding,
    ChannelBindingNotSupported,
    UnsupportedChannelBindingType,
    UnknownUser,
    InvalidUsernameEncoding,
    NoResources,
    OtherError,
};

bool GenerateScramSecrets(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& storedKey, std::string& serverKey, std::string& errorText);

bool ComputeServerKey(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& serverKey, std::string& errorText);

bool ComputeServerSignature(const std::string& hashType, const std::string& serverKey, const std::string& authMessage,
    std::string& serverSignature, std::string& errorText);

bool ComputeClientProof(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount, const std::string& authMessage,
    std::string& clientProof, std::string& errorText);

bool VerifyClientProof(const std::string& hashType, const std::string& clientProof, const std::string& storedKey,
    const std::string& authMessage, std::string& errorText);

EParseMsgReturnCodes ParseFirstClientMsg(const std::string& msg, TFirstClientMsg& parsedMsg);
EParseMsgReturnCodes ParseFinalClientMsg(const std::string& msg, TFinalClientMsg& parsedMsg);
EParseMsgReturnCodes ParseFirstServerMsg(const std::string& msg, TFirstServerMsg& parsedMsg);
EParseMsgReturnCodes ParseFinalServerMsg(const std::string& msg, TFinalServerMsg& parsedMsg);

std::string BuildFirstServerMsg(const std::string& nonce, const std::string& salt, const std::string& iterationsCount,
    const std::unordered_map<char, std::string>& extensions = {});

std::string BuildFinalServerMsg(const std::string& serverSignature,
    const std::unordered_map<char, std::string>& extensions = {});

constexpr std::string_view BuildErrorMsg(EScramServerError errorCode) {
    switch (errorCode) {
    case EScramServerError::InvalidEncoding:
        return "e=invalid-encoding";
    case EScramServerError::ExtensionsNoSupported:
        return "e=extensions-not-supported";
    case EScramServerError::InvalidProof:
        return "e=invalid-proof";
    case EScramServerError::ChannelBindingsDontMatch:
        return "e=channel-bindings-dont-match";
    case EScramServerError::ServerDoesSupportChannelBinding:
        return "e=server-does-support-channel-binding";
    case EScramServerError::ChannelBindingNotSupported:
        return "e=channel-binding-not-supported";
    case EScramServerError::UnsupportedChannelBindingType:
        return "e=unsupported-channel-binding-type";
    case EScramServerError::UnknownUser:
        return "e=unknown-user";
    case EScramServerError::InvalidUsernameEncoding:
        return "e=invalid-username-encoding";
    case EScramServerError::NoResources:
        return "e=no-resources";
    case EScramServerError::OtherError:
        return "e=other-error";
    default:
        return "e=other-error";
    }
}

} // namespace NLogin::NSasl
