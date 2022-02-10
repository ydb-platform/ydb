#pragma once 
 
#include <util/generic/strbuf.h> 
 
namespace NTvmAuth {
    /*!
     * Status mean result of ticket check
     */
    enum class ETicketStatus {
        Ok, 
        Expired, 
        InvalidBlackboxEnv, 
        InvalidDst, 
        InvalidTicketType, 
        Malformed, 
        MissingKey, 
        SignBroken, 
        UnsupportedVersion, 
        NoRoles,
    }; 
 
    TStringBuf StatusToString(ETicketStatus st);
} 
