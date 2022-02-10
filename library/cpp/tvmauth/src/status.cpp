#include <library/cpp/tvmauth/ticket_status.h> 

#include <util/generic/yexception.h> 
 
namespace NTvmAuth { 
    TStringBuf StatusToString(ETicketStatus st) { 
        switch (st) {
            case ETicketStatus::Ok: 
                return "OK";
            case ETicketStatus::Expired: 
                return "Expired ticket";
            case ETicketStatus::InvalidBlackboxEnv: 
                return "Invalid BlackBox environment";
            case ETicketStatus::InvalidDst: 
                return "Invalid ticket destination";
            case ETicketStatus::InvalidTicketType: 
                return "Invalid ticket type";
            case ETicketStatus::Malformed: 
                return "Malformed ticket";
            case ETicketStatus::MissingKey: 
                return "Context does not have required key to check ticket: public keys are too old"; 
            case ETicketStatus::SignBroken: 
                return "Invalid ticket signature";
            case ETicketStatus::UnsupportedVersion: 
                return "Unsupported ticket version";
            case ETicketStatus::NoRoles: 
                return "Subject (src or defaultUid) does not have any roles in IDM"; 
        }

        ythrow yexception() << "Unexpected status: " << static_cast<int>(st); 
    }
} 
