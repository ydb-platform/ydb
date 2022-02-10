#pragma once 
 
#include <util/generic/strbuf.h> 
 
namespace NTvmAuth::NUtils { 
    /*! 
     * Remove signature from ticket string - rest part can be parsed later with `tvmknife parse_ticket ...` 
     * @param ticketBody Raw ticket body 
     * @return safe for logging part of ticket 
     */ 
    TStringBuf RemoveTicketSignature(TStringBuf ticketBody); 
} 
