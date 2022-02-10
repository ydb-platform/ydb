#pragma once 
 
#include <library/cpp/tvmauth/exception.h> 
 
namespace NTvmAuth { 
    class TClientException: public TTvmException { 
    }; 
 
    class TRetriableException: public TClientException { 
    }; 
    class TNonRetriableException: public TClientException { 
    }; 
 
    class TIllegalUsage: public TNonRetriableException { 
    }; 
 
    class TBrokenTvmClientSettings: public TIllegalUsage { 
    }; 
    class TMissingServiceTicket: public TNonRetriableException { 
    }; 
    class TPermissionDenied: public TNonRetriableException { 
    }; 
} 
