#pragma once

#include <ydb/services/datastreams/datastreams_codes.h>

#include <library/cpp/http/misc/httpcodes.h>


using TException = std::pair<TString, HttpCodes>;


namespace NKikimr::NHttpProxy {

    TException AlreadyExistsExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException BadRequestExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException GenericErrorExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException InternalErrorExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException NotFoundExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException OverloadedExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException PreconditionFailedExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException SchemeErrorExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException UnauthorizedExceptions(const TString& method, NYds::EErrorCodes issueCode);
    TException UnsupportedExceptions(const TString& method, NYds::EErrorCodes issueCode);

}