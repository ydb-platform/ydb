#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NSysView {

void EscapeName(const TString& str, TStringStream& stream);
void EscapeString(const TString& str, TStringStream& stream);
void EscapeBinary(const TString& str, TStringStream& stream);
void EscapeValue(bool value, TStringStream& stream);

template<class T>
void EscapeValue(const T& value, TStringStream& stream) {
    EscapeName(ToString<T>(value), stream);
}

class TFormatFail : public yexception {
public:
    Ydb::StatusIds::StatusCode Status;
    TString Error;

    TFormatFail(Ydb::StatusIds::StatusCode status, TString error = {})
        : Status(status)
        , Error(std::move(error))
    {}
};

class TFormatResult {
public:
    TFormatResult(TString out)
        : Out(std::move(out))
        , Status(Ydb::StatusIds::SUCCESS)
    {}

    TFormatResult(Ydb::StatusIds::StatusCode status, TString error)
        : Status(status)
        , Error(std::move(error))
    {}

    TFormatResult(Ydb::StatusIds::StatusCode status, NYql::TIssues& issues)
        : TFormatResult(status, issues.ToString())
    {}

    bool IsSuccess() const {
        return Status == Ydb::StatusIds::SUCCESS;
    }

    Ydb::StatusIds::StatusCode GetStatus() const {
        return Status;
    }

    const TString& GetError() const {
        return Error;
    }

    TString ExtractOut() {
        return std::move(Out);
    }

private:
    TString Out;

    Ydb::StatusIds::StatusCode Status;
    TString Error;
};

}
