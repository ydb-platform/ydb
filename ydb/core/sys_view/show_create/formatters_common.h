#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>

namespace NKikimr::NSysView {

void EscapeName(const TString& str, TStringStream& stream);
void EscapeString(const TString& str, TStringStream& stream);
void EscapeBinary(const TString& str, TStringStream& stream);

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
