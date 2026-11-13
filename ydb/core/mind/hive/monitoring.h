#pragma once

#include "hive_log.h"

#include <library/cpp/json/json_writer.h>
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/mon.h>

namespace NKikimr {
namespace NHive {

class TLoggedMonTransaction : public TLoggedTransaction {
protected:
    TLoggedMonTransaction(const NMon::TEvRemoteHttpInfo::TPtr& evi, THive* self);
};

TCgiParameters GetParams(const NMon::TEvRemoteHttpInfo* ev);

// user SID from the request's auth token; empty when the request carries no token
TString GetMonUser(const NMon::TEvRemoteHttpInfo* ev);

// the same with "anonymous" instead of an empty SID: for log lines and event history
TString GetMonUserForLog(const NMon::TEvRemoteHttpInfo* ev);

struct THttpStatus {
    static const inline TString OK = "200 Ok";
    static const inline TString BAD_REQUEST = "400 Bad Request";
};

NMon::TEvRemoteBinaryInfoRes* MakeRawHttpEvent(const TString& status, const TString& content);

}
}
