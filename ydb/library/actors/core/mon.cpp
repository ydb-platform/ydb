#include "mon.h"

#include <ydb/library/actors/protos/actors.pb.h>

namespace NActors::NMon {

    TEvRemoteHttpInfo::TEvRemoteHttpInfo()
    {}

    TEvRemoteHttpInfo::TEvRemoteHttpInfo(const TString& query, HTTP_METHOD method)
        : Query(query)
        , Method(method)
    {}

    TEvRemoteHttpInfo::TEvRemoteHttpInfo(NActorsProto::TRemoteHttpInfo info)
        : Query(MakeSerializedQuery(info))
        , ExtendedQuery(std::make_unique<NActorsProto::TRemoteHttpInfo>(info))
    {}

    TEvRemoteHttpInfo::~TEvRemoteHttpInfo()
    {}

    TString TEvRemoteHttpInfo::MakeSerializedQuery(const NActorsProto::TRemoteHttpInfo& info) {
        TString s(1, '\0');
        const bool success = info.AppendToString(&s);
        Y_ABORT_UNLESS(success);
        return s;
    }

    TString TEvRemoteHttpInfo::PathInfo() const {
        if (ExtendedQuery) {
            return ExtendedQuery->GetPath();
        } else {
            const size_t pos = Query.find('?');
            return (pos == TString::npos) ? TString() : Query.substr(0, pos);
        }
    }

    TCgiParameters TEvRemoteHttpInfo::Cgi() const {
        if (ExtendedQuery) {
            TCgiParameters params;
            for (const auto& kv : ExtendedQuery->GetQueryParams()) {
                params.emplace(kv.GetKey(), kv.GetValue());
            }
            return params;
        } else {
            const size_t pos = Query.find('?');
            return TCgiParameters((pos == TString::npos) ? TString() : Query.substr(pos + 1));
        }
    }

    HTTP_METHOD TEvRemoteHttpInfo::GetMethod() const {
        return ExtendedQuery ? static_cast<HTTP_METHOD>(ExtendedQuery->GetMethod()) : Method;
    }

    IEventBase* TEvRemoteHttpInfo::Load(TEventSerializedData* bufs) {
        TString s = bufs->GetString();
        if (s.size() && s[0] == '\0') {
            TRope::TConstIterator iter = bufs->GetBeginIter();
            ui64 size = bufs->GetSize();
            iter += 1, --size; // skip '\0'
            TRopeStream stream(iter, size);

            auto res = std::make_unique<TEvRemoteHttpInfo>();
            res->Query = s;
            res->ExtendedQuery = std::make_unique<NActorsProto::TRemoteHttpInfo>();
            const bool success = res->ExtendedQuery->ParseFromZeroCopyStream(&stream);
            Y_ABORT_UNLESS(success);
            return res.release();
        } else {
            return new TEvRemoteHttpInfo(s);
        }
    }

} // NActors::NMon
