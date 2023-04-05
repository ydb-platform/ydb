#pragma once

#include "json_proxy.h"

namespace NKikimr::NCms {

class TJsonProxySentinel : public TJsonProxyCms<TEvCms::TEvGetSentinelStateRequest, TEvCms::TEvGetSentinelStateResponse> {
public:
    TJsonProxySentinel(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyCms<TEvCms::TEvGetSentinelStateRequest, TEvCms::TEvGetSentinelStateResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override {
        TAutoPtr<TRequest> request = new TRequest;
        const TCgiParameters& cgi = RequestEvent->Get()->Request.GetParams();

        if (cgi.Has("show")) {
            NKikimrCms::TGetSentinelStateRequest::EShow show;
            NKikimrCms::TGetSentinelStateRequest::EShow_Parse(cgi.Get("show"), &show);
            request->Record.SetShow(show);
        }

        if (cgi.Has("range")) {
            TVector<std::pair<ui32, ui32>> ranges;
            auto rangesStr = cgi.Get("range");
            TVector<TString> strRanges;
            StringSplitter(rangesStr).Split(',').Collect(&strRanges);
            for (auto& strRange : strRanges) {
                ui32 begin = 0;
                ui32 end = 0;
                if (!StringSplitter(strRange).Split('-').TryCollectInto(&begin, &end)) {
                    if (TryFromString<ui32>(strRange, begin)) {
                        end = begin;
                    } else {
                        break; // TODO
                    }
                }
                ranges.push_back({begin, end});
            }
            sort(ranges.begin(), ranges.end());
            auto it = ranges.begin();
            auto current = *(it)++;
            while (it != ranges.end()) {
                if (current.second > it->first){
                    current.second = std::max(current.second, it->second);
                } else {
                    auto* newRange = request->Record.AddRanges();
                    newRange->SetBegin(current.first);
                    newRange->SetEnd(current.second);
                    current = *(it);
                }
                it++;
            }
            auto* newRange = request->Record.AddRanges();
            newRange->SetBegin(current.first);
            newRange->SetEnd(current.second);
        }

        return request;
    }
};

} // namespace NKikimr::NCms
