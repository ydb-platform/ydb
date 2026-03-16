#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/mvp/core/mvp_test_runtime.h>

namespace NMVP::NMeta::NUT {

class TGrafanaMockServiceActor : public NActors::TActor<TGrafanaMockServiceActor> {
public:
    using TBase = NActors::TActor<TGrafanaMockServiceActor>;

    TGrafanaMockServiceActor()
        : TBase(&TGrafanaMockServiceActor::StateWork)
    {}

    static TStringBuf DashboardsYdbCommonJson() {
        return R"json([
    {
        "id": 176,
        "uid": "ydb_cpu",
        "orgId": 1,
        "title": "CPU",
        "uri": "db/cpu",
        "url": "/d/ydb_cpu/cpu",
        "slug": "",
        "type": "dash-db",
        "tags": [
            "ydb-common"
        ],
        "isStarred": false,
        "folderId": 158,
        "folderUid": "d22318e2-bfdb-45f6-b238-4562085ba79c",
        "folderTitle": "YDB",
        "folderUrl": "/dashboards/f/d22318e2-bfdb-45f6-b238-4562085ba79c/ydb",
        "sortMeta": 0,
        "isDeleted": false
    },
    {
        "id": 312,
        "uid": "ydb_dboverview",
        "orgId": 1,
        "title": "DB overview",
        "uri": "db/db-overview",
        "url": "/d/ydb_dboverview/db-overview",
        "slug": "",
        "type": "dash-db",
        "tags": [
            "ydb-common"
        ],
        "isStarred": false,
        "folderId": 158,
        "folderUid": "d22318e2-bfdb-45f6-b238-4562085ba79c",
        "folderTitle": "YDB",
        "folderUrl": "/dashboards/f/d22318e2-bfdb-45f6-b238-4562085ba79c/ydb",
        "sortMeta": 0,
        "isDeleted": false
    },
    {
        "id": 5648,
        "uid": "bevkmtk3xlclca",
        "orgId": 1,
        "title": "Interconnect",
        "uri": "db/interconnect",
        "url": "/d/bevkmtk3xlclca/interconnect",
        "slug": "",
        "type": "dash-db",
        "tags": [
            "ydb-common"
        ],
        "isStarred": false,
        "folderId": 158,
        "folderUid": "d22318e2-bfdb-45f6-b238-4562085ba79c",
        "folderTitle": "YDB",
        "folderUrl": "/dashboards/f/d22318e2-bfdb-45f6-b238-4562085ba79c/ydb",
        "sortMeta": 0,
        "isDeleted": false
    }
])json";
    }

    static TString ExtractQueryParam(TStringBuf url, TStringBuf name) {
        const size_t queryPos = url.find('?');
        if (queryPos == TStringBuf::npos) {
            return {};
        }

        TStringBuf query = url.SubStr(queryPos + 1);
        size_t pos = 0;
        while (pos <= query.size()) {
            size_t ampPos = query.find('&', pos);
            TStringBuf part = ampPos == TStringBuf::npos
                ? query.SubStr(pos)
                : query.SubStr(pos, ampPos - pos);
            const size_t eqPos = part.find('=');
            if (eqPos != TStringBuf::npos) {
                TStringBuf key = part.SubStr(0, eqPos);
                TStringBuf value = part.SubStr(eqPos + 1);
                if (key == name) {
                    return TString(value);
                }
            }
            if (ampPos == TStringBuf::npos) {
                break;
            }
            pos = ampPos + 1;
        }
        return {};
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        const TString tag = ExtractQueryParam(request->URL, "tag");

        const TString responseBody = (tag == "ydb-common") ? TString(DashboardsYdbCommonJson()) : "[]";

        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatWholeString(
            response,
            TStringBuilder()
                << "HTTP/1.1 200 OK\r\n"
                << "Connection: close\r\n"
                << "Content-Type: application/json; charset=utf-8\r\n"
                << "Content-Length: " << responseBody.size() << "\r\n"
                << "\r\n"
                << responseBody
        );

        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }
};

} // namespace NMVP::NMeta::NUT
