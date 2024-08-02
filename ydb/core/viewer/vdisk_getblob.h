#pragma once
#include "json_handlers.h"
#include "json_vdisk_req.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr::NViewer {

using TJsonGetBlob = TJsonVDiskRequest<TEvGetLogoBlobRequest, TEvGetLogoBlobResponse>;

template <>
struct TJsonVDiskRequestHelper<TEvGetLogoBlobRequest, TEvGetLogoBlobResponse>  {
    static std::unique_ptr<TEvGetLogoBlobRequest> MakeRequest(NMon::TEvHttpInfo::TPtr &ev, TString *error) {
        const TCgiParameters& cgi = ev->Get()->Request.GetParams();

        bool internals = cgi.Has("internals");
        TString from = cgi.Get("from");
        TString to = cgi.Get("to");

        auto assign_blob_id = [] (NKikimrVDisk::LogoBlobId *id, const TLogoBlobID &blobId) {
            const ui64 *raw = blobId.GetRaw();
            id->set_raw_x1(raw[0]);
            id->set_raw_x2(raw[1]);
            id->set_raw_x3(raw[2]);
        };

        TString errorExplanation;
        auto try_to_parse = [&] (const TString &field, const TString &param, NKikimrVDisk::LogoBlobId *id) {
            TLogoBlobID blobId;
            bool good = TLogoBlobID::Parse(blobId, param, errorExplanation);
            if (!good) {
                *error = "Failed to parse '" + field + "' field: " + errorExplanation;
                return true;
            }
            assign_blob_id(id, blobId);
            return false;
        };


        auto req = std::make_unique<TEvGetLogoBlobRequest>();
        req->Record.set_show_internals(internals);

        NKikimrVDisk::LogoBlobIdRange *range = req->Record.mutable_range();
        if (from) {
            try_to_parse("from", from, range->mutable_from());
        } else {
            assign_blob_id(range->mutable_from(), Min<TLogoBlobID>());
        }
        if (to) {
            try_to_parse("to", to, range->mutable_to());
        } else {
            assign_blob_id(range->mutable_to(), Max<TLogoBlobID>());
        }

        return req;
    }

    static TString GetAdditionalParameters() {
        return R"___(
            - name: from
              in: query
              description: blob identifier, inclusive lower bound for getting range, default is minimal blob id
              type: string
            - name: to
              in: query
              description: blob identifier, inclusive upper bound for getting range, default is maximal blob id
              required: false
              type: string
            - name: internals
              in: query
              description: return ingress of each blob
              required: false
              type: boolean
        )___";
    }
};

}
