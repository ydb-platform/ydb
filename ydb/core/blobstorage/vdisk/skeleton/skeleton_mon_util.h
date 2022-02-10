#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
    namespace NMonUtil {

        enum class EParseRes {
            OK = 0,
            Empty = 1,
            Error = 3,
        };

        template <class TValue>
        struct TParseResult {
            EParseRes Status;   // parse status
            TString StrVal;     // contains extracted string value
            TValue Value;       // parsed value
        };

        static inline TParseResult<NKikimrBlobStorage::EDbStatType> ParseDbName(const TString &dbname) {
            if (dbname == "LogoBlobs") {
                return {EParseRes::OK, dbname, NKikimrBlobStorage::StatLogoBlobs};
            } else if (dbname == "Blocks") {
                return {EParseRes::OK, dbname, NKikimrBlobStorage::StatBlocks};
            } else if (dbname == "Barriers") {
                return {EParseRes::OK, dbname, NKikimrBlobStorage::StatBarriers};
            } else {
                return {EParseRes::Error, dbname, NKikimrBlobStorage::StatLogoBlobs};
            }
        }


        static inline IEventBase *PrepareError(const TString &explanation) {
            TStringStream str;
            HTML(str) {
                DIV_CLASS("alert alert-error") {str << "ERROR: " << explanation;}
            }
            return new NMon::TEvHttpInfoRes(str.Str());
        }

    } // NMonUtil
} // NKikimr

