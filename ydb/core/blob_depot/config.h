#pragma once

#include <ydb/core/protos/s3_settings.pb.h>

#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/string/split.h>

#include <optional>

namespace NKikimr::NBlobDepot {

    inline TString MakeS3BasePath(const NKikimrSchemeOp::TS3Settings& settings, TStringBuf name) {
        // The SDK drops empty segments in point requests. LIST and batch DELETE use literal keys.
        const TString rawPath = TStringBuilder() << settings.GetObjectKeyPattern() << '/' << name;
        TString path;
        path.reserve(rawPath.size());
        for (TStringBuf segment : StringSplitter(rawPath).Split('/').SkipEmpty()) {
            if (path) {
                path += '/';
            }
            path += segment;
        }
        return path;
    }

    inline std::optional<TStringBuf> ValidateS3BasePath(TStringBuf basePath) {
        if (basePath.empty()) {
            return "The effective S3 key prefix must be nonempty";
        }
        for (TStringBuf segment : StringSplitter(basePath).Split('/')) {
            if (segment == "." || segment == "..") {
                return "The effective S3 key prefix must not contain dot segments";
            }
        }
        return std::nullopt;
    }

} // NKikimr::NBlobDepot
