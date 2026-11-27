#pragma once

#include "s3_error.h"

#include <ydb/core/wrappers/abstract.h>
#include <ydb/library/aws_init/cancellable_request.h>

#include <memory>
#include <optional>

namespace NKikimr::NBlobDepot {

    class TS3Listing {
        struct TLifetime {};
        const std::shared_ptr<TLifetime> Lifetime = std::make_shared<TLifetime>();
        static constexpr int MaxKeysPerPage = 1000; // amortize LISTs over large depots
        const TString Prefix;
        TString Marker;

    public:
        explicit TS3Listing(const TString& basePath, TString marker = {})
            : Prefix(basePath.empty() ? TString() : basePath + "/")
            , Marker(std::move(marker))
        {}

        const TString& GetPrefix() const { return Prefix; }
        const TString& GetMarker() const { return Marker; }

        Aws::S3::Model::ListObjectsRequest MakeRequest(const TString& bucket) const {
            auto request = Aws::S3::Model::ListObjectsRequest().WithBucket(bucket).WithPrefix(Prefix).WithMaxKeys(MaxKeysPerPage);
            // An abandoned page may still be in the SDK queue or waiting for a response on an idle connection.
            request.SetContinueRequestHandler(TCancellableAwsRequest{[lifetime = std::weak_ptr(Lifetime)](const auto*) {
                return !lifetime.expired();
            }});
            if (Marker) {
                request.SetMarker(Marker);
            }
            return request;
        }

        template<typename TCallback>
        std::optional<TString> ReadPage(const Aws::S3::Model::ListObjectsResult& result, TCallback&& visit) {
            if (result.GetIsTruncated() && result.GetContents().empty()) {
                return "truncated S3 listing has no marker";
            }
            for (const auto& item : result.GetContents()) {
                if (!item.KeyHasBeenSet() || !item.SizeHasBeenSet() || item.GetSize() < 0) {
                    return "S3 listing contains an invalid key or size";
                }
                const TString key(item.GetKey().data(), item.GetKey().size());
                if (!key.StartsWith(Prefix) || key <= Marker) {
                    return "S3 listing returned a key outside its prefix or marker order";
                }
                visit(key, TStringBuf(key).SubStr(Prefix.size()), static_cast<ui64>(item.GetSize()));
                Marker = key;
            }
            return std::nullopt;
        }
    };

    enum class ES3DeleteStatus { Deleted, Missing, SlowDown, Failed };

    // The caller tracks omitted and duplicate entries against the submitted keys.
    template<typename TCallback>
    void VisitS3DeleteResults(const Aws::S3::Model::DeleteObjectsResult& result, TCallback&& visit) {
        for (const auto& item : result.GetDeleted()) {
            if (item.KeyHasBeenSet()) {
                visit(TString(item.GetKey().data(), item.GetKey().size()), ES3DeleteStatus::Deleted, TStringBuf(), TStringBuf());
            }
        }
        for (const auto& item : result.GetErrors()) {
            if (item.KeyHasBeenSet()) {
                const auto status = IsS3MissingKey(item.GetCode()) ? ES3DeleteStatus::Missing
                    : IsS3SlowDownCode(item.GetCode()) ? ES3DeleteStatus::SlowDown : ES3DeleteStatus::Failed;
                visit(TString(item.GetKey().data(), item.GetKey().size()), status, TStringBuf(item.GetCode().data(), item.GetCode().size()),
                    TStringBuf(item.GetMessage().data(), item.GetMessage().size()));
            }
        }
    }
}
