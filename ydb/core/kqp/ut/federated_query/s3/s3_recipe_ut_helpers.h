#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/env.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>

namespace NTestUtils {

    constexpr TStringBuf TEST_CONTENT =
        R"({"key": "1", "value": "trololo"}
           {"key": "2", "value": "hello world"})"sv;

    constexpr TStringBuf TEST_CONTENT_KEYS =
        R"({"key": "1"}
           {"key": "3"})"sv;

    extern const TString TEST_SCHEMA;
    extern const TString TEST_SCHEMA_IDS;

    std::shared_ptr<NKikimr::NKqp::TKikimrRunner> MakeKikimrRunner(std::optional<NKikimrConfig::TAppConfig> appConfig = std::nullopt, const TString& domainRoot = "Root");

    Aws::S3::S3Client MakeS3Client();

    void CreateBucket(const TString& bucket, Aws::S3::S3Client& s3Client);
    void CreateBucket(const TString& bucket);

    void UploadObject(const TString& bucket, const TString& object, const TStringBuf& content, Aws::S3::S3Client& s3Client);
    void UploadObject(const TString& bucket, const TString& object, const TStringBuf& content);

    void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content, Aws::S3::S3Client& s3Client);
    void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content);

    TString GetObject(const TString& bucket, const TString& object, Aws::S3::S3Client& s3Client);
    TString GetObject(const TString& bucket, const TString& object);

    std::vector<TString> GetObjectKeys(const TString& bucket, Aws::S3::S3Client& s3Client);
    std::vector<TString> GetObjectKeys(const TString& bucket);

    TString GetAllObjects(const TString& bucket, TStringBuf separator, Aws::S3::S3Client& s3Client);
    TString GetAllObjects(const TString& bucket, TStringBuf separator = {});

    TString GetBucketLocation(const TStringBuf bucket);

} // namespace NTestUtils
