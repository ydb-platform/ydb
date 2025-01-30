#pragma once

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
