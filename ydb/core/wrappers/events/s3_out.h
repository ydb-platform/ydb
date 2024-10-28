#pragma once

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/AbortMultipartUploadRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/CreateMultipartUploadRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/CompleteMultipartUploadRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/GetObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/PutObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/UploadPartCopyRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/UploadPartRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Client.h>

#include <util/stream/output.h>

namespace NKikimr::NWrappers {

void Out(IOutputStream& out, const Aws::S3::Model::GetObjectRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::GetObjectResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::GetObjectOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::ListObjectsRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::ListObjectsResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::ListObjectsOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::HeadObjectRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::HeadObjectResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::HeadObjectOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::PutObjectRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::PutObjectResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::PutObjectOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::DeleteObjectRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::DeleteObjectResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::DeleteObjectOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::DeleteObjectsRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::DeleteObjectsResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::DeleteObjectsOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::CreateMultipartUploadRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::CreateMultipartUploadResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::CreateMultipartUploadOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::CompleteMultipartUploadRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::CompleteMultipartUploadResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::CompleteMultipartUploadOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::AbortMultipartUploadRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::AbortMultipartUploadResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::AbortMultipartUploadOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::UploadPartRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::UploadPartResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::UploadPartOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::UploadPartCopyRequest& request);
void Out(IOutputStream& out, const Aws::S3::Model::UploadPartCopyResult& result);
void Out(IOutputStream& out, const Aws::S3::Model::UploadPartCopyOutcome& outcome);

void Out(IOutputStream& out, const Aws::S3::Model::CompletedMultipartUpload& upload);
void Out(IOutputStream& out, const Aws::S3::Model::CompletedPart& part);

using TStringOutcome = Aws::Utils::Outcome<Aws::String, Aws::S3::S3Error>;
void Out(IOutputStream& out, const TStringOutcome& outcome);

} // NKikimr::NWrappers

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::GetObjectRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::GetObjectResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::GetObjectOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::ListObjectsRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::ListObjectsResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::ListObjectsOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::DeleteObjectsRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::DeleteObjectsResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::DeleteObjectsOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::HeadObjectRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::HeadObjectResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::HeadObjectOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::PutObjectRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::PutObjectResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::PutObjectOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::DeleteObjectRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::DeleteObjectResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::DeleteObjectOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CreateMultipartUploadRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CreateMultipartUploadResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CreateMultipartUploadOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CompleteMultipartUploadRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CompleteMultipartUploadResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CompleteMultipartUploadOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::AbortMultipartUploadRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::AbortMultipartUploadResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::AbortMultipartUploadOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::UploadPartRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::UploadPartResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::UploadPartOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::UploadPartCopyRequest, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::UploadPartCopyResult, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::UploadPartCopyOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CompletedMultipartUpload, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, Aws::S3::Model::CompletedPart, out, value) {
    NKikimr::NWrappers::Out(out, value);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NWrappers::TStringOutcome, out, value) {
    NKikimr::NWrappers::Out(out, value);
}
