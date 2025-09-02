#if defined(KIKIMR_DISABLE_S3_WRAPPER)
#error "s3 wrapper is disabled"
#endif

#include "s3_out.h"

#include <util/generic/vector.h>
#include <util/string/join.h>

namespace NKikimr::NWrappers {

using namespace Aws::S3::Model;

#define DEFINE_PRINTER(field) \
    template <typename T> \
    void field(IOutputStream& out, const T& obj) { \
        out << #field ": " << obj.Get##field(); \
    }

DEFINE_PRINTER(Bucket)
DEFINE_PRINTER(Key)
DEFINE_PRINTER(UploadId)
DEFINE_PRINTER(MultipartUpload)
DEFINE_PRINTER(PartNumber)
DEFINE_PRINTER(ETag)
DEFINE_PRINTER(ContentLength)
DEFINE_PRINTER(Range)

#undef DEFINE_PRINTER

template <typename T>
using TPrinter = std::function<void(IOutputStream&, const T&)>;

template <typename T>
void OutRequest(IOutputStream& out, const T& request, const TVector<TPrinter<T>>& printers) {
    out << request.GetServiceRequestName() << " {";
    for (const auto& printer : printers) {
        out << " ";
        printer(out, request);
    }
    out << " }";
}

template <typename T>
void OutResult(IOutputStream& out, const T& result, TStringBuf header, const TVector<TPrinter<T>>& printers) {
    out << header << " {";
    for (const auto& printer : printers) {
        out << " ";
        printer(out, result);
    }
    out << " }";
}

template <typename T>
void OutOutcome(IOutputStream& out, const Aws::Utils::Outcome<T, Aws::S3::S3Error>& outcome) {
    if (outcome.IsSuccess()) {
        out << outcome.GetResult();
    } else {
        out << outcome.GetError().GetMessage().c_str();
    }
}

void Out(IOutputStream& out, const GetObjectRequest& request) {
    using T = GetObjectRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>, &Range<T>});
}

void Out(IOutputStream& out, const GetObjectResult& result) {
    OutResult(out, result, "GetObjectResult", {});
}

void Out(IOutputStream& out, const GetObjectOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const ListObjectsRequest& request) {
    using T = ListObjectsRequest;
    OutRequest(out, request, {&Bucket<T>});
}

void Out(IOutputStream& out, const ListObjectsResult& result) {
    OutResult(out, result, "ListObjectsResult", {});
}

void Out(IOutputStream& out, const ListObjectsOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const HeadObjectRequest& request) {
    using T = HeadObjectRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>});
}

void Out(IOutputStream& out, const HeadObjectResult& result) {
    using T = HeadObjectResult;
    OutResult(out, result, "HeadObjectResult", {&ETag<T>, &ContentLength<T>});
}

void Out(IOutputStream& out, const HeadObjectOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const PutObjectRequest& request) {
    using T = PutObjectRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>});
}

void Out(IOutputStream& out, const PutObjectResult& result) {
    using T = PutObjectResult;
    OutResult(out, result, "PutObjectResult", {&ETag<T>});
}

void Out(IOutputStream& out, const PutObjectOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const DeleteObjectRequest& request) {
    using T = DeleteObjectRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>});
}

void Out(IOutputStream& out, const DeleteObjectResult& result) {
    OutResult(out, result, "DeleteObjectResult", {});
}

void Out(IOutputStream& out, const DeleteObjectOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const DeleteObjectsRequest& request) {
    using T = DeleteObjectsRequest;
    OutRequest(out, request, { &Bucket<T> });
}

void Out(IOutputStream& out, const DeleteObjectsResult& result) {
    OutResult(out, result, "DeleteObjectsResult", {});
}

void Out(IOutputStream& out, const DeleteObjectsOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const CreateMultipartUploadRequest& request) {
    using T = CreateMultipartUploadRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>});
}

void Out(IOutputStream& out, const CreateMultipartUploadResult& result) {
    using T = CreateMultipartUploadResult;
    OutResult(out, result, "CreateMultipartUploadResult", {&Bucket<T>, &Key<T>, &UploadId<T>});
}

void Out(IOutputStream& out, const CreateMultipartUploadOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const CompleteMultipartUploadRequest& request) {
    using T = CompleteMultipartUploadRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>, &UploadId<T>, &MultipartUpload<T>});
}

void Out(IOutputStream& out, const CompleteMultipartUploadResult& result) {
    using T = CompleteMultipartUploadResult;
    OutResult(out, result, "CompleteMultipartUploadResult", {&Bucket<T>, &Key<T>, &ETag<T>});
}

void Out(IOutputStream& out, const CompleteMultipartUploadOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const AbortMultipartUploadRequest& request) {
    using T = AbortMultipartUploadRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>, &UploadId<T>});
}

void Out(IOutputStream& out, const AbortMultipartUploadResult& result) {
    OutResult(out, result, "AbortMultipartUploadResult", {});
}

void Out(IOutputStream& out, const AbortMultipartUploadOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const UploadPartRequest& request) {
    using T = UploadPartRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>, &UploadId<T>, &PartNumber<T>});
}

void Out(IOutputStream& out, const UploadPartResult& result) {
    using T = UploadPartResult;
    OutResult(out, result, "UploadPartResult", {&ETag<T>});
}

void Out(IOutputStream& out, const UploadPartOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const UploadPartCopyRequest& request) {
    using T = UploadPartCopyRequest;
    OutRequest(out, request, {&Bucket<T>, &Key<T>, &UploadId<T>, &PartNumber<T>});
}

void Out(IOutputStream& out, const UploadPartCopyResult& result) {
    OutResult(out, result, "UploadPartCopyResult", {});
}

void Out(IOutputStream& out, const UploadPartCopyOutcome& outcome) {
    OutOutcome(out, outcome);
}

void Out(IOutputStream& out, const CompletedMultipartUpload& upload) {
    out << "{ Parts: [" << JoinSeq(",", upload.GetParts()) << "] }";
}

void Out(IOutputStream& out, const CompletedPart& part) {
    out << part.GetETag();
}

void Out(IOutputStream& out, const TStringOutcome& outcome) {
    OutOutcome(out, outcome);
}

} // NKikimr::NWrappers
