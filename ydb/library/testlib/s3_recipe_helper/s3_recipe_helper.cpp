#include "s3_recipe_helper.h"

#include <library/cpp/testing/hook/hook.h>

#include <util/string/builder.h>

#include <aws/core/Aws.h>

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    Aws::InitAPI(Aws::SDKOptions());
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    Aws::ShutdownAPI(Aws::SDKOptions());
}

namespace NTestUtils {

    Aws::S3::S3Client MakeS3Client() {
        Aws::Client::ClientConfiguration s3ClientConfig;
        s3ClientConfig.endpointOverride = GetEnv("S3_ENDPOINT");
        s3ClientConfig.scheme = Aws::Http::Scheme::HTTP;
        return Aws::S3::S3Client(
            std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(),
            s3ClientConfig,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /*useVirtualAddressing=*/true);
    }

    void CreateBucket(const TString& bucket, Aws::S3::S3Client& s3Client) {
        Aws::S3::Model::CreateBucketRequest req;
        req.SetBucket(bucket);
        req.SetACL(Aws::S3::Model::BucketCannedACL::public_read_write);
        const Aws::S3::Model::CreateBucketOutcome result = s3Client.CreateBucket(req);
        UNIT_ASSERT_C(result.IsSuccess(), "Error creating bucket \"" << bucket << "\": " << result.GetError().GetExceptionName() << ": " << result.GetError().GetMessage());
    }

    void CreateBucket(const TString& bucket) {
        Aws::S3::S3Client s3Client = MakeS3Client();

        CreateBucket(bucket, s3Client);
    }

    void UploadObject(const TString& bucket, const TString& object, const TStringBuf& content, Aws::S3::S3Client& s3Client) {
        Aws::S3::Model::PutObjectRequest req;
        req.WithBucket(bucket).WithKey(object);

        auto inputStream = std::make_shared<std::stringstream>();
        *inputStream << content;
        req.SetBody(inputStream);
        const Aws::S3::Model::PutObjectOutcome result = s3Client.PutObject(req);
        UNIT_ASSERT_C(result.IsSuccess(), "Error uploading object \"" << object << "\" to a bucket \"" << bucket << "\": " << result.GetError().GetExceptionName() << ": " << result.GetError().GetMessage());
    }

    void UploadObject(const TString& bucket, const TString& object, const TStringBuf& content) {
        Aws::S3::S3Client s3Client = MakeS3Client();

        UploadObject(bucket, object, content, s3Client);
    }

    void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content, Aws::S3::S3Client& s3Client) {
        CreateBucket(bucket, s3Client);
        UploadObject(bucket, object, content, s3Client);
    }

    void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content) {
        Aws::S3::S3Client s3Client = MakeS3Client();

        CreateBucketWithObject(bucket, object, content, s3Client);
    }

    TString GetObject(const TString& bucket, const TString& object, Aws::S3::S3Client& s3Client) {
        Aws::S3::Model::GetObjectRequest req;
        req.WithBucket(bucket).WithKey(object);

        Aws::S3::Model::GetObjectOutcome outcome = s3Client.GetObject(req);
        UNIT_ASSERT(outcome.IsSuccess());
        Aws::S3::Model::GetObjectResult& result = outcome.GetResult();
        std::istreambuf_iterator<char> eos;
        std::string objContent(std::istreambuf_iterator<char>(result.GetBody()), eos);
        Cerr << "Got object content from \"" << bucket << "." << object << "\"\n"
             << objContent << Endl;
        return objContent;
    }

    TString GetObject(const TString& bucket, const TString& object) {
        Aws::S3::S3Client s3Client = MakeS3Client();

        return GetObject(bucket, object, s3Client);
    }

    std::vector<TString> GetObjectKeys(const TString& bucket, Aws::S3::S3Client& s3Client) {
        Aws::S3::Model::ListObjectsRequest listReq;
        listReq.WithBucket(bucket);

        Aws::S3::Model::ListObjectsOutcome outcome = s3Client.ListObjects(listReq);
        UNIT_ASSERT(outcome.IsSuccess());

        std::vector<TString> keys;
        for (auto& obj : outcome.GetResult().GetContents()) {
            keys.push_back(TString(obj.GetKey()));
            Cerr << "Found S3 object: \"" << obj.GetKey() << "\"" << Endl;
        }
        return keys;
    }

    std::vector<TString> GetObjectKeys(const TString& bucket) {
        Aws::S3::S3Client s3Client = MakeS3Client();

        return GetObjectKeys(bucket, s3Client);
    }

    TString GetAllObjects(const TString& bucket, TStringBuf separator, Aws::S3::S3Client& s3Client) {
        std::vector<TString> keys = GetObjectKeys(bucket, s3Client);
        TString result;
        bool firstObject = true;
        for (const TString& key : keys) {
            result += GetObject(bucket, key, s3Client);
            if (!firstObject) {
                result += separator;
            }
            firstObject = false;
        }
        return result;
    }

    TString GetAllObjects(const TString& bucket, TStringBuf separator) {
        Aws::S3::S3Client s3Client = MakeS3Client();

        return GetAllObjects(bucket, separator, s3Client);
    }

    TString GetBucketLocation(const TStringBuf bucket) {
        return TStringBuilder() << GetEnv("S3_ENDPOINT") << '/' << bucket << '/';
    }

} // namespace NTestUtils
