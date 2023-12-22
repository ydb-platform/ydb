#include "aws.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

namespace NYdb::NConsoleClient {

const TString TCommandWithAwsCredentials::AwsCredentialsFile = "~/.aws/credentials";
const TString TCommandWithAwsCredentials::AwsDefaultProfileName = "default";

TString TCommandWithAwsCredentials::ReadIniKey(const TString& iniKey) {
    using namespace NConfig;

    const auto fileName = "AWS Credentials";
    const auto& profileName = AwsProfile.GetOrElse(AwsDefaultProfileName);

    try {
        if (!Config) {
            TString filePath = AwsCredentialsFile;
            const auto content = ReadFromFile(filePath, fileName);
            Config.ConstructInPlace(TConfig::ReadIni(content));
        }

        const auto& profiles = Config->Get<TDict>();
        if (!profiles.contains(profileName)) {
            throw yexception() << fileName << " file does not contain a profile '" << profileName << "'";
        }

        const auto& profile = profiles.At(profileName).Get<TDict>();
        if (!profile.contains(iniKey)) {
            throw yexception() << "Invalid profile '" << profileName << "' in " << fileName << " file";
        }

        return profile.At(iniKey).As<TString>();
    } catch (const TConfigError& ex) {
        throw yexception() << "Invalid " << fileName << " file: " << ex.what();
    }
}

class TS3ClientWrapper : public IS3ClientWrapper {
public:
    TS3ClientWrapper(const NImport::TImportFromS3Settings& settings) 
        : Bucket(settings.Bucket_)
    {
        Aws::S3::S3ClientConfiguration config;
        config.endpointOverride = settings.Endpoint_;
        if (settings.Scheme_ == ES3Scheme::HTTP) {
            config.scheme = Aws::Http::Scheme::HTTP;
        } else if (settings.Scheme_ == ES3Scheme::HTTPS) {
            config.scheme = Aws::Http::Scheme::HTTPS;
        } else {
            throw TMisuseException() << "\"" << settings.Scheme_ << "\" scheme type is not supported";
        }

        Client = std::make_unique<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(settings.AccessKey_, settings.SecretKey_),
                                   config,
                                   Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                   true);
    }

    std::pair<std::vector<TString>, std::optional<TString>> ListObjectKeys(const TString& prefix, const std::optional<TString>& token) override {
        auto request = Aws::S3::Model::ListObjectsV2Request()
            .WithBucket(Bucket)
            .WithPrefix(prefix);
        if (token) {
            request.WithContinuationToken(*token);
        }
        auto response = Client->ListObjectsV2(request);
        if (!response.IsSuccess()) {
            throw TMisuseException() << "ListObjectKeys error: " << response.GetError().GetMessage();
        }
        std::vector<TString> keys;
        for (const auto& object : response.GetResult().GetContents()) {
            keys.emplace_back(object.GetKey());
        }
        std::optional<TString> nextToken;
        if (response.GetResult().GetIsTruncated()) {
            nextToken = TString(response.GetResult().GetNextContinuationToken());
        }
        return {keys, nextToken};
    }

private:
    std::unique_ptr<Aws::S3::S3Client> Client;
    const TString Bucket;
};

std::unique_ptr<IS3ClientWrapper> CreateS3ClientWrapper(const NImport::TImportFromS3Settings& settings) {
    return std::make_unique<TS3ClientWrapper>(settings);
}

void InitAwsAPI() {
    Aws::InitAPI(Aws::SDKOptions());
}

void ShutdownAwsAPI() {
    Aws::ShutdownAPI(Aws::SDKOptions());
}

}
