#include "aws.h"

namespace NYdb {
namespace NConsoleClient {

const TString TCommandWithAwsCredentials::AwsCredentialsFile= "~/.aws/credentials";

const TString& TCommandWithAwsCredentials::ReadAwsCredentialsFile() {
    if (!FileContent) {
        TString credentialsFile = AwsCredentialsFile;
        FileContent = ReadFromFile(credentialsFile, "AWS Credentials");
    }

    return FileContent.GetRef();
}

static bool IsSkipSymbol(char s) {
    switch (s) {
    case ' ':
    case '\t':
    case '=':
        return true;
    }
    return false;
}

static TStringBuf GetKey(TStringBuf content, const TString& key) {
    TStringBuf line;
    while (content.ReadLine(line)) {
        if (!line.SkipPrefix(key)) {
            continue;
        }

        while (!line.empty() && IsSkipSymbol(line.front())) {
            line.Skip(1);
        }

        return line;
    }

    throw TMisuseException() << "Cannot find \"" << key << "\" key";
}

void TCommandWithAwsCredentials::ReadAwsAccessKey() {
    AwsAccessKey = GetKey(ReadAwsCredentialsFile(), "aws_access_key_id");
}

void TCommandWithAwsCredentials::ReadAwsSecretKey() {
    AwsSecretKey = GetKey(ReadAwsCredentialsFile(), "aws_secret_access_key");
}

}
}
