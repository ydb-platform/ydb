#pragma once

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/map.h>

namespace NYdb {
namespace NConsoleClient {

class TMisuseException : public yexception {};

class TProfileConfig {
public:
    TProfileConfig(const TString& profileName);
    bool GetVariable(const TString& name, TString& value);

private:
    void ReadFromFile();

    TString ProfileName;
    bool InMemory = false;
    TMap<TString, TString> Values;
};

bool ReadFromFileIfExists(TString& filePath, const TString& fileName, TString& output);
TString ReadFromFile(TString& filePath, const TString& fileName);
size_t TermWidth();
TString InputPassword();

}
}
