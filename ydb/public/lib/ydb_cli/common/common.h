#pragma once

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/string.h>
#include <util/folder/dirut.h>
#include <util/generic/yexception.h>
#include <util/generic/map.h>

#if defined(_win32_)
#include <util/system/env.h>
#endif

namespace NYdb {
namespace NConsoleClient {

#if defined(_darwin_)
    const TString HomeDir = GetHomeDir();
#elif defined(_win32_)
    const TString HomeDir = GetEnv("USERPROFILE");
#else
    const TString HomeDir = GetHomeDir();
#endif

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

bool ReadFromFileIfExists(TString& filePath, const TString& fileName, TString& output, bool allowEmpty = false);
bool ReadFromFileIfExists(const TString& filePath, const TString& fileName, TString& output, bool allowEmpty = false);
TString ReadFromFile(TString& filePath, const TString& fileName, bool allowEmpty = false);
TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty = false);
TString InputPassword();

}
}
