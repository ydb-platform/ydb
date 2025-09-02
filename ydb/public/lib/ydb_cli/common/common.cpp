#include "common.h"

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/string/strip.h>

#if defined(_unix_)
#include <sys/ioctl.h>
#include <termios.h>

#elif defined(_win_)
#include <windows.h>
#endif

namespace NYdb {
namespace NConsoleClient {

TProfileConfig::TProfileConfig(const TString& profileName)
    : ProfileName(profileName)
{
}

bool TProfileConfig::GetVariable(const TString& name, TString& value) {
    if (!InMemory) {
        ReadFromFile();
    }
    const auto it = Values.find(name);
    if (it != Values.end()) {
        value = it->second;
        return true;
    }
    return false;
}

void TProfileConfig::ReadFromFile() {
    TString profileConfigFile = "~/.ydb/" + ProfileName + "/config";
    correctpath(profileConfigFile);
    TFsPath configFile(profileConfigFile);
    if (!configFile.Exists()) {
        throw yexception() << "Can't find provided config file \"" << profileConfigFile << "\" for given profile.";
    }
    TFileInput config(profileConfigFile);
    TString line;
    while (config.ReadLine(line)) {
        auto it = line.find('=');
        if (it != TString::npos) {
            TString name = Strip(line.substr(0, it));
            TString value = Strip(line.substr(it + 1));
            if (name && value) {
                Values[name] = value;
                continue;
            }
        }
        throw yexception() << "Error parsing config file \"" << profileConfigFile << "\" for given profile.";
    }
    if (Values.empty()) {
        throw yexception() << "Empty config file \"" << profileConfigFile << "\" for given profile.";
    }
    InMemory = true;
}

TFsPath GetFsPath(TString& filePath) {
    if (filePath.StartsWith("~")) {
        filePath = HomeDir + filePath.substr(1);
    }
    TFsPath fsPath(filePath);
    if (!fsPath.Exists()) {
        correctpath(filePath);
        fsPath = TFsPath(filePath);
    }
    return fsPath;
}

bool ReadFromFileIfExists(TString& filePath, const TString& fileName, TString& output, bool allowEmpty) {
    TFsPath fsPath = GetFsPath(filePath);
    TString content;
    if (fsPath.Exists()) {
        content = Strip(TUnbufferedFileInput(fsPath).ReadAll());
        if (!content && !allowEmpty) {
            throw yexception() << "Empty " << fileName << " file provided: \"" << filePath << "\".";
        } else {
            output = content;
            return true;
        }
    }
    return false;
}

bool ReadFromFileIfExists(const TString& filePath, const TString& fileName, TString& output, bool allowEmpty) {
    TString fpCopy = filePath;
    return ReadFromFileIfExists(fpCopy, fileName, output, allowEmpty);
}

TString ReadFromFile(TString& filePath, const TString& fileName, bool allowEmpty) {
    TString content;
    if (ReadFromFileIfExists(filePath, fileName, content, allowEmpty)) {
        return content;
    } else {
        throw yexception() << "Can't find " << fileName << " file \"" << filePath << "\".";
    }
}

TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty) {
    TString fpCopy = filePath;
    return ReadFromFile(fpCopy, fileName, allowEmpty);
}

TFsPath GetExistingFsPath(TString& filePath, const TString& fileName) {
    TFsPath fsPath = GetFsPath(filePath);
    if (fsPath.Exists()) {
        return fsPath;
    }
    throw yexception() << "Can't find " << fileName << " file \"" << filePath << "\".";
}

TString InputPassword() {
    // Disable echoing characters and enable per-symbol input handling
#if defined(_unix_)
    termios oldState;
    tcgetattr(STDIN_FILENO, &oldState);
    termios newState = oldState;
    newState.c_lflag &= ~ECHO;
    newState.c_lflag &= VEOF;
    tcsetattr(STDIN_FILENO, TCSANOW, &newState);
#elif defined(_win_)
    HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE);
    DWORD mode;
    DWORD count;
    GetConsoleMode(hStdin, &mode);
    SetConsoleMode(hStdin, mode & (~ENABLE_ECHO_INPUT) & (~ENABLE_LINE_INPUT));
#endif

    TString password;
    char c;

    for (
#if defined(_unix_)
        Cin >> c;
#elif defined(_win_)
        // Cin doesn't handle "Enter" press properly on Windows
        ReadConsoleA(hStdin, &c, 1, &count, NULL);
#endif
        c != '\r' && c != '\n';
#if defined(_unix_)
        Cin >> c
#elif defined(_win_)
        ReadConsoleA(hStdin, &c, 1, &count, NULL)
#endif
        ) {
        if (c == '\b' || c == 0x7F) {
            // Backspace. Remove last char if there is any
            if (password.size()) {
                Cerr << "\b \b";
                password.pop_back();
            }
        } else if (c == 0x03) {
            // Ctrl + C
            Cerr << Endl << "Input interrupted" << Endl;
#if defined(_unix_)
            tcsetattr(STDIN_FILENO, TCSANOW, &oldState);
#elif defined(_win_)
            SetConsoleMode(hStdin, mode);
#endif
            throw yexception() << "Input interrupted";
        } else {
            Cerr << '*';
            password.push_back(c);
        }
    }
    Cerr << Endl;

#if defined(_unix_)
    tcsetattr(STDIN_FILENO, TCSANOW, &oldState);
#elif defined(_win_)
    SetConsoleMode(hStdin, mode);
#endif

    return password;
}

}
}
