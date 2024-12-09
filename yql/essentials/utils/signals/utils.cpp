#include "utils.h"

#include <util/generic/yexception.h>
#include <util/string/subst.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/json/yson/json2yson.h>

#include <string.h>

extern char** environ;

namespace NYql {

static char** g_OriginalArgv = nullptr;
static char* g_OriginalArgvLast = nullptr;

/*
 * To change the process title in Linux and Darwin we have to set argv[1]
 * to NULL and to copy the title to the same place where the argv[0] points to.
 * However, argv[0] may be too small to hold a new title. Fortunately, Linux
 * and Darwin store argv[] and environ[] one after another. So we should
 * ensure that is the continuous memory and then we allocate the new memory
 * for environ[] and copy it. After this we could use the memory starting
 * from argv[0] for our process title.
 *
 *               continuous memory block for process title
 *  ________________________________/\____________________________________
 * /                                                                      \
 * +---------+---------+-----+------+------------+------------+-----+------+
 * | argv[0] | argv[1] | ... | NULL | environ[0] | environ[1] | ... | NULL |
 * +---------+---------+-----+------+------------+------------+-----+------+
 *                                   \_________________  _________________/
 *                                                     \/
 *                                         must be relocated elsewhere
 */
void ProcTitleInit(int argc, const char* argv[])
{
    Y_UNUSED(argc);
    Y_ABORT_UNLESS(!g_OriginalArgv, "ProcTitleInit() was already called");

    g_OriginalArgv = const_cast<char**>(argv);

    size_t size = 0;
    for (int i = 0; environ[i]; i++) {
        size += strlen(environ[i]) + 1;
    }

    char* newEnviron = new char[size];
    g_OriginalArgvLast = g_OriginalArgv[0];

    for (int i = 0; g_OriginalArgv[i]; i++) {
        if (g_OriginalArgvLast == g_OriginalArgv[i]) {
            g_OriginalArgvLast = g_OriginalArgv[i] + strlen(g_OriginalArgv[i]) + 1;
        }
    }

    for (int i = 0; environ[i]; i++) {
        if (g_OriginalArgvLast == environ[i]) {
            size_t size = strlen(environ[i]) + 1;
            g_OriginalArgvLast = environ[i] + size;

            strncpy(newEnviron, environ[i], size);
            environ[i] = newEnviron;
            newEnviron += size;
        }
    }

    g_OriginalArgvLast--;
}

void SetProcTitle(const char* title)
{
    if (!g_OriginalArgv) return;

    char* p = g_OriginalArgv[0];
    p += strlcpy(p, "yqlworker: ", g_OriginalArgvLast - p);
    p += strlcpy(p, title, g_OriginalArgvLast - p);

    if (g_OriginalArgvLast - p > 0) {
        memset(p, 0, g_OriginalArgvLast - p);
    }

    g_OriginalArgv[1] = nullptr;
}

void AddProcTitleSuffix(const char* suffix)
{
    if (!g_OriginalArgv) return;

    char* p = g_OriginalArgv[0];
    p += strlcat(p, " ", g_OriginalArgvLast - p);
    p += strlcat(p, suffix, g_OriginalArgvLast - p);
}

const char* GetProcTitle()
{
    return g_OriginalArgv ? g_OriginalArgv[0] : "UNKNOWN";
}

TString PbMessageToStr(const google::protobuf::Message& msg)
{
    TString str;
    ::google::protobuf::TextFormat::Printer printer;
    printer.SetSingleLineMode(true);
    printer.PrintToString(msg, &str);
    return str;
}

TString Proto2Yson(const google::protobuf::Message& proto) {
    NJson::TJsonValue json;
    NProtobufJson::Proto2Json(proto, json);

    TString ysonResult;
    TStringOutput stream(ysonResult);
    NJson2Yson::SerializeJsonValueAsYson(json, &stream);
    return ysonResult;
}

} // namespace NYql
