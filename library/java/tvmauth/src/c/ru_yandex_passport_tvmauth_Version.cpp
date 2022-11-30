#include "ru_yandex_passport_tvmauth_Version.h"

#include <library/cpp/tvmauth/version.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

using namespace NTvmAuth;

jstring Java_ru_yandex_passport_tvmauth_Version_get(JNIEnv* jenv, jclass) {
    return jenv->NewStringUTF((TString("java_") + LibVersion()).c_str());
}
