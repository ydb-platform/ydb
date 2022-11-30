#include "ru_yandex_passport_tvmauth_Utils.h"

#include "util.h"

#include <library/cpp/tvmauth/utils.h>

#include <util/generic/strbuf.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

jstring Java_ru_yandex_passport_tvmauth_Utils_removeTicketSignature(JNIEnv* jenv, jclass, jstring ticketBody) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jstring {
        Y_ENSURE(ticketBody);
        TString removedSignature(NUtils::RemoveTicketSignature(TJavaString(jenv, ticketBody)));
        jstring result = jenv->NewStringUTF(removedSignature.c_str());
        return result;
    });
}
