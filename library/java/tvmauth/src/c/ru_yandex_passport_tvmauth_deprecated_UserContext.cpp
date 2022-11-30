#include "ru_yandex_passport_tvmauth_deprecated_UserContext.h"

#include "util.h"

#include <library/cpp/tvmauth/deprecated/user_context.h>
#include <library/cpp/tvmauth/src/user_impl.h>

#include <util/generic/strbuf.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

void Java_ru_yandex_passport_tvmauth_deprecated_UserContext_dispose(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        delete reinterpret_cast<TUserContext::TImpl*>(jobj);
    });
}

jobject Java_ru_yandex_passport_tvmauth_deprecated_UserContext_checkNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring ticketBody) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(ticketBody);
        auto* instance = reinterpret_cast<TUserContext::TImpl*>(jobj);
        return BuildJavaObject(jenv, instance->Check(TJavaString(jenv, ticketBody)));
    });
}

jlong Java_ru_yandex_passport_tvmauth_deprecated_UserContext_factory(
    JNIEnv* jenv,
    jclass,
    jint env,
    jstring tvmKeysResponse) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        Y_ENSURE(tvmKeysResponse);
        return (jlong) new TUserContext::TImpl(
            EBlackboxEnv(env),
            TJavaString(jenv, tvmKeysResponse));
    });
}
