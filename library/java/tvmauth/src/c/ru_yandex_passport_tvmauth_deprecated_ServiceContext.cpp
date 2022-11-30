#include "ru_yandex_passport_tvmauth_deprecated_ServiceContext.h"

#include "util.h"

#include <library/cpp/tvmauth/deprecated/service_context.h>
#include <library/cpp/tvmauth/src/service_impl.h>

#include <util/generic/strbuf.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

void Java_ru_yandex_passport_tvmauth_deprecated_ServiceContext_dispose(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        delete reinterpret_cast<TServiceContext::TImpl*>(jobj);
    });
}

jstring Java_ru_yandex_passport_tvmauth_deprecated_ServiceContext_signCgiParamsForTvmNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring ts,
    jstring dst,
    jstring scopes) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jstring {
        Y_ENSURE(ts);
        Y_ENSURE(dst);
        Y_ENSURE(scopes);
        auto* instance = reinterpret_cast<TServiceContext::TImpl*>(jobj);
        jstring result = jenv->NewStringUTF(
            instance->SignCgiParamsForTvm(
                        TJavaString(jenv, ts),
                        TJavaString(jenv, dst),
                        TJavaString(jenv, scopes))
                .c_str());
        return result;
    });
}

jobject Java_ru_yandex_passport_tvmauth_deprecated_ServiceContext_checkNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring ticketBody) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(ticketBody);
        auto* instance = reinterpret_cast<TServiceContext::TImpl*>(jobj);

        return BuildJavaObject(jenv, instance->Check(TJavaString(jenv, ticketBody)));
    });
}

jlong Java_ru_yandex_passport_tvmauth_deprecated_ServiceContext_factory(
    JNIEnv* jenv,
    jclass,
    jint tvmId,
    jstring secretBase64,
    jstring tvmKeysResponse) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        if (secretBase64 != nullptr && tvmKeysResponse != nullptr) {
            return (jlong) new TServiceContext::TImpl(
                TJavaString(jenv, secretBase64),
                tvmId,
                TJavaString(jenv, tvmKeysResponse));
        }

        if (tvmKeysResponse != nullptr) {
            return (jlong) new TServiceContext::TImpl(
                tvmId,
                TJavaString(jenv, tvmKeysResponse));
        }

        if (secretBase64 != nullptr) {
            return (jlong) new TServiceContext::TImpl(TJavaString(jenv, secretBase64));
        }

        ythrow yexception() << "Missing tvmKeysResponse or secretBase64";
    });
}
