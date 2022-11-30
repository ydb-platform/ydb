#include "ru_yandex_passport_tvmauth_internal_LogFetcher.h"

#include "util.h"

using namespace NTvmAuth;
using namespace NTvmAuthJava;

jobjectArray Java_ru_yandex_passport_tvmauth_internal_LogFetcher_fetch(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobjectArray {
        TJavaLogger* instance = reinterpret_cast<TJavaLogger*>(jobj);
        const TVector<TJavaLogger::TMessage> messages = instance->FetchMessages();
        if (messages.empty()) {
            return nullptr;
        }

        jclass cls = jenv->FindClass("ru/yandex/passport/tvmauth/internal/LogFetcher$Message");
        jobjectArray res = jenv->NewObjectArray(
            messages.size(),
            cls,
            nullptr);

        const jfieldID lvlFld = jenv->GetFieldID(cls, "lvl", "I");
        const jfieldID msgFld = jenv->GetFieldID(cls, "msg", "Ljava/lang/String;");

        for (size_t idx = 0; idx < messages.size(); ++idx) {
            jobject message = jenv->AllocObject(cls);

            jenv->SetIntField(
                message,
                lvlFld,
                messages[idx].first);

            jenv->SetObjectField(
                message,
                msgFld,
                jenv->NewStringUTF(messages[idx].second.c_str()));

            jenv->SetObjectArrayElement(res, idx, message);
        }

        return res;
    });
}

jlong Java_ru_yandex_passport_tvmauth_internal_LogFetcher_factory(
    JNIEnv* jenv,
    jclass) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        return (jlong) new TJavaLogger;
    });
}

void Java_ru_yandex_passport_tvmauth_internal_LogFetcher_dispose(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        delete reinterpret_cast<TJavaLogger*>(jobj);
    });
}
