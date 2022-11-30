#include "ru_yandex_passport_tvmauth_NativeTvmClient.h"

#include "util.h"

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/src/utils.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

void Java_ru_yandex_passport_tvmauth_NativeTvmClient_dispose(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        delete reinterpret_cast<TTvmClient*>(jobj);
    });
}

jlong Java_ru_yandex_passport_tvmauth_NativeTvmClient_factoryTvmApi(
    JNIEnv* jenv,
    jclass,
    jlong settings,
    jlong logger) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        Y_ENSURE(settings);
        Y_ENSURE(logger);

        NTvmApi::TClientSettings& s = *reinterpret_cast<NTvmApi::TClientSettings*>(settings);
        s.LibVersionPrefix = "java_";

        return (jlong) new TTvmClient(
            s,
            reinterpret_cast<TJavaLogger*>(logger));
    });
}

jlong Java_ru_yandex_passport_tvmauth_NativeTvmClient_factoryTvmTool(
    JNIEnv* jenv,
    jclass,
    jlong settings,
    jlong logger) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        Y_ENSURE(settings);
        Y_ENSURE(logger);

        return (jlong) new TTvmClient(
            *reinterpret_cast<NTvmTool::TClientSettings*>(settings),
            reinterpret_cast<TJavaLogger*>(logger));
    });
}

jobject Java_ru_yandex_passport_tvmauth_NativeTvmClient_getStatusNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);
        const TClientStatus status = instance->GetStatus();

        jclass cls = jenv->FindClass("ru/yandex/passport/tvmauth/ClientStatus");
        jobject res = jenv->AllocObject(cls);

        SetClientStatus(jenv, status.GetCode(), cls, res);

        jenv->SetObjectField(
            res,
            jenv->GetFieldID(cls, "lastError", "Ljava/lang/String;"),
            jenv->NewStringUTF(status.GetLastError().c_str()));

        return res;
    });
}

jstring Java_ru_yandex_passport_tvmauth_NativeTvmClient_getServiceTicketForAlias(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring alias) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jstring {
        Y_ENSURE(alias);
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);
        return jenv->NewStringUTF(instance->GetServiceTicketFor(TString(TJavaString(jenv, alias))).c_str());
    });
}

jstring Java_ru_yandex_passport_tvmauth_NativeTvmClient_getServiceTicketForTvmId(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jint tvmId) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jstring {
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);
        return jenv->NewStringUTF(instance->GetServiceTicketFor(tvmId).c_str());
    });
}

jobject Java_ru_yandex_passport_tvmauth_NativeTvmClient_checkServiceTicketNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring ticket) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(ticket);
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);
        TCheckedServiceTicket t = instance->CheckServiceTicket(TJavaString(jenv, ticket));
        return BuildJavaObject(jenv, t);
    });
}

jobject Java_ru_yandex_passport_tvmauth_NativeTvmClient_checkUserTicketNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring ticket) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(ticket);
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);
        TCheckedUserTicket t = instance->CheckUserTicket(TJavaString(jenv, ticket));
        return BuildJavaObject(jenv, t);
    });
}

jobject Java_ru_yandex_passport_tvmauth_NativeTvmClient_checkUserTicketNativeWithOverridedEnv(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring ticket,
    jint env) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(ticket);
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);
        TCheckedUserTicket t = instance->CheckUserTicket(TJavaString(jenv, ticket), (EBlackboxEnv)env);
        return BuildJavaObject(jenv, t);
    });
}

jstring Java_ru_yandex_passport_tvmauth_NativeTvmClient_getRolesNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring revision) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jstring {
        TTvmClient* instance = reinterpret_cast<TTvmClient*>(jobj);

        NRoles::TRolesPtr roles = instance->GetRoles();
        if (revision && TJavaString(jenv, revision) == roles->GetMeta().Revision) {
            return nullptr;
        }

        return jenv->NewStringUTF(roles->GetRaw().c_str());
    });
}
