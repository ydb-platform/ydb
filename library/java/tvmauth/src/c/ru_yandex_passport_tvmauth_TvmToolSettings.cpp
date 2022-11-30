#include "ru_yandex_passport_tvmauth_TvmToolSettings.h"

#include "util.h"

#include <library/cpp/tvmauth/client/misc/tool/settings.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_dispose(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        delete reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
    });
}

jlong Java_ru_yandex_passport_tvmauth_TvmToolSettings_factory(
    JNIEnv* jenv,
    jclass,
    jstring selfAlias) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        Y_ENSURE(selfAlias);
        return (jlong) new NTvmTool::TClientSettings(
            TString(TJavaString(jenv, selfAlias)));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_setPortNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jint port) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmTool::TClientSettings* instance = reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
        instance->SetPort(port);
    });
}

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_setHostnameNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring hostname) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        Y_ENSURE(hostname);
        NTvmTool::TClientSettings* instance = reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
        instance->SetHostname(TString(TJavaString(jenv, hostname)));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_setAuthTokenNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring authtoken) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        Y_ENSURE(authtoken);
        NTvmTool::TClientSettings* instance = reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
        instance->SetAuthToken(TString(TJavaString(jenv, authtoken)));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_overrideBlackboxEnv(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jint env) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmTool::TClientSettings* instance = reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
        instance->OverrideBlackboxEnv(static_cast<EBlackboxEnv>(env));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_shouldCheckSrcNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jboolean value) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmTool::TClientSettings* instance = reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
        instance->ShouldCheckSrc = value;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmToolSettings_shouldCheckDefaultUidNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jboolean value) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmTool::TClientSettings* instance = reinterpret_cast<NTvmTool::TClientSettings*>(jobj);
        instance->ShouldCheckDefaultUid = value;
    });
}
