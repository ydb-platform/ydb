#include "ru_yandex_passport_tvmauth_TvmApiSettings.h"

#include "util.h"

#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/api/settings.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_dispose(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        delete reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
    });
}

jlong Java_ru_yandex_passport_tvmauth_TvmApiSettings_factory(
    JNIEnv* jenv,
    jclass) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jlong {
        return (jlong) new NTvmApi::TClientSettings;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_setSelfTvmIdNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jint tvmId) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->SelfTvmId = tvmId;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_enableServiceTicketCheckingNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->CheckServiceTickets = true;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_enableUserTicketCheckingNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jint env) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->CheckUserTicketsWithBbEnv = static_cast<EBlackboxEnv>(env);
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_setDiskCacheDirNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring dir) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        Y_ENSURE(dir);
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->DiskCacheDir = TString(TJavaString(jenv, dir));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_enableServiceTicketsFetchOptionsWithAliases(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring selfSecret,
    jstring dsts) {
    return CatchAndRethrowExceptions(jenv, [=]() -> void {
        Y_ENSURE(selfSecret);
        Y_ENSURE(dsts);
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->Secret = TString(TJavaString(jenv, selfSecret));
        instance->FetchServiceTicketsForDstsWithAliases = NUtils::ParseDstMap(TJavaString(jenv, dsts));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_enableServiceTicketsFetchOptionsWithTvmIds(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring selfSecret,
    jstring dsts) {
    return CatchAndRethrowExceptions(jenv, [=]() -> void {
        Y_ENSURE(selfSecret);
        Y_ENSURE(dsts);
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->Secret = TString(TJavaString(jenv, selfSecret));
        instance->FetchServiceTicketsForDsts = NUtils::ParseDstVector(TJavaString(jenv, dsts));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_fetchRolesForIdmSystemSlugNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring slug) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->FetchRolesForIdmSystemSlug = TString(TJavaString(jenv, slug));
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_shouldCheckSrcNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jboolean value) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->ShouldCheckSrc = value;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_shouldCheckDefaultUidNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jboolean value) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->ShouldCheckDefaultUid = value;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_setTvmHostPortNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring host,
    jint port) {
    return CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->TvmHost = TString(TJavaString(jenv, host));
        instance->TvmPort = port;
    });
}

void Java_ru_yandex_passport_tvmauth_TvmApiSettings_setTiroleConnectionParamsNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring host,
    jint port,
    jint tvmid) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        NTvmApi::TClientSettings* instance = reinterpret_cast<NTvmApi::TClientSettings*>(jobj);
        instance->TiroleHost = TString(TJavaString(jenv, host));
        instance->TirolePort = port;
        instance->TiroleTvmId = tvmid;
    });
}
