#include "ru_yandex_passport_tvmauth_DynamicClient.h"

#include "util.h"

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/misc/utils.h>
#include <library/cpp/tvmauth/client/misc/api/dynamic_dst/tvm_client.h>
#include <library/cpp/tvmauth/src/utils.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

jobject Java_ru_yandex_passport_tvmauth_DynamicClient_factoryDynamicClientNative(
    JNIEnv* jenv,
    jclass,
    jlong settings,
    jlong logger) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(settings);
        NTvmApi::TClientSettings& s = *reinterpret_cast<NTvmApi::TClientSettings*>(settings);
        s.LibVersionPrefix = "javadyn_";

        auto c = NDynamicClient::TTvmClient::Create(
            s,
            reinterpret_cast<TJavaLogger*>(logger));

        THolder<TTvmClient> instance = MakeHolder<TTvmClient>(TAsyncUpdaterPtr(c));

        jclass cls = jenv->FindClass("ru/yandex/passport/tvmauth/DynamicClient$NativeHandles");
        jobject res = jenv->AllocObject(cls);

        jenv->SetLongField(
            res,
            jenv->GetFieldID(cls, "dyn", "J"),
            (jlong)c.Get());
        jenv->SetLongField(
            res,
            jenv->GetFieldID(cls, "common", "J"),
            (jlong)instance.Get());

        Y_UNUSED(instance.Release());
        return res;
    });
}

void Java_ru_yandex_passport_tvmauth_DynamicClient_addDstsNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jstring dsts) {
    CatchAndRethrowExceptions(jenv, [=]() -> void {
        Y_ENSURE(dsts);
        auto* instance = reinterpret_cast<NDynamicClient::TTvmClient*>(jobj);

        NTvmApi::TClientSettings::TDstVector vec = NUtils::ParseDstVector(TJavaString(jenv, dsts));
        instance->Add(NDynamicClient::TDsts(vec.begin(), vec.end()));
    });
}

jstring Java_ru_yandex_passport_tvmauth_DynamicClient_getOptionalServiceTicketForTvmIdNative(
    JNIEnv* jenv,
    jclass,
    jlong jobj,
    jint tvmId) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jstring {
        auto* instance = reinterpret_cast<NDynamicClient::TTvmClient*>(jobj);
        std::optional<TString> res = instance->GetOptionalServiceTicketFor(tvmId);

        return res ? jenv->NewStringUTF(res->c_str()) : nullptr;
    });
}
