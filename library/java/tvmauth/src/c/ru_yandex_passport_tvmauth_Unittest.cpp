#include "ru_yandex_passport_tvmauth_Unittest.h"

#include "util.h"

#include <library/cpp/tvmauth/src/service_impl.h>
#include <library/cpp/tvmauth/src/user_impl.h>

#include <util/generic/strbuf.h>
#include <util/string/cast.h>

using namespace NTvmAuth;
using namespace NTvmAuthJava;

jobject Java_ru_yandex_passport_tvmauth_Unittest_createServiceTicketNative(
    JNIEnv* jenv,
    jclass,
    jint status,
    jint src,
    jlong issuerUid) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        return BuildJavaObject(
            jenv,
            TCheckedServiceTicket::TImpl::CreateTicketForTests(
                static_cast<ETicketStatus>(status),
                src,
                issuerUid));
    });
}

jobject Java_ru_yandex_passport_tvmauth_Unittest_createUserTicketNative(
    JNIEnv* jenv,
    jclass,
    jint status,
    jlong defaultUid,
    jstring scopes,
    jstring uids,
    jint env) {
    return CatchAndRethrowExceptions(jenv, [=]() -> jobject {
        Y_ENSURE(scopes);
        Y_ENSURE(uids);
        TJavaString scTmp(jenv, scopes);
        TScopes sc;
        TStringBuf scTmpBuf = scTmp;
        while (scTmpBuf) {
            sc.push_back(scTmpBuf.NextTok(';'));
        }

        TJavaString uiTmp(jenv, uids);
        TUids ui;
        TStringBuf uiTmpBuf = uiTmp;
        while (uiTmpBuf) {
            ui.push_back(IntFromString<TUid, 10>(uiTmpBuf.NextTok(';')));
        }

        return BuildJavaObject(
            jenv,
            TCheckedUserTicket::TImpl::CreateTicketForTests(
                static_cast<ETicketStatus>(status),
                defaultUid,
                sc,
                ui,
                EBlackboxEnv(env)));
    });
}
