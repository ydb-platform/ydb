#include "util.h"

#include <util/generic/maybe.h>

#include <map>

namespace NTvmAuthJava {
    // https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/types.html

    static const std::map<NTvmAuth::ETicketStatus, TString> TICKET_STATUSES = {
        {NTvmAuth::ETicketStatus::Ok, "OK"},
        {NTvmAuth::ETicketStatus::Expired, "EXPIRED"},
        {NTvmAuth::ETicketStatus::InvalidBlackboxEnv, "INVALID_BLACKBOX_ENV"},
        {NTvmAuth::ETicketStatus::InvalidDst, "INVALID_DST"},
        {NTvmAuth::ETicketStatus::InvalidTicketType, "INVALID_TICKET_TYPE"},
        {NTvmAuth::ETicketStatus::Malformed, "MALFORMED"},
        {NTvmAuth::ETicketStatus::MissingKey, "MISSING_KEY"},
        {NTvmAuth::ETicketStatus::SignBroken, "SIGN_BROKEN"},
        {NTvmAuth::ETicketStatus::UnsupportedVersion, "UNSUPPORTED_VERSION"},
        {NTvmAuth::ETicketStatus::NoRoles, "NO_ROLES"},
    };

    static const char* TicketStatusToName(NTvmAuth::ETicketStatus s) {
        auto it = TICKET_STATUSES.find(s);
        Y_VERIFY(it != TICKET_STATUSES.end());
        return it->second.c_str();
    }

    static const std::map<NTvmAuth::TClientStatus::ECode, TString> CLIENT_STATUSES = {
        {NTvmAuth::TClientStatus::ECode::Ok, "OK"},
        {NTvmAuth::TClientStatus::ECode::Warning, "WARNING"},
        {NTvmAuth::TClientStatus::ECode::Error, "ERROR"},
    };

    static const char* ClientStatusToName(NTvmAuth::TClientStatus::ECode s) {
        auto it = CLIENT_STATUSES.find(s);
        Y_VERIFY(it != CLIENT_STATUSES.end());
        return it->second.c_str();
    }

    static const std::map<NTvmAuth::EBlackboxEnv, TString> BLACKBOX_ENV = {
        {NTvmAuth::EBlackboxEnv::Prod, "PROD"},
        {NTvmAuth::EBlackboxEnv::Test, "TEST"},
        {NTvmAuth::EBlackboxEnv::ProdYateam, "PROD_YATEAM"},
        {NTvmAuth::EBlackboxEnv::TestYateam, "TEST_YATEAM"},
        {NTvmAuth::EBlackboxEnv::Stress, "STRESS"},
    };

    static const char* BlackboxEnvToName(NTvmAuth::EBlackboxEnv e) {
        auto it = BLACKBOX_ENV.find(e);
        Y_VERIFY(it != BLACKBOX_ENV.end());
        return it->second.c_str();
    }

    void SetTicketStatus(JNIEnv* jenv, const NTvmAuth::ETicketStatus status, jclass cls, jobject res) {
        jclass statusClass = jenv->FindClass("ru/yandex/passport/tvmauth/TicketStatus");

        jfieldID statusValue = jenv->GetStaticFieldID(
            statusClass,
            TicketStatusToName(status),
            "Lru/yandex/passport/tvmauth/TicketStatus;");
        jobject value = jenv->GetStaticObjectField(statusClass, statusValue);

        jenv->SetObjectField(
            res,
            jenv->GetFieldID(cls, "status", "Lru/yandex/passport/tvmauth/TicketStatus;"),
            value);
    }

    void SetClientStatus(JNIEnv* jenv,
                         const NTvmAuth::TClientStatus::ECode status,
                         jclass cls,
                         jobject res) {
        jclass statusClass = jenv->FindClass("ru/yandex/passport/tvmauth/ClientStatus$Code");

        jfieldID statusValue = jenv->GetStaticFieldID(
            statusClass,
            ClientStatusToName(status),
            "Lru/yandex/passport/tvmauth/ClientStatus$Code;");
        jobject value = jenv->GetStaticObjectField(statusClass, statusValue);

        jenv->SetObjectField(
            res,
            jenv->GetFieldID(cls, "code", "Lru/yandex/passport/tvmauth/ClientStatus$Code;"),
            value);
    }

    jobject BuildJavaObject(JNIEnv* jenv, const NTvmAuth::TCheckedServiceTicket& ticket) {
        jclass cls = jenv->FindClass("ru/yandex/passport/tvmauth/CheckedServiceTicket");
        jobject res = jenv->AllocObject(cls);

        const NTvmAuth::ETicketStatus status = ticket.GetStatus();
        SetTicketStatus(jenv, status, cls, res);

        jenv->SetObjectField(
            res,
            jenv->GetFieldID(cls, "debugInfo", "Ljava/lang/String;"),
            jenv->NewStringUTF(ticket.DebugInfo().c_str()));

        if (status == NTvmAuth::ETicketStatus::Ok) {
            jenv->SetIntField(
                res,
                jenv->GetFieldID(cls, "src", "I"),
                ticket.GetSrc());

            TMaybe<NTvmAuth::TUid> issuerUid = ticket.GetIssuerUid();
            jenv->SetLongField(
                res,
                jenv->GetFieldID(cls, "issuerUid", "J"),
                issuerUid ? *issuerUid : 0);
        }

        return res;
    }

    jobject BuildJavaObject(JNIEnv* jenv, const NTvmAuth::TCheckedUserTicket& ticket) {
        jclass cls = jenv->FindClass("ru/yandex/passport/tvmauth/CheckedUserTicket");
        jobject res = jenv->AllocObject(cls);

        const NTvmAuth::ETicketStatus status = ticket.GetStatus();
        SetTicketStatus(jenv, status, cls, res);

        jenv->SetObjectField(
            res,
            jenv->GetFieldID(cls, "debugInfo", "Ljava/lang/String;"),
            jenv->NewStringUTF(ticket.DebugInfo().c_str()));

        if (status == NTvmAuth::ETicketStatus::Ok) {
            jenv->SetLongField(
                res,
                jenv->GetFieldID(cls, "defaultUid", "J"),
                ticket.GetDefaultUid());

            jlongArray uids = jenv->NewLongArray(ticket.GetUids().size());
            jenv->SetLongArrayRegion(uids, 0, ticket.GetUids().size(), reinterpret_cast<const jlong*>(ticket.GetUids().data()));
            jenv->SetObjectField(
                res,
                jenv->GetFieldID(cls, "uids", "[J"),
                uids);

            jobjectArray scopes = jenv->NewObjectArray(ticket.GetScopes().size(),
                                                       jenv->FindClass("java/lang/String"),
                                                       0);
            const auto& scopesVector = ticket.GetScopes();
            for (size_t idx = 0; idx < scopesVector.size(); ++idx) {
                jenv->SetObjectArrayElement(scopes, idx, jenv->NewStringUTF(scopesVector[idx].data()));
            }
            jenv->SetObjectField(
                res,
                jenv->GetFieldID(cls, "scopes", "[Ljava/lang/String;"),
                scopes);

            {
                jclass envClass = jenv->FindClass("ru/yandex/passport/tvmauth/BlackboxEnv");

                jfieldID envValue = jenv->GetStaticFieldID(
                    envClass,
                    BlackboxEnvToName(ticket.GetEnv()),
                    "Lru/yandex/passport/tvmauth/BlackboxEnv;");
                jobject value = jenv->GetStaticObjectField(envClass, envValue);

                jenv->SetObjectField(
                    res,
                    jenv->GetFieldID(cls, "env", "Lru/yandex/passport/tvmauth/BlackboxEnv;"),
                    value);
            }
        }

        return res;
    }
}
