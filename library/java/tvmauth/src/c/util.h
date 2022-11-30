#pragma once

#include <library/cpp/tvmauth/checked_service_ticket.h>
#include <library/cpp/tvmauth/checked_user_ticket.h>
#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/client/client_status.h>
#include <library/cpp/tvmauth/client/exception.h>
#include <library/cpp/tvmauth/client/logger.h>

#include <util/generic/vector.h>
#include <util/thread/lfqueue.h>

#include <exception>

#include <jni.h>

namespace NTvmAuthJava {
    template <class T>
    auto CatchAndRethrowExceptions(JNIEnv* jenv, T lambda) -> decltype(lambda()) {
        using namespace NTvmAuth;

        try {
            return lambda();
        } catch (const TEmptyTvmKeysException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/EmptyTvmKeysException"),
                           ex.what());
        } catch (const TMalformedTvmKeysException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/MalformedTvmKeysException"),
                           ex.what());
        } catch (const TMalformedTvmSecretException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/MalformedTvmSecretException"),
                           ex.what());
        } catch (const TNotAllowedException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/NotAllowedException"),
                           ex.what());
        } catch (const TPermissionDenied& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/PermissionDenied"),
                           ex.what());
        } catch (const TMissingServiceTicket& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/MissingServiceTicket"),
                           ex.what());
        } catch (const TBrokenTvmClientSettings& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/BrokenTvmClientSettings"),
                           ex.what());
        } catch (const TNonRetriableException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/NonRetriableException"),
                           ex.what());
        } catch (const TRetriableException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/RetriableException"),
                           ex.what());
        } catch (const TClientException& ex) {
            jenv->ThrowNew(jenv->FindClass("ru/yandex/passport/tvmauth/exception/ClientException"),
                           ex.what());
        } catch (const std::exception& ex) {
            jenv->ThrowNew(jenv->FindClass("java/lang/Exception"),
                           ex.what());
        } catch (...) {
            jenv->ThrowNew(jenv->FindClass("java/lang/Exception"),
                           "Unknown exception");
        }
        return decltype(lambda())();
    }

    class TJavaString {
    public:
        TJavaString(JNIEnv* jenv, jstring orig)
            : jenv_(jenv)
            , orig_(orig)
            , k_(jenv_->GetStringUTFChars(orig_, nullptr))
        {
        }

        ~TJavaString() {
            jenv_->ReleaseStringUTFChars(orig_, k_);
        }

        operator TStringBuf() const {
            return TStringBuf(k_, jenv_->GetStringUTFLength(orig_));
        }

    private:
        JNIEnv* jenv_;
        jstring orig_;
        const char* k_;
    };

    class TJavaLogger: public NTvmAuth::ILogger {
    public:
        TJavaLogger() {
            Ref(); // to avoid deletion by intrusive ptr
        }

        using TMessage = std::pair<int, TString>;

        void Log(int lvl, const TString& msg) override {
            queue_.Enqueue(TMessage{lvl, msg});
        }

        TVector<TMessage> FetchMessages() {
            TVector<TMessage> res;
            queue_.DequeueAll(&res);
            return res;
        }

    private:
        TLockFreeQueue<TMessage> queue_;
    };

    jobject BuildJavaObject(JNIEnv* jenv, const NTvmAuth::TCheckedServiceTicket& ticket);
    jobject BuildJavaObject(JNIEnv* jenv, const NTvmAuth::TCheckedUserTicket& ticket);

    void SetTicketStatus(JNIEnv* jenv, const NTvmAuth::ETicketStatus status, jclass cls, jobject res);
    void SetClientStatus(JNIEnv* jenv, const NTvmAuth::TClientStatus::ECode status, jclass cls, jobject res);
}
