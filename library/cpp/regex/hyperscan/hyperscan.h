#pragma once

#include <contrib/libs/hyperscan/src/hs.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/cpu_id.h>

namespace NHyperscan {
    using TCPUFeatures = decltype(hs_platform_info_t::cpu_features);
    constexpr TCPUFeatures CPU_FEATURES_AVX2 = HS_CPU_FEATURES_AVX2;
    constexpr TCPUFeatures CPU_FEATURES_AVX512 = HS_CPU_FEATURES_AVX512 | HS_CPU_FEATURES_AVX2;

    template<typename TNativeDeleter, TNativeDeleter NativeDeleter>
    class TDeleter {
    public:
        template<typename T>
        static void Destroy(T* ptr) {
            NativeDeleter(ptr);
        }
    };

    using TDatabase = THolder<hs_database_t, TDeleter<decltype(&hs_free_database), &hs_free_database>>;

    using TScratch = THolder<hs_scratch_t, TDeleter<decltype(&hs_free_scratch), &hs_free_scratch>>;

    class TCompileException : public yexception {
    };


    namespace NPrivate {
        enum class ERuntime {
            Core2 = 0,
            Corei7 = 1,
            AVX2 = 2,
            AVX512 = 3
        };

        ERuntime DetectCurrentRuntime();

        TCPUFeatures RuntimeCpuFeatures(ERuntime runtime);

        hs_platform_info_t MakePlatformInfo(TCPUFeatures cpuFeatures);

        struct TImpl {
            hs_error_t (*AllocScratch)(const hs_database_t* db, hs_scratch_t** scratch);

            hs_error_t (*Scan)(const hs_database_t* db, const char* data,
                                unsigned length, unsigned flags, hs_scratch_t* scratch,
                                match_event_handler onEvent, void* userCtx);

            hs_error_t (*SerializeDatabase)(const hs_database_t* db, char** bytes, size_t* serialized_length);

            hs_error_t (*DeserializeDatabase)(const char* bytes, size_t length, hs_database_t** info);

            TImpl() : TImpl(DetectCurrentRuntime()) {}

            explicit TImpl(ERuntime runtime);
        };

        TDatabase Compile(const TStringBuf& regex, unsigned int flags, hs_platform_info_t* platform);

        TDatabase CompileMulti(
            const TVector<const char*>& regexs,
            const TVector<unsigned int>& flags,
            const TVector<unsigned int>& ids,
            hs_platform_info_t* platform,
            const TVector<const hs_expr_ext_t*>* extendedParameters = nullptr);

        // We need to parametrize Scan and Matches functions for testing purposes
        template<typename TCallback>
        void Scan(
            const TDatabase& db,
            const TScratch& scratch,
            const TStringBuf& text,
            TCallback& callback, // applied to index of matched regex
            const TImpl& impl
        ) {
            struct TCallbackWrapper {
                static int EventHandler(
                    unsigned int id,
                    unsigned long long from,
                    unsigned long long to,
                    unsigned int flags,
                    void* ctx) {
                    Y_UNUSED(flags);
                    TCallback& callback2 = *reinterpret_cast<TCallback*>(ctx);
                    if constexpr (std::is_same_v<int, std::invoke_result_t<TCallback, unsigned int, unsigned long long, unsigned long long>>) {
                        return callback2(id, from, to);
                    } else {
                        callback2(id, from, to);
                        return 0;
                    }
                }
            };
            unsigned int flags = 0; // unused at present
            hs_error_t status = impl.Scan(
                db.Get(),
                text.begin(),
                text.size(),
                flags,
                scratch.Get(),
                &TCallbackWrapper::EventHandler,
                &callback);
            if (status != HS_SUCCESS && status != HS_SCAN_TERMINATED) {
                ythrow yexception() << "Failed to scan against text: " << text;
            }
        }

        bool Matches(
            const TDatabase& db,
            const TScratch& scratch,
            const TStringBuf& text,
            const TImpl& impl);
    }

    TDatabase Compile(const TStringBuf& regex, unsigned int flags);

    TDatabase Compile(const TStringBuf& regex, unsigned int flags, TCPUFeatures cpuFeatures);

    TDatabase CompileMulti(
        const TVector<const char*>& regexs,
        const TVector<unsigned int>& flags,
        const TVector<unsigned int>& ids,
        const TVector<const hs_expr_ext_t*>* extendedParameters = nullptr);

    TDatabase CompileMulti(
        const TVector<const char*>& regexs,
        const TVector<unsigned int>& flags,
        const TVector<unsigned int>& ids,
        TCPUFeatures cpuFeatures,
        const TVector<const hs_expr_ext_t*>* extendedParameters = nullptr);

    TScratch MakeScratch(const TDatabase& db);

    void GrowScratch(TScratch& scratch, const TDatabase& db);

    TScratch CloneScratch(const TScratch& scratch);

    template<typename TCallback>
    void Scan(
        const TDatabase& db,
        const TScratch& scratch,
        const TStringBuf& text,
        TCallback& callback // applied to index of matched regex
    ) {
        NPrivate::Scan<TCallback>(db, scratch, text, callback, *Singleton<NPrivate::TImpl>());
    }

    bool Matches(
        const TDatabase& db,
        const TScratch& scratch,
        const TStringBuf& text);

    TString Serialize(const TDatabase& db);

    TDatabase Deserialize(const TStringBuf& serialization);
}
