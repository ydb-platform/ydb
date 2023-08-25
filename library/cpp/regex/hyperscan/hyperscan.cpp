#include "hyperscan.h"

#include <contrib/libs/hyperscan/runtime_core2/hs_common.h>
#include <contrib/libs/hyperscan/runtime_core2/hs_runtime.h>
#include <contrib/libs/hyperscan/runtime_corei7/hs_common.h>
#include <contrib/libs/hyperscan/runtime_corei7/hs_runtime.h>
#include <contrib/libs/hyperscan/runtime_avx2/hs_common.h>
#include <contrib/libs/hyperscan/runtime_avx2/hs_runtime.h>
#include <contrib/libs/hyperscan/runtime_avx512/hs_common.h>
#include <contrib/libs/hyperscan/runtime_avx512/hs_runtime.h>

#include <util/generic/singleton.h>

namespace NHyperscan {
    using TSerializedDatabase = THolder<char, TDeleter<decltype(&free), &free>>;

    using TCompileError = THolder<hs_compile_error_t, TDeleter<decltype(&hs_free_compile_error), &hs_free_compile_error>>;

    namespace NPrivate {
        ERuntime DetectCurrentRuntime() {
            if (NX86::HaveAVX512F() && NX86::HaveAVX512BW()) {
                return ERuntime::AVX512;
            } else if (NX86::HaveAVX() && NX86::HaveAVX2()) {
                return ERuntime::AVX2;
            } else if (NX86::HaveSSE42() && NX86::HavePOPCNT()) {
                return ERuntime::Corei7;
            } else {
                return ERuntime::Core2;
            }
        }

        TCPUFeatures RuntimeCpuFeatures(ERuntime runtime) {
            switch (runtime) {
                default:
                    Y_ASSERT(false);
                    [[fallthrough]];
                case ERuntime::Core2:
                case ERuntime::Corei7:
                    return 0;
                case ERuntime::AVX2:
                    return CPU_FEATURES_AVX2;
                case ERuntime::AVX512:
                    return CPU_FEATURES_AVX512;
            }
        }

        hs_platform_info_t MakePlatformInfo(TCPUFeatures cpuFeatures) {
            hs_platform_info_t platformInfo{HS_TUNE_FAMILY_GENERIC, cpuFeatures, 0, 0};
            return platformInfo;
        }

        hs_platform_info_t MakeCurrentPlatformInfo() {
            return MakePlatformInfo(RuntimeCpuFeatures(DetectCurrentRuntime()));
        }

        TImpl::TImpl(ERuntime runtime) {
            switch (runtime) {
                default:
                    Y_ASSERT(false);
                    [[fallthrough]];
                case ERuntime::Core2:
                    AllocScratch = core2_hs_alloc_scratch;
                    Scan = core2_hs_scan;
                    SerializeDatabase = core2_hs_serialize_database;
                    DeserializeDatabase = core2_hs_deserialize_database;
                    break;
                case ERuntime::Corei7:
                    AllocScratch = corei7_hs_alloc_scratch;
                    Scan = corei7_hs_scan;
                    SerializeDatabase = corei7_hs_serialize_database;
                    DeserializeDatabase = corei7_hs_deserialize_database;
                    break;
                case ERuntime::AVX2:
                    AllocScratch = avx2_hs_alloc_scratch;
                    Scan = avx2_hs_scan;
                    SerializeDatabase = avx2_hs_serialize_database;
                    DeserializeDatabase = avx2_hs_deserialize_database;
                    break;
                case ERuntime::AVX512:
                    AllocScratch = avx512_hs_alloc_scratch;
                    Scan = avx512_hs_scan;
                    SerializeDatabase = avx512_hs_serialize_database;
                    DeserializeDatabase = avx512_hs_deserialize_database;
            }
        }

        TDatabase Compile(const TStringBuf& regex, unsigned int flags, hs_platform_info_t* platform) {
            hs_database_t* rawDb = nullptr;
            hs_compile_error_t* rawCompileErr = nullptr;
            hs_error_t status = hs_compile(
                    regex.begin(),
                    flags,
                    HS_MODE_BLOCK,
                    platform,
                    &rawDb,
                    &rawCompileErr);
            TDatabase db(rawDb);
            NHyperscan::TCompileError compileError(rawCompileErr);
            if (status != HS_SUCCESS) {
                ythrow TCompileException()
                        << "Failed to compile regex: " << regex << ". "
                        << "Error message (hyperscan): " << compileError->message;
            }
            return db;
        }

        TDatabase CompileMulti(
                const TVector<const char*>& regexs,
                const TVector<unsigned int>& flags,
                const TVector<unsigned int>& ids,
                hs_platform_info_t* platform,
                const TVector<const hs_expr_ext_t*>* extendedParameters) {
            unsigned int count = regexs.size();
            if (flags.size() != count) {
                ythrow yexception()
                        << "Mismatch of sizes vectors passed to CompileMulti. "
                        << "size(regexs) = " << regexs.size() << ". "
                        << "size(flags) = " << flags.size() << ".";
            }
            if (ids.size() != count) {
                ythrow yexception()
                        << "Mismatch of sizes vectors passed to CompileMulti. "
                        << "size(regexs) = " << regexs.size() << ". "
                        << "size(ids) = " << ids.size() << ".";
            }
            if (extendedParameters && extendedParameters->size() != count) {
                ythrow yexception()
                        << "Mismatch of sizes vectors passed to CompileMulti. "
                        << "size(regexs) = " << regexs.size() << ". "
                        << "size(extendedParameters) = " << extendedParameters->size() << ".";
            }
            hs_database_t* rawDb = nullptr;
            hs_compile_error_t* rawCompileErr = nullptr;
            hs_error_t status = hs_compile_ext_multi(
                    regexs.data(),
                    flags.data(),
                    ids.data(),
                    extendedParameters ? extendedParameters->data() : nullptr,
                    count,
                    HS_MODE_BLOCK,
                    platform,
                    &rawDb,
                    &rawCompileErr);
            TDatabase db(rawDb);
            NHyperscan::TCompileError compileError(rawCompileErr);
            if (status != HS_SUCCESS) {
                if (compileError->expression >= 0) {
                    const char* regex = regexs[compileError->expression];
                    ythrow TCompileException()
                            << "Failed to compile regex: " << regex << ". "
                            << "Error message (hyperscan): " << compileError->message;
                } else {
                    ythrow TCompileException()
                            << "Failed to compile multiple regexs. "
                            << "Error message (hyperscan): " << compileError->message;
                }
            }
            return db;
        }

        bool Matches(
                const TDatabase& db,
                const TScratch& scratch,
                const TStringBuf& text,
                const TImpl& impl) {
            bool result = false;
            auto callback = [&](unsigned int /* id */, unsigned long long /* from */, unsigned long long /* to */) {
                result = true;
                return 1; // stop scan
            };
            Scan(
                    db,
                    scratch,
                    text,
                    callback,
                    impl);
            return result;
        }
    } // namespace NPrivate

    TDatabase Compile(const TStringBuf& regex, unsigned int flags) {
        auto platformInfo = NPrivate::MakeCurrentPlatformInfo();
        return NPrivate::Compile(regex, flags, &platformInfo);
    }

    TDatabase Compile(const TStringBuf& regex, unsigned int flags, TCPUFeatures cpuFeatures) {
        auto platformInfo = NPrivate::MakePlatformInfo(cpuFeatures);
        return NPrivate::Compile(regex, flags, &platformInfo);
    }

    TDatabase CompileMulti(
            const TVector<const char*>& regexs,
            const TVector<unsigned int>& flags,
            const TVector<unsigned int>& ids,
            const TVector<const hs_expr_ext_t*>* extendedParameters)
    {
        auto platformInfo = NPrivate::MakeCurrentPlatformInfo();
        return NPrivate::CompileMulti(regexs, flags, ids, &platformInfo, extendedParameters);
    }

    TDatabase CompileMulti(
        const TVector<const char*>& regexs,
        const TVector<unsigned int>& flags,
        const TVector<unsigned int>& ids,
        TCPUFeatures cpuFeatures,
        const TVector<const hs_expr_ext_t*>* extendedParameters)
    {
        auto platformInfo = NPrivate::MakePlatformInfo(cpuFeatures);
        return NPrivate::CompileMulti(regexs, flags, ids, &platformInfo, extendedParameters);
    }

    TScratch MakeScratch(const TDatabase& db) {
        hs_scratch_t* rawScratch = nullptr;
        hs_error_t status = Singleton<NPrivate::TImpl>()->AllocScratch(db.Get(), &rawScratch);
        NHyperscan::TScratch scratch(rawScratch);
        if (status != HS_SUCCESS) {
            ythrow yexception() << "Failed to make scratch for hyperscan database";
        }
        return scratch;
    }

    void GrowScratch(TScratch& scratch, const TDatabase& db) {
        hs_scratch_t* rawScratch = scratch.Get();
        hs_error_t status = Singleton<NPrivate::TImpl>()->AllocScratch(db.Get(), &rawScratch);
        if (rawScratch != scratch.Get()) {
            Y_UNUSED(scratch.Release()); // freed by hs_alloc_scratch
            scratch.Reset(rawScratch);
        }
        if (status != HS_SUCCESS) {
            ythrow yexception() << "Failed to make grow scratch for hyperscan database";
        }
    }

    TScratch CloneScratch(const TScratch& scratch) {
        hs_scratch_t* rawScratch = nullptr;
        hs_error_t status = hs_clone_scratch(scratch.Get(), &rawScratch);
        TScratch scratchCopy(rawScratch);
        if (status != HS_SUCCESS) {
            ythrow yexception() << "Failed to clone scratch for hyperscan database";
        }
        return scratchCopy;
    }

    bool Matches(
        const TDatabase& db,
        const TScratch& scratch,
        const TStringBuf& text)
    {
        return NPrivate::Matches(db, scratch, text, *Singleton<NPrivate::TImpl>());
    }

    TString Serialize(const TDatabase& db) {
        char* databaseBytes = nullptr;
        size_t databaseLength;
        hs_error_t status = Singleton<NPrivate::TImpl>()->SerializeDatabase(
            db.Get(),
            &databaseBytes,
            &databaseLength);
        TSerializedDatabase serialization(databaseBytes);
        if (status != HS_SUCCESS) {
            ythrow yexception() << "Failed to serialize hyperscan database";
        }
        return TString(serialization.Get(), databaseLength);
    }

    TDatabase Deserialize(const TStringBuf& serialization) {
        hs_database_t* rawDb = nullptr;
        hs_error_t status = Singleton<NPrivate::TImpl>()->DeserializeDatabase(
            serialization.begin(),
            serialization.size(),
            &rawDb);
        TDatabase db(rawDb);
        if (status != HS_SUCCESS) {
            if (status == HS_DB_PLATFORM_ERROR) {
                ythrow yexception() << "Serialized Hyperscan database is incompatible with current CPU";
            } else if (status == HS_DB_VERSION_ERROR) {
                ythrow yexception() << "Need recreate Hyperscan database with new version Hyperscan";
            } else {
                ythrow yexception() << "Failed to deserialize hyperscan database (status = " << status << ")";
            }
        }
        return db;
    }
}
