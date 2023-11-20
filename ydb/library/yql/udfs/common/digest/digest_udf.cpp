#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <util/digest/murmur.h>
#include <util/digest/city.h>
#include <util/digest/numeric.h>
#include <util/digest/fnv.h>

#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/digest/argonish/blake2b.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/digest/murmur/murmur.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/digest/sfh/sfh.h>

#include <contrib/libs/highwayhash/highwayhash/c_bindings.h>
#include <contrib/libs/highwayhash/highwayhash/sip_hash.h>

#include <contrib/libs/farmhash/farmhash.h>
#include <contrib/libs/xxhash/xxhash.h>

#include <openssl/sha.h>

using namespace NKikimr;
using namespace NUdf;

namespace {
    SIMPLE_STRICT_UDF(TCrc32c, ui32(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui32 hash = Crc32c(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TCrc64, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui64 hash = crc64(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TFnv32, ui32(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui32 hash = FnvHash<ui32>(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TFnv64, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui64 hash = FnvHash<ui64>(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TMurMurHash, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui64 hash = MurmurHash<ui64>(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TMurMurHash32, ui32(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui32 hash = MurmurHash<ui32>(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TMurMurHash2A, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui64 hash = TMurmurHash2A<ui64>{}.Update(inputRef.Data(), inputRef.Size()).Value();
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TMurMurHash2A32, ui32(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui32 hash = TMurmurHash2A<ui32>{}.Update(inputRef.Data(), inputRef.Size()).Value();
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TCityHash, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui64 hash = CityHash64(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    using TUi64Pair = NUdf::TTuple<ui64, ui64>;

    class TCityHash128: public TBoxedValue {
    public:
        static TStringRef Name() {
            static auto name = TStringRef::Of("CityHash128");
            return name;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                auto type = builder.Tuple(2)->Add<ui64>().Add<ui64>().Build();
                builder.Args(1)->Add<TAutoMap<char*>>();
                builder.Returns(type);
                if (!typesOnly) {
                    builder.Implementation(new TCityHash128);
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            TUnboxedValue* items = nullptr;
            auto val = valueBuilder->NewArray(2U, items);
            const auto& inputRef = args[0].AsStringRef();
            uint128 hash = CityHash128(inputRef.Data(), inputRef.Size());
            items[0] = TUnboxedValuePod(hash.first);
            items[1] = TUnboxedValuePod(hash.second);
            return val;
        }
    };

    SIMPLE_STRICT_UDF(TNumericHash, ui64(TAutoMap<ui64>)) {
        Y_UNUSED(valueBuilder);
        ui64 input = args[0].Get<ui64>();
        ui64 hash = (ui64)NumericHash(input);
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TMd5Hex, char*(TAutoMap<char*>)) {
        const auto& inputRef = args[0].AsStringRef();
        MD5 md5;
        const TString& hash = md5.Calc(inputRef);
        return valueBuilder->NewString(hash);
    }

    SIMPLE_STRICT_UDF(TMd5Raw, char*(TAutoMap<char*>)) {
        const auto& inputRef = args[0].AsStringRef();
        MD5 md5;
        const TString& hash = md5.CalcRaw(inputRef);
        return valueBuilder->NewString(hash);
    }

    SIMPLE_STRICT_UDF(TMd5HalfMix, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        return TUnboxedValuePod(MD5::CalcHalfMix(args[0].AsStringRef()));
    }

    SIMPLE_STRICT_UDF(TArgon2, char*(TAutoMap<char*>, TAutoMap<char*>)) {
        const static ui32 outSize = 32;
        const static NArgonish::TArgon2Factory afactory;
        const static THolder<NArgonish::IArgon2Base> argon2 = afactory.Create(
            NArgonish::EArgon2Type::Argon2d, 1, 32, 1);

        const TStringRef inputRef = args[0].AsStringRef();
        const TStringRef saltRef = args[1].AsStringRef();
        ui8 out[outSize];
        argon2->Hash(reinterpret_cast<const ui8*>(inputRef.Data()), inputRef.Size(),
                     reinterpret_cast<const ui8*>(saltRef.Data()), saltRef.Size(),
                     out, outSize);
        return valueBuilder->NewString(TStringRef(reinterpret_cast<char*>(&out[0]), outSize));
    }

    SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TBlake2B, char*(TAutoMap<char*>, TOptional<char*>), 1) {
        const static ui32 outSize = 32;
        const static NArgonish::TBlake2BFactory bfactory;
        const TStringRef inputRef = args[0].AsStringRef();

        THolder<NArgonish::IBlake2Base> blake2b;
        if (args[1]) {
            const TStringRef keyRef = args[1].AsStringRef();
            if (keyRef.Size() == 0) {
                blake2b = bfactory.Create(outSize);
            } else {
                blake2b = bfactory.Create(outSize, reinterpret_cast<const ui8*>(keyRef.Data()), keyRef.Size());
            }
        } else {
            blake2b = bfactory.Create(outSize);
        }

        ui8 out[outSize];
        blake2b->Update(inputRef.Data(), inputRef.Size());
        blake2b->Final(out, outSize);
        return valueBuilder->NewString(TStringRef(reinterpret_cast<char*>(&out[0]), outSize));
    }

    SIMPLE_STRICT_UDF(TSipHash, ui64(ui64, ui64, TAutoMap<char*>)) {
        using namespace highwayhash;
        Y_UNUSED(valueBuilder);
        const TStringRef inputRef = args[2].AsStringRef();
        const HH_U64 state[2] = {args[0].Get<ui64>(), args[1].Get<ui64>()};
        ui64 hash = SipHash(state, inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(THighwayHash, ui64(ui64, ui64, ui64, ui64, TAutoMap<char*>)) {
        using namespace highwayhash;
        Y_UNUSED(valueBuilder);
        const TStringRef inputRef = args[4].AsStringRef();
        const uint64_t key[4] = {
            args[0].Get<ui64>(),
            args[1].Get<ui64>(),
            args[2].Get<ui64>(),
            args[3].Get<ui64>()};
        ui64 hash = HighwayHash64(key, inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TFarmHashFingerprint, ui64(TAutoMap<ui64>)) {
        Y_UNUSED(valueBuilder);
        ui64 input = args[0].Get<ui64>();
        ui64 hash = util::Fingerprint(input);
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TFarmHashFingerprint2, ui64(TAutoMap<ui64>, TAutoMap<ui64>)) {
        Y_UNUSED(valueBuilder);
        ui64 low  = args[0].Get<ui64>();
        ui64 high = args[1].Get<ui64>();
        ui64 hash = util::Fingerprint(util::Uint128(low, high));
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TFarmHashFingerprint32, ui32(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        auto hash = util::Fingerprint32(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(ui32(hash));
    }

    SIMPLE_STRICT_UDF(TFarmHashFingerprint64, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        auto hash = util::Fingerprint64(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(ui64(hash));
    }

    class TFarmHashFingerprint128: public TBoxedValue {
    public:
        static TStringRef Name() {
            static auto name = TStringRef::Of("FarmHashFingerprint128");
            return name;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                auto type = builder.Tuple(2)->Add<ui64>().Add<ui64>().Build();
                builder.Args(1)->Add<TAutoMap<char*>>();
                builder.Returns(type);
                if (!typesOnly) {
                    builder.Implementation(new TFarmHashFingerprint128);
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            TUnboxedValue* items = nullptr;
            auto val = valueBuilder->NewArray(2U, items);
            const auto& inputRef = args[0].AsStringRef();
            auto hash = util::Fingerprint128(inputRef.Data(), inputRef.Size());
            items[0] = TUnboxedValuePod(static_cast<ui64>(hash.first));
            items[1] = TUnboxedValuePod(static_cast<ui64>(hash.second));
            return val;
        }
    };

    SIMPLE_STRICT_UDF(TSuperFastHash, ui32(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        ui32 hash = SuperFastHash(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    SIMPLE_STRICT_UDF(TSha1, char*(TAutoMap<char*>)) {
        const auto& inputRef = args[0].AsStringRef();
        SHA_CTX sha;
        SHA1_Init(&sha);
        SHA1_Update(&sha, inputRef.Data(), inputRef.Size());
        unsigned char hash[SHA_DIGEST_LENGTH];
        SHA1_Final(hash, &sha);
        return valueBuilder->NewString(TStringRef(reinterpret_cast<char*>(hash), sizeof(hash)));
    }

    SIMPLE_STRICT_UDF(TSha256, char*(TAutoMap<char*>)) {
        const auto& inputRef = args[0].AsStringRef();
        SHA256_CTX sha;
        SHA256_Init(&sha);
        SHA256_Update(&sha, inputRef.Data(), inputRef.Size());
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_Final(hash, &sha);
        return valueBuilder->NewString(TStringRef(reinterpret_cast<char*>(hash), sizeof(hash)));
    }

    SIMPLE_STRICT_UDF(TIntHash64, ui64(TAutoMap<ui64>)) {
        Y_UNUSED(valueBuilder);
        ui64 x = args[0].Get<ui64>();
        x ^= 0x4CF2D2BAAE6DA887ULL;
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return TUnboxedValuePod(x);
    }

    SIMPLE_STRICT_UDF(TXXH3, ui64(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        const ui64 hash = XXH3_64bits(inputRef.Data(), inputRef.Size());
        return TUnboxedValuePod(hash);
    }

    class TXXH3_128: public TBoxedValue {
    public:
        static TStringRef Name() {
            static auto name = TStringRef::Of("XXH3_128");
            return name;
        }

        static bool DeclareSignature(const TStringRef& name, TType*, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
            if (Name() == name) {
                const auto type = builder.Tuple(2)->Add<ui64>().Add<ui64>().Build();
                builder.Args(1)->Add<TAutoMap<char*>>();
                builder.Returns(type);
                if (!typesOnly) {
                    builder.Implementation(new TXXH3_128);
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            TUnboxedValue* items = nullptr;
            auto val = valueBuilder->NewArray(2U, items);
            const auto& inputRef = args[0].AsStringRef();
            const auto hash = XXH3_128bits(inputRef.Data(), inputRef.Size());
            items[0] = TUnboxedValuePod(ui64(hash.low64));
            items[1] = TUnboxedValuePod(ui64(hash.high64));
            return val;
        }
    };

    SIMPLE_MODULE(TDigestModule,
                  TCrc32c,
                  TCrc64,
                  TFnv32,
                  TFnv64,
                  TMurMurHash,
                  TMurMurHash32,
                  TMurMurHash2A,
                  TMurMurHash2A32,
                  TCityHash,
                  TCityHash128,
                  TNumericHash,
                  TMd5Hex,
                  TMd5Raw,
                  TMd5HalfMix,
                  TArgon2,
                  TBlake2B,
                  TSipHash,
                  THighwayHash,
                  TFarmHashFingerprint,
                  TFarmHashFingerprint2,
                  TFarmHashFingerprint32,
                  TFarmHashFingerprint64,
                  TFarmHashFingerprint128,
                  TSuperFastHash,
                  TSha1,
                  TSha256,
                  TIntHash64,
                  TXXH3,
                  TXXH3_128
    )

}

REGISTER_MODULES(TDigestModule)
