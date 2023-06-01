#pragma once
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <util/system/types.h>
#include <util/generic/string.h>

#include <util/random/random.h>

namespace NIdxTest {

class TRandomValueProvider {
public:
    TRandomValueProvider(ui8 ranges = 0, ui8 limit = 0);
    bool RandomBool() const {
        return RandomNumber<ui8>(2);
    }

    i8 RandomI8() const {
        return RandomUi8()  - 0x80;
    }

    i16 RandomI16() const {
        return RandomUi16() - 0x8000;
    }

    i32 RandomI32() const {
        return RandomUi32() - 0x80000000;
    }

    i64 RandomI64() const {
        return RandomUi64() - 0x8000000000000000;
    }

    ui8 RandomUi8() const {
        auto rnd = RandomNumber<ui8>();
        if (!RangesBits_)
            return rnd & LimitMask_;
        auto shard = RandomNumber<ui8>(Min(1 << RangesBits_, 128));
        return rnd & LimitMask_ | (shard << 8 - RangesBits_);
    }

    ui16 RandomUi16() const {
        auto rnd = RandomNumber<ui16>();
        if (!RangesBits_)
            return rnd & LimitMask_;
        auto shard = RandomNumber<ui16>(1 << RangesBits_);
        return rnd & LimitMask_ | (shard << 16 - RangesBits_);
    }

    ui32 RandomUi32() const {
        auto rnd = RandomNumber<ui32>();
        if (!RangesBits_)
            return rnd & LimitMask_;
        auto shard = RandomNumber<ui32>(1 << RangesBits_);
        return rnd & LimitMask_ | (shard << 32 - RangesBits_);
    }

    ui64 RandomUi64() const {
        auto rnd = RandomNumber<ui64>();
        if (!RangesBits_)
            return rnd & LimitMask_;
        auto shard = RandomNumber<ui64>(1 << RangesBits_);
        return rnd & LimitMask_ | (shard << 64 - RangesBits_);
    }

    float RandomFloat() const {
        return RandomDouble();
    }

    double RandomDouble() const {
        return RandomI32() / (double)(1ull << 32);
    }

    // Random binary data
    TString RandomString() const {
        ui64 random = RandomUi64();
        size_t sz = (random & 0xf) * 8;
        if (!sz)
            return {};
        auto buf = std::unique_ptr<char[]>(new char[sz]);

        for (size_t i = 0; i < sz; i += 8) {
            memcpy(buf.get() + i, (char*)(&random), 8);
        }
        buf.get()[sz - 1] = 0;

        return TString(buf.get(), sz - 1);
    }
private:
    const ui16 RangesBits_;
    const ui64 LimitMask_;

};

NYdb::TValue CreateOptionalValue(const NYdb::TColumn& column, const TRandomValueProvider& rvp);
NYdb::TValue CreateValue(const NYdb::TColumn& column, const TRandomValueProvider& rvp);
NYdb::TValue CreateRow(const TVector<NYdb::TColumn>& columns, const TRandomValueProvider& rvp);
NYdb::TParams CreateParamsAsItems(const TVector<NYdb::TValue>& values, const TVector<TString>& paramNames);
NYdb::TParams CreateParamsAsList(const TVector<NYdb::TValue>& batch, const TString& paramName);
void AddParamsAsList(NYdb::TParamsBuilder& paramsBuilder, const TVector<NYdb::TValue>& batch, const TString& paramName);

}
