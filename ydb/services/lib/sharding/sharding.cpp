#include "sharding.h"

#include <vector>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/string/printf.h>
#include <library/cpp/digest/md5/md5.h>

namespace NKikimr::NDataStreams::V1 {

    namespace {

        NYql::NDecimal::TUint128 Uint128FromString(const TString& bytes, ui32 base = 10) {
            Y_ABORT_UNLESS(base == 10 || base == 16);
            NYql::NDecimal::TUint128 x = 1;
            NYql::NDecimal::TUint128 res = 0;
            for (auto it = bytes.rbegin(); it != bytes.rend(); ++it) {
                if (!((*it >= '0' && *it <='9') || (*it >= 'a' && *it <= 'z') || (*it >= 'A' && *it <= 'Z')))
                    ythrow yexception() << "invalid character '" << *it << "'";
                ui32 v = (*it >= '0' && *it <= '9') ? (*it - '0') : (( *it >= 'a' && *it <= 'z') ? (*it - 'a' + 10) : (*it - 'A' + 10));
                if (v >= base)
                    ythrow yexception() << "string is not valid Uint128";
                res += x * v;
                x = base == 16 ? x << 4 : x * 10;
            }
            return res;
        }

        ui32 ShardFromUint128(NYql::NDecimal::TUint128 value, NYql::NDecimal::TUint128 totalShardsCount) {
            NYql::NDecimal::TUint128 max = -1;
            NYql::NDecimal::TUint128 sliceSize = max / totalShardsCount;
            NYql::NDecimal::TUint128 shard = value / sliceSize;

            return shard >= totalShardsCount ? (ui32)(totalShardsCount - 1) : (ui32)shard;
        }

    }

    TString Uint128ToDecimalString(NYql::NDecimal::TUint128 value, const NYql::NDecimal::TUint128& base) {
        std::vector<char> result;
        while (value != 0) {
            result.push_back((char)('0' + ui32(value % base)));
            value = value / base;
        }
        std::reverse(result.begin(), result.end());
        return result.size() > 0 ? TString(result.begin(), result.end()) : "0";
    }

    TString GetShardName(ui32 index) {
        return Sprintf("shard-%06d", index);
    }

    NYql::NDecimal::TUint128 BytesToDecimal(const TString& bytes) {
        return Uint128FromString(bytes, 10);
    }

    bool IsValidDecimal(const TString& bytes) {
        if (bytes.empty())
            return false;
        if (bytes.size() > 1 && (bytes[0] < '1' || bytes[0] > '9'))
            return false;

        static const TString UI128_MAX = "340282366920938463463374607431768211455";
        if (bytes.size() > UI128_MAX.size() || (bytes.size() == UI128_MAX.size() && bytes > UI128_MAX)) {
            return false;
        }
        for (auto& c : bytes) {
            if (c < '0' || c > '9')
                return false;
        }
        return true;
    }

    NYql::NDecimal::TUint128 HexBytesToDecimal(const TString& hex) {
        return Uint128FromString(hex, 16);
    }

    ui32 ShardFromDecimal(const NYql::NDecimal::TUint128& decimal, ui32 totalShardsCount) {
        return ShardFromUint128(decimal, totalShardsCount);
    }

    THashKeyRange RangeFromShardNumber(ui32 shardNumber, ui32 shardCount) {
        Y_ENSURE(shardNumber < shardCount);

        NYql::NDecimal::TUint128 max = -1;
        if (shardCount == 1) {
            return {0, max};
        }
        NYql::NDecimal::TUint128 slice = max / shardCount;
        NYql::NDecimal::TUint128 left = NYql::NDecimal::TUint128(shardNumber) * slice;
        NYql::NDecimal::TUint128 right =
                shardNumber + 1 == shardCount ? max : NYql::NDecimal::TUint128(shardNumber + 1) * slice -
                                                      NYql::NDecimal::TUint128(1);
        return {left, right};
    }

    ui32 CalculateShardFromSrcId(const TString& sourceId, ui32 partitionToTablet) {
        return ShardFromDecimal(HexBytesToDecimal(MD5::Calc(sourceId)), partitionToTablet);
    }
}
