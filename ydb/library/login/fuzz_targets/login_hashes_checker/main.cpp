#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <ydb/library/login/hashes_checker/hashes_checker.h>

namespace {

TString ConsumeBase64(FuzzedDataProvider& fdp, size_t maxLen) {
    const size_t size = fdp.ConsumeIntegralInRange<size_t>(0, maxLen);
    const auto bytes = fdp.ConsumeBytes<uint8_t>(size);
    return Base64Encode(TStringBuf(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
}

TString BuildOldHash(FuzzedDataProvider& fdp) {
    if (fdp.ConsumeBool()) {
        return NAuthSecurityFuzz::ValidOldArgonHash();
    }

    const TString type = fdp.ConsumeBool() ? TString("argon2id") : NAuthSecurityFuzz::ConsumeToken(fdp, 12);
    const TString salt = fdp.ConsumeBool() ? ConsumeBase64(fdp, 24) : NAuthSecurityFuzz::ConsumeToken(fdp, 32);
    const TString hash = fdp.ConsumeBool() ? ConsumeBase64(fdp, 48) : NAuthSecurityFuzz::ConsumeToken(fdp, 64);

    TStringBuilder builder;
    builder << "{\"type\":\"" << type << "\",\"salt\":\"" << salt << "\",\"hash\":\"" << hash << "\"";
    if (fdp.ConsumeBool()) {
        builder << ",\"" << NAuthSecurityFuzz::ConsumeToken(fdp, 8) << "\":\"" << NAuthSecurityFuzz::ConsumeToken(fdp, 16) << "\"";
    }
    builder << "}";
    return builder;
}

TString BuildNewHashesJson(FuzzedDataProvider& fdp) {
    if (fdp.ConsumeBool()) {
        return NAuthSecurityFuzz::ValidHashesJson();
    }

    const TString argon = fdp.ConsumeBool()
        ? TStringBuilder() << ConsumeBase64(fdp, 24) << "$" << ConsumeBase64(fdp, 48)
        : NAuthSecurityFuzz::ConsumeToken(fdp, 96);
    const TString scram = TStringBuilder()
        << fdp.ConsumeIntegralInRange<ui32>(0, 8192) << ':'
        << (fdp.ConsumeBool() ? ConsumeBase64(fdp, 24) : NAuthSecurityFuzz::ConsumeToken(fdp, 24))
        << '$'
        << (fdp.ConsumeBool() ? ConsumeBase64(fdp, 48) : NAuthSecurityFuzz::ConsumeToken(fdp, 48))
        << ':'
        << (fdp.ConsumeBool() ? ConsumeBase64(fdp, 48) : NAuthSecurityFuzz::ConsumeToken(fdp, 48));

    TStringBuilder builder;
    builder << "{\"version\":";
    if (fdp.ConsumeBool()) {
        builder << 1;
    } else {
        builder << fdp.ConsumeIntegral<ui32>();
    }
    builder << ",\"argon2id\":\"" << argon << "\"";
    if (fdp.ConsumeBool()) {
        builder << ",\"scram-sha-256\":\"" << scram << "\"";
    }
    if (fdp.ConsumeBool()) {
        builder << ",\"" << NAuthSecurityFuzz::ConsumeToken(fdp, 8) << "\":\"" << NAuthSecurityFuzz::ConsumeToken(fdp, 32) << "\"";
    }
    builder << "}";
    return builder;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TString raw(reinterpret_cast<const char*>(data), size);
    FuzzedDataProvider fdp(data, size);

    try {
        (void)NLogin::SplitPasswordHash(raw);
    } catch (...) {
    }
    try {
        (void)NLogin::ParseArgonHash(raw);
    } catch (...) {
    }
    try {
        (void)NLogin::ParseScramHashInitParams(raw);
    } catch (...) {
    }
    try {
        (void)NLogin::ParseScramHashValues(raw);
    } catch (...) {
    }
    try {
        (void)NLogin::ParseScramHash(raw);
    } catch (...) {
    }
    try {
        if (NLogin::IsOldFormatHash(raw)) {
            const TString converted = NLogin::ConvertOldFormatHash(raw);
            (void)NLogin::THashesChecker::NewFormatCheck(converted);
            (void)NLogin::MakePasswordHashesMap(converted);
        }
    } catch (...) {
    }
    try {
        (void)NLogin::THashesChecker::OldFormatCheck(raw);
    } catch (...) {
    }
    try {
        (void)NLogin::THashesChecker::NewFormatCheck(raw);
    } catch (...) {
    }
    try {
        (void)NLogin::MakePasswordHashesMap(raw);
    } catch (...) {
    }

    const TString oldHash = BuildOldHash(fdp);
    try {
        (void)NLogin::THashesChecker::OldFormatCheck(oldHash);
    } catch (...) {
    }
    try {
        if (NLogin::IsOldFormatHash(oldHash)) {
            const TString converted = NLogin::ConvertOldFormatHash(oldHash);
            (void)NLogin::THashesChecker::NewFormatCheck(converted);
            (void)NLogin::MakePasswordHashesMap(converted);
        }
    } catch (...) {
    }

    const TString newHashesJson = BuildNewHashesJson(fdp);
    const TString newHashesBase64 = Base64Encode(newHashesJson);
    try {
        (void)NLogin::THashesChecker::NewFormatCheck(newHashesBase64);
    } catch (...) {
    }
    try {
        (void)NLogin::MakePasswordHashesMap(newHashesBase64);
    } catch (...) {
    }

    return 0;
}
