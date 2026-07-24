#include <ydb/core/discovery/discovery.h>
#include <ydb/core/protos/statestorage.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <cmath>

namespace {

using namespace NKikimr;
using namespace NKikimr::NDiscovery;

constexpr ui32 MaxEntries = 24;
constexpr ui32 MaxServices = 4;

TString SmallAscii(FuzzedDataProvider& provider, TStringBuf fallback, size_t maxLen = 12) {
    TString out = provider.ConsumeRandomLengthString(provider.ConsumeIntegralInRange<size_t>(0, maxLen));
    for (char& ch : out) {
        const ui8 value = static_cast<ui8>(ch);
        ch = char('a' + value % 26);
    }
    if (out.empty()) {
        out = fallback;
    }
    return out;
}

TString Location(FuzzedDataProvider& provider) {
    TString out = SmallAscii(provider, "dc", 8);
    if (provider.ConsumeIntegralInRange<ui8>(0, 7) == 0) {
        out.push_back(char(0x80 | provider.ConsumeIntegralInRange<ui8>(0, 0x7f)));
    }
    return out;
}

struct TExpectedEndpoint {
    float Load = 0;
    THashSet<TString> Services;
    THashSet<TString> SafeLocations;
};

using TEndpointKey = std::pair<TString, ui32>;
using TExpectedMap = THashMap<TEndpointKey, TExpectedEndpoint>;

bool SafeLocation(TStringBuf value) {
    for (char ch : value) {
        if (static_cast<ui8>(ch) >= 0x80 || ch == '/') {
            return false;
        }
    }
    return true;
}

bool MatchesServices(const TSet<TString>& requested, const NKikimrStateStorage::TEndpointBoardEntry& entry) {
    if (requested.empty()) {
        return true;
    }
    for (const auto& service : entry.GetServices()) {
        if (requested.contains(service)) {
            return true;
        }
    }
    return false;
}

bool MatchesEndpointId(const TString& requested, const NKikimrStateStorage::TEndpointBoardEntry& entry) {
    if (requested.empty() && !entry.HasEndpointId()) {
        return true;
    }
    return entry.HasEndpointId() && entry.GetEndpointId() == requested;
}

void AddExpected(
        TExpectedMap& expected,
        const NKikimrStateStorage::TEndpointBoardEntry& entry,
        const TString& endpointId,
        const TSet<TString>& requestedServices) {
    if (!MatchesServices(requestedServices, entry) || !MatchesEndpointId(endpointId, entry)) {
        return;
    }

    auto& item = expected[TEndpointKey(entry.GetAddress(), entry.GetPort())];
    item.Load += entry.GetLoad();
    for (const auto& service : entry.GetServices()) {
        item.Services.insert(service);
    }
    if (SafeLocation(entry.GetDataCenter())) {
        item.SafeLocations.insert(entry.GetDataCenter());
    }
}

Ydb::Discovery::ListEndpointsResult ParseCachedResult(const TString& bytes) {
    Ydb::Discovery::ListEndpointsResponse response;
    Y_ABORT_UNLESS(response.ParseFromString(bytes));
    Y_ABORT_UNLESS(response.operation().ready());
    Y_ABORT_UNLESS(response.operation().status() == Ydb::StatusIds::SUCCESS);

    Ydb::Discovery::ListEndpointsResult result;
    Y_ABORT_UNLESS(response.operation().result().UnpackTo(&result));
    return result;
}

THashSet<TString> SplitLocations(TStringBuf value) {
    THashSet<TString> out;
    TString current;
    for (char ch : value) {
        if (ch == '/') {
            if (!current.empty()) {
                out.insert(current);
                current.clear();
            }
        } else {
            current.push_back(ch);
        }
    }
    if (!current.empty()) {
        out.insert(current);
    }
    return out;
}

void CheckResult(const Ydb::Discovery::ListEndpointsResult& result, const TExpectedMap& expected, bool ssl) {
    Y_ABORT_UNLESS(static_cast<size_t>(result.endpoints_size()) == expected.size());

    THashSet<TEndpointKey> seen;
    for (const auto& endpoint : result.endpoints()) {
        const TEndpointKey key(endpoint.address(), endpoint.port());
        Y_ABORT_UNLESS(seen.insert(key).second);
        const auto it = expected.find(key);
        Y_ABORT_UNLESS(it != expected.end());

        Y_ABORT_UNLESS(endpoint.ssl() == ssl);
        Y_ABORT_UNLESS(std::fabs(endpoint.load_factor() - it->second.Load) < 0.01f);

        THashSet<TString> services(endpoint.service().begin(), endpoint.service().end());
        Y_ABORT_UNLESS(services == it->second.Services);

        Y_ABORT_UNLESS(SplitLocations(endpoint.location()) == it->second.SafeLocations);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    TMap<NActors::TActorId, TEvStateStorage::TBoardInfoEntry> entries;
    TVector<NKikimrStateStorage::TEndpointBoardEntry> generated;

    const ui32 count = provider.ConsumeIntegralInRange<ui32>(0, MaxEntries);
    generated.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        NKikimrStateStorage::TEndpointBoardEntry entry;
        entry.SetAddress(SmallAscii(provider, "host", 10));
        entry.SetPort(provider.ConsumeIntegralInRange<ui32>(1, 65535));
        entry.SetSsl(provider.ConsumeBool());
        entry.SetLoad(provider.ConsumeFloatingPointInRange<float>(0.0f, 1000.0f));
        entry.SetNodeId(provider.ConsumeIntegralInRange<ui32>(1, 1000000));
        entry.SetDataCenter(Location(provider));
        if (provider.ConsumeBool()) {
            entry.SetEndpointId(SmallAscii(provider, "endpoint", 8));
        }

        const ui32 services = provider.ConsumeIntegralInRange<ui32>(0, MaxServices);
        for (ui32 j = 0; j < services; ++j) {
            entry.AddServices(SmallAscii(provider, "svc", 8));
        }

        TString payload;
        Y_PROTOBUF_SUPPRESS_NODISCARD entry.SerializeToString(&payload);
        entries.emplace(NActors::TActorId(1, 0, i + 1, 0), TEvStateStorage::TBoardInfoEntry{payload, false});
        generated.push_back(std::move(entry));
    }

    TString endpointId;
    if (provider.ConsumeBool() && !generated.empty()) {
        const auto& entry = generated[provider.ConsumeIntegralInRange<size_t>(0, generated.size() - 1)];
        endpointId = entry.HasEndpointId() ? entry.GetEndpointId() : SmallAscii(provider, "endpoint", 8);
    } else if (provider.ConsumeBool()) {
        endpointId = SmallAscii(provider, "endpoint", 8);
    }

    TSet<TString> requestedServices;
    const ui32 requestedServiceCount = provider.ConsumeIntegralInRange<ui32>(0, MaxServices);
    for (ui32 i = 0; i < requestedServiceCount; ++i) {
        if (provider.ConsumeBool() && !generated.empty() && generated.front().ServicesSize()) {
            const auto& entry = generated[provider.ConsumeIntegralInRange<size_t>(0, generated.size() - 1)];
            if (entry.ServicesSize()) {
                requestedServices.insert(entry.GetServices(provider.ConsumeIntegralInRange<int>(0, entry.ServicesSize() - 1)));
                continue;
            }
        }
        requestedServices.insert(SmallAscii(provider, "svc", 8));
    }

    TExpectedMap expectedPlain;
    TExpectedMap expectedSsl;
    for (const auto& entry : generated) {
        AddExpected(entry.GetSsl() ? expectedSsl : expectedPlain, entry, endpointId, requestedServices);
    }

    THolder<TEvInterconnect::TEvNodeInfo> nameservice(new TEvInterconnect::TEvNodeInfo(1));
    TCachedMessageData cache(std::move(entries), nameservice, nullptr, endpointId, requestedServices);
    THolder<TEvDiscovery::TEvDiscoveryData> event(cache.ToEvent(true));
    Y_ABORT_UNLESS(!event->CachedMessage.empty());
    Y_ABORT_UNLESS(!event->CachedMessageSsl.empty());

    CheckResult(ParseCachedResult(event->CachedMessage), expectedPlain, false);
    CheckResult(ParseCachedResult(event->CachedMessageSsl), expectedSsl, true);

    THolder<TEvDiscovery::TEvDiscoveryData> rawEvent(cache.ToEvent(false));
    Y_ABORT_UNLESS(rawEvent->CachedMessage.empty());
    Y_ABORT_UNLESS(rawEvent->CachedMessageSsl.empty());
    Y_ABORT_UNLESS(rawEvent->InfoEntries.size() == generated.size());

    return 0;
}
