#include "load_actor_impl.h"
#include "scheme.h"

namespace NKikimr::NTestShard {

    void TLoadActor::GenerateKeyValue(TString *key, TString *value, bool *isInline) {
        const size_t len = GenerateRandomSize(Settings.GetSizes(), isInline);
        const ui64 id = RandomNumber<ui64>();
        const ui64 seed = RandomNumber<ui64>();
        *key = TStringBuilder() << len << ',' << seed << ',' << id;
        *value = FastGenDataForLZ4(len, seed);
    }

    void TLoadActor::IssueWrite() {
        TString key, value;
        bool isInline;
        do {
            GenerateKeyValue(&key, &value, &isInline);
        } while (Keys.count(key));

        auto ev = CreateRequest();
        auto& r = ev->Record;
        auto *write = r.AddCmdWrite();
        write->SetKey(key);
        if (RandomNumber(2u)) {
            write->SetPayloadId(ev->AddPayload(TRope(value)));
        } else {
            write->SetValue(value);
        }
        if (isInline) {
            write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        }

        STLOG(PRI_INFO, TEST_SHARD, TS12, "writing data", (TabletId, TabletId), (Key, key), (Size, value.size()));

        NWilson::TTraceId traceId;
        if (RandomNumber(1000000u) < Settings.GetPutTraceFractionPPM()) {
            traceId = NWilson::TTraceId::NewTraceId(Settings.GetPutTraceVerbosity(), Max<ui32>());
        }

        auto [wifIt, wifInserted] = WritesInFlight.try_emplace(r.GetCookie(), key);
        Y_ABORT_UNLESS(wifInserted);
        Y_ABORT_UNLESS(wifIt->second.KeysInQuery.size() == 1);

        auto [it, inserted] = Keys.try_emplace(key, value.size());
        Y_ABORT_UNLESS(inserted);
        RegisterTransition(*it, ::NTestShard::TStateServer::ABSENT, ::NTestShard::TStateServer::WRITE_PENDING,
            std::move(ev), std::move(traceId));

        ++KeysWritten;
        BytesProcessed += value.size();
    }

    void TLoadActor::IssuePatch() {
        Y_ABORT_UNLESS(!ConfirmedKeys.empty());
        const size_t index = RandomNumber(ConfirmedKeys.size());
        const TString originalKey = ConfirmedKeys[index];

        // extract length from the original key -- it may not change
        ui64 len, seed, id;
        StringSplitter(originalKey).Split(',').CollectInto(&len, &seed, &id);
        TString originalValue = FastGenDataForLZ4(len, seed);

        // generate patched key
        seed = RandomNumber<ui64>();
        id = RandomNumber<ui64>();
        const TString patchedKey = TStringBuilder() << len << ',' << seed << ',' << id;

        // generate random value for the new key
        TString patchedValue = FastGenDataForLZ4(len, seed);

        auto ev = CreateRequest();
        auto& r = ev->Record;
        auto *patch = r.AddCmdPatch();
        patch->SetOriginalKey(originalKey);
        patch->SetPatchedKey(patchedKey);

        TRope rope(patchedValue);
        ui64 offset = 0;
        for (size_t chunks = 0; offset != len; ++chunks) {
            // skip matching parts
            while (offset + 1 < len && originalValue[offset] == patchedValue[offset]) {
                ++offset;
            }
            Y_ABORT_UNLESS(offset < len);

            // add patched part
            size_t pos = offset + 1;
            while (pos < len && originalValue[pos] != patchedValue[pos]) {
                ++pos;
            }
            const size_t size = (chunks < 8 ? pos : len) - offset;

            auto *diff = patch->AddDiffs();
            diff->SetOffset(offset);
            if (RandomNumber(2u)) {
                diff->SetPayloadId(ev->AddPayload(TRope(rope.Position(offset), rope.Position(offset + size))));
            } else {
                diff->SetValue(patchedValue.substr(offset, size));
            }
            offset += size;
        }

        auto [pifIt, pifInserted] = PatchesInFlight.try_emplace(r.GetCookie(), patchedKey);
        Y_ABORT_UNLESS(pifInserted);

        auto [it, inserted] = Keys.try_emplace(patchedKey, len);
        Y_ABORT_UNLESS(inserted);
        RegisterTransition(*it, ::NTestShard::TStateServer::ABSENT, ::NTestShard::TStateServer::WRITE_PENDING, std::move(ev));

        ++KeysWritten;
        BytesProcessed += len;
    }

    void TLoadActor::ProcessWriteResult(ui64 cookie, const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TWriteResult>& results) {
        if (const auto wifIt = WritesInFlight.find(cookie); wifIt != WritesInFlight.end()) {
            TWriteInfo& info = wifIt->second;
            const TDuration latency = TDuration::Seconds(info.Timer.Passed());
            STLOG(PRI_DEBUG, TEST_SHARD, TS29, "data written", (TabletId, TabletId), (Key, info.KeysInQuery),
                (Latency, latency));
            WriteLatency.Add(TActivationContext::Monotonic(), latency);
            Y_ABORT_UNLESS(info.KeysInQuery.size() == (size_t)results.size(), "%zu/%d", info.KeysInQuery.size(), results.size());
            for (size_t i = 0; i < info.KeysInQuery.size(); ++i) {
                const auto& res = results[i];
                Y_VERIFY_S(res.GetStatus() == NKikimrProto::OK, "TabletId# " << TabletId << " CmdWrite failed Status# "
                    << NKikimrProto::EReplyStatus_Name(NKikimrProto::EReplyStatus(res.GetStatus())));

                const auto it = Keys.find(info.KeysInQuery[i]);
                Y_VERIFY_S(it != Keys.end(), "Key# " << info.KeysInQuery[i] << " not found in Keys dict");
                TKeyInfo& k = it->second;
                WriteSpeed.Add(TActivationContext::Now(), k.Len);

                RegisterTransition(*it, ::NTestShard::TStateServer::WRITE_PENDING, ::NTestShard::TStateServer::CONFIRMED);
            }
            WritesInFlight.erase(wifIt);
        }
    }

    void TLoadActor::ProcessPatchResult(ui64 cookie, const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TPatchResult>& results) {
        if (auto nh = PatchesInFlight.extract(cookie)) {
            Y_ABORT_UNLESS(results.size() == 1);
            const auto& res = results[0];
            Y_VERIFY_S(res.GetStatus() == NKikimrProto::OK, "TabletId# " << TabletId << " CmdPatch failed Status# "
                << NKikimrProto::EReplyStatus_Name(NKikimrProto::EReplyStatus(res.GetStatus())));
            const TString& key = nh.mapped();
            const auto it = Keys.find(key);
            Y_VERIFY_S(it != Keys.end(), "Key# " << key << " not found in Keys dict");
            RegisterTransition(*it, ::NTestShard::TStateServer::WRITE_PENDING, ::NTestShard::TStateServer::CONFIRMED);
        }
    }

} // NKikimr::NTestShard
