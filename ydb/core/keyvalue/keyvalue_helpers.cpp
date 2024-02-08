#include "keyvalue_helpers.h"
#include "keyvalue_data_header.h"
#include "keyvalue_stored_state_data.h"
#include "keyvalue_trash_key_arbitrary.h"
#include "keyvalue_collect_operation.h"

namespace NKikimr {
namespace NKeyValue {

ui8 THelpers::Checksum(ui8 prev, size_t dataSize, const ui8* data) {
    for (size_t i = 0; i < dataSize; ++i) {
        prev ^= data[i];
    }
    return prev;
}

bool THelpers::CheckChecksum(const TString &key) {
    ui8 sum = Checksum(0, key.size(), (const ui8*)key.data());
    return (sum == 0);
}

TString THelpers::GenerateKeyFor(EItemType itemType, const ui8* data, size_t size) {
    TString key = TString::Uninitialized(sizeof(TKeyHeader) + size);
    TDataHeader header;
    header.ItemType = itemType;
    header.Checksum = Checksum(itemType, size, data);
    memcpy(const_cast<char *>(key.data()), &header, sizeof(TKeyHeader));
    memcpy(const_cast<char *>(key.data()) + sizeof(TKeyHeader), data, size);
    return key;
}

TString THelpers::GenerateKeyFor(EItemType itemType, const TString &arbitraryPart) {
    const size_t size = arbitraryPart.size();
    const ui8 *data = (const ui8 *) arbitraryPart.data();
    return GenerateKeyFor(itemType, data, size);
}

bool THelpers::ExtractKeyParts(const TString &key, TString &arbitraryPart, TKeyHeader &header) {
    if (!CheckChecksum(key)) {
        return false;
    }
    if (key.size() < sizeof(TKeyHeader)) {
        return false;
    }
    memcpy(&header, key.data(), sizeof(TKeyHeader));
    size_t arbitrarySize = key.size() - sizeof(TKeyHeader);
    arbitraryPart.clear();
    arbitraryPart.resize(arbitrarySize);
    memcpy(const_cast<char *>(arbitraryPart.data()), key.data() + sizeof(TKeyHeader), arbitrarySize);
    return true;
}

void THelpers::DbUpdateState(TKeyValueStoredStateData &state, ISimpleDb &db, const TActorContext &ctx) {
    TString empty;
    TString key = THelpers::GenerateKeyFor(EIT_STATE, empty);
    state.UpdateChecksum();
    TString value = TString::Uninitialized(sizeof(state));
    memcpy(const_cast<char*>(value.data()), &state, sizeof(state));
    db.Update(key, value, ctx);
}

void THelpers::DbEraseUserKey(const TString &userKey, ISimpleDb &db, const TActorContext &ctx) {
    TString key = THelpers::GenerateKeyFor(EIT_KEYVALUE_1, userKey);
    db.Erase(key, ctx);
}

void THelpers::DbUpdateUserKeyValue(const TString &userKey, const TString& value, ISimpleDb &db,
        const TActorContext &ctx) {
    TString key = THelpers::GenerateKeyFor(EIT_KEYVALUE_1, userKey);
    db.Update(key, value, ctx);
}

void THelpers::DbEraseTrash(const TLogoBlobID &id, ISimpleDb &db, const TActorContext &ctx) {
    TTrashKeyArbitrary arbitrary;
    arbitrary.LogoBlobId = id;
    TString key = THelpers::GenerateKeyFor(EIT_TRASH, (ui8*)&arbitrary, sizeof(TTrashKeyArbitrary));
    db.Erase(key, ctx);
}

void THelpers::DbUpdateTrash(const TLogoBlobID &id, ISimpleDb &db, const TActorContext &ctx) {
    TTrashKeyArbitrary arbitrary;
    arbitrary.LogoBlobId = id;
    TString key = THelpers::GenerateKeyFor(EIT_TRASH, (ui8*)&arbitrary, sizeof(TTrashKeyArbitrary));
    TString value;
    db.Update(key, value, ctx);
}

void THelpers::DbEraseCollect(ISimpleDb &db, const TActorContext &ctx) {
    TString empty;
    TString key = THelpers::GenerateKeyFor(EIT_COLLECT, empty);
    db.Erase(key, ctx);
}

THelpers::TGenerationStep THelpers::GenerationStep(const TLogoBlobID &id) {
    return std::make_tuple(id.Generation(), id.Step());
}

} // NKeyValue
} // NKikimr
