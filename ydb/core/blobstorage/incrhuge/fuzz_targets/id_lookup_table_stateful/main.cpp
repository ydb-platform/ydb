#include <ydb/core/blobstorage/incrhuge/incrhuge_id_dict.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <map>

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    using TTable = NKikimr::NIncrHuge::TIdLookupTable<ui32, ui16, 8>;
    using TId = NKikimr::NIncrHuge::TIncrHugeBlobId;

    FuzzedDataProvider provider(data, size);
    TTable table;
    std::map<TId, ui32> model;

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, 192);
    for (size_t step = 0; step < steps && provider.remaining_bytes(); ++step) {
        const ui32 op = provider.ConsumeIntegralInRange<ui32>(0, 7);
        const TId id(provider.ConsumeIntegralInRange<ui64>(0, 511));
        const ui32 value = provider.ConsumeIntegralInRange<ui32>(0, (1u << 20) - 1);

        switch (op) {
            case 0: {
                const TId created = table.Create(ui32(value));
                Y_ABORT_UNLESS(model.emplace(created, value).second);
                break;
            }
            case 1: {
                ui32* duplicate = table.Insert(id, ui32(value), true);
                Y_ABORT_UNLESS(bool(duplicate) == model.contains(id));
                if (duplicate) {
                    Y_ABORT_UNLESS(*duplicate == model.at(id));
                } else {
                    model.emplace(id, value);
                }
                break;
            }
            case 2: {
                ui32 removed = 0;
                const bool deleted = table.Delete(id, &removed);
                const auto it = model.find(id);
                Y_ABORT_UNLESS(deleted == (it != model.end()));
                if (it != model.end()) {
                    Y_ABORT_UNLESS(removed == it->second);
                    model.erase(it);
                }
                break;
            }
            case 3:
                if (model.contains(id)) {
                    table.Replace(id, ui32(value));
                    model[id] = value;
                }
                break;
            case 4:
                if (model.contains(id)) {
                    Y_ABORT_UNLESS(table.Lookup(id) == model.at(id));
                }
                break;
            default: {
                std::map<TId, ui32> enumerated;
                table.Enumerate([&](TId itemId, const ui32& itemValue) {
                    Y_ABORT_UNLESS(enumerated.emplace(itemId, itemValue).second);
                });
                Y_ABORT_UNLESS(enumerated == model);
                Y_ABORT_UNLESS(table.GetNumPagesUsed() <= model.size());
                break;
            }
        }
    }

    return 0;
}
