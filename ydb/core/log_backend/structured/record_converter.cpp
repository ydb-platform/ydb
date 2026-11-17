#include "record_converter.h"

#include <ydb/library/actors/core/log_structured_keys.h>

#include <library/cpp/logger/record.h>
#include <util/charset/utf8.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NKikimr {
namespace {

class TStructuredLogRecordConverter : public NUnifiedAgent::IRecordConverter {
public:
    NUnifiedAgent::TClientMessage Convert(const TLogRecord& record) const override {
        using namespace NActors::NLog::NStructured;
        bool hasTimestamp = false;
        ui64 timestampUs = 0;
        THashMap<TString, TString> meta;
        meta.reserve(record.MetaFlags.size());
        for (const auto& [key, value] : record.MetaFlags) {
            if (key == TimestampKey) {
                hasTimestamp = TryFromString(value, timestampUs);
            }
            // UA client drops a message whose meta is not valid UTF-8.
            meta[key] = IsUtf(value) ? value : EscapeC(value);
        }
        if (!hasTimestamp) {
            return DefaultConverter->Convert(record);
        }
        const TInstant timestamp = TInstant::MicroSeconds(timestampUs);
        // UA config rebuilds the logger's leading UTC time token in the legacy kikimr format.
        TStringBuf payload(record.Data, record.Len);
        payload.SkipPrefix(timestamp.ToString() + ' ');
        return {TString(payload), std::move(meta), timestamp};
    }

private:
    const THolder<NUnifiedAgent::IRecordConverter> DefaultConverter = NUnifiedAgent::MakeDefaultRecordConverter();
};

} // namespace

THolder<NUnifiedAgent::IRecordConverter> MakeStructuredLogRecordConverter() {
    return MakeHolder<TStructuredLogRecordConverter>();
}

} // namespace NKikimr
