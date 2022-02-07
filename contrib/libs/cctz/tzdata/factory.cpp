#include <contrib/libs/cctz/include/cctz/zone_info_source.h>

#include <library/cpp/resource/resource.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace cctz_extension {
    namespace {

        class TZoneInfoSource: public cctz::ZoneInfoSource {
        private:
            TZoneInfoSource(TString&& data)
                : TimeZoneData_(data)
                , Stream_(TimeZoneData_)
            {
            }

        public:
            static std::unique_ptr<cctz::ZoneInfoSource> LoadZone(const std::string& zoneName) {
                TString resourceName = TStringBuilder() << "/cctz/tzdata/"sv << zoneName;
                TString tzData;
                if (!NResource::FindExact(resourceName, &tzData)) {
                    return nullptr;
                }
                return std::unique_ptr<cctz::ZoneInfoSource>(new TZoneInfoSource(std::move(tzData)));
            }

            size_t Read(void* buf, size_t size) override {
                return Stream_.Read(buf, size);
            }

            int Skip(size_t offset) override {
                Stream_.Skip(offset);
                return 0;
            }

        private:
            const TString TimeZoneData_;
            TStringInput Stream_;
        };

        std::unique_ptr<cctz::ZoneInfoSource> CustomFactory(
            const std::string& name,
            const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string& name)>& /*fallback*/
        )
        {
            std::unique_ptr<cctz::ZoneInfoSource> zis = TZoneInfoSource::LoadZone(name);
            if (zis) {
                return zis;
            }
            return nullptr;
        }

    } // namespace

    ZoneInfoSourceFactory zone_info_source_factory = CustomFactory;

} // namespace cctz_extension
