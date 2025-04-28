#include "schema_gateway.h"

#include <yql/essentials/sql/v1/complete/text/case.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        class TSchemaGateway: public ISchemaGateway {
            static constexpr size_t MaxLimit = 4 * 1024;

        public:
            explicit TSchemaGateway(THashMap<TString, TVector<TFolderEntry>> data)
                : Data_(std::move(data))
            {
                for (const auto& [k, _] : Data_) {
                    Y_ENSURE(k.StartsWith("/"), k << " must start with the '/'");
                    Y_ENSURE(k.EndsWith("/"), k << " must end with the '/'");
                }
            }

            NThreading::TFuture<TListResponse> List(const TListRequest& request) const override {
                auto [path, prefix] = ParsePath(request.Path);

                TVector<TFolderEntry> entries;
                if (const auto* data = Data_.FindPtr(path)) {
                    entries = *data;
                }

                EraseIf(entries, [prefix = ToLowerUTF8(prefix)](const TFolderEntry& entry) {
                    return !entry.Name.StartsWith(prefix);
                });

                EraseIf(entries, [types = std::move(request.Filter.Types)](const TFolderEntry& entry) {
                    return types && !types->contains(entry.Type);
                });

                Y_ENSURE(request.Limit <= MaxLimit);
                entries.crop(request.Limit);

                TListResponse response = {
                    .NameHintLength = prefix.size(),
                    .Entries = std::move(entries),
                };

                return NThreading::MakeFuture(std::move(response));
            }

        private:
            static std::tuple<TStringBuf, TStringBuf> ParsePath(TString path Y_LIFETIME_BOUND) {
                size_t pos = path.find_last_of('/');
                if (pos == TString::npos) {
                    return {"", path};
                }

                TStringBuf head, tail;
                TStringBuf(path).SplitAt(pos + 1, head, tail);
                return {head, tail};
            }

            THashMap<TString, TVector<TFolderEntry>> Data_;
        };

    } // namespace

    ISchemaGateway::TPtr MakeStaticSchemaGateway(THashMap<TString, TVector<TFolderEntry>> fs) {
        return MakeIntrusive<TSchemaGateway>(std::move(fs));
    }

} // namespace NSQLComplete
