#include "schema.h"

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        class TSimpleSchema: public ISchema {
        private:
            static auto FilterByName(TString name) {
                return [name = std::move(name)](auto f) {
                    TVector<TFolderEntry> entries = f.ExtractValue();
                    EraseIf(entries, [prefix = ToLowerUTF8(name)](const TFolderEntry& entry) {
                        return !entry.Name.StartsWith(prefix);
                    });
                    return entries;
                };
            }

            static auto FilterByTypes(TMaybe<THashSet<TString>> types) {
                return [types = std::move(types)](auto f) {
                    TVector<TFolderEntry> entries = f.ExtractValue();
                    EraseIf(entries, [types = std::move(types)](const TFolderEntry& entry) {
                        return types && !types->contains(entry.Type);
                    });
                    return entries;
                };
            }

            static auto Crop(size_t limit) {
                return [limit](auto f) {
                    TVector<TFolderEntry> entries = f.ExtractValue();
                    entries.crop(limit);
                    return entries;
                };
            }

            static auto ToResponse(TStringBuf name) {
                const auto length = name.length();
                return [length](auto f) {
                    return TListResponse{
                        .NameHintLength = length,
                        .Entries = f.ExtractValue(),
                    };
                };
            }

        public:
            explicit TSimpleSchema(ISimpleSchema::TPtr simple)
                : Simple_(std::move(simple))
            {
            }

            NThreading::TFuture<TListResponse> List(const TListRequest& request) const override {
                auto [path, name] = Simple_->Split(request.Path);
                return Simple_->List(TString(path))
                    .Apply(FilterByName(TString(name)))
                    .Apply(FilterByTypes(std::move(request.Filter.Types)))
                    .Apply(Crop(request.Limit))
                    .Apply(ToResponse(name));
            }

        private:
            ISimpleSchema::TPtr Simple_;
        };

    } // namespace

    NThreading::TFuture<TVector<TFolderEntry>>
    ISimpleSchema::List(TString folder) const {
        return List(/* cluster = */ "", folder);
    }

    NThreading::TFuture<TVector<TFolderEntry>>
    ISimpleSchema::List(TString /* cluster */, TString folder) const {
        return List(folder);
    }

    ISchema::TPtr MakeSimpleSchema(ISimpleSchema::TPtr simple) {
        return ISchema::TPtr(new TSimpleSchema(std::move(simple)));
    }

} // namespace NSQLComplete
