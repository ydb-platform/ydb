#include "schema.h"

namespace NSQLComplete {

    namespace {

        class TSimpleSchema: public ISimpleSchema {
        public:
            explicit TSimpleSchema(THashMap<TString, TVector<TFolderEntry>> data)
                : Data_(std::move(data))
            {
                for (const auto& [k, _] : Data_) {
                    Y_ENSURE(k.StartsWith("/"), k << " must start with the '/'");
                    Y_ENSURE(k.EndsWith("/"), k << " must end with the '/'");
                }
            }

            TSplittedPath Split(TStringBuf path) const override {
                size_t pos = path.find_last_of('/');
                if (pos == TString::npos) {
                    return {"", path};
                }

                TStringBuf head, tail;
                TStringBuf(path).SplitAt(pos + 1, head, tail);
                return {head, tail};
            }

            NThreading::TFuture<TVector<TFolderEntry>> List(TString folder) const override {
                if (!folder.StartsWith('/')) {
                    folder.prepend('/');
                }

                TVector<TFolderEntry> entries;
                if (const auto* data = Data_.FindPtr(folder)) {
                    entries = *data;
                }
                return NThreading::MakeFuture(std::move(entries));
            }

        private:
            THashMap<TString, TVector<TFolderEntry>> Data_;
        };

    } // namespace

    ISimpleSchema::TPtr MakeStaticSimpleSchema(THashMap<TString, TVector<TFolderEntry>> fs) {
        return new TSimpleSchema(std::move(fs));
    }

} // namespace NSQLComplete
