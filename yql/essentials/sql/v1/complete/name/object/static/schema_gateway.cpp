#include "schema_gateway.h"

#include <yql/essentials/sql/v1/complete/name/object/simple/schema_gateway.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        class TSimpleSchemaGateway: public ISimpleSchemaGateway {
        public:
            explicit TSimpleSchemaGateway(THashMap<TString, TVector<TFolderEntry>> data)
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

    ISchemaGateway::TPtr MakeStaticSchemaGateway(THashMap<TString, TVector<TFolderEntry>> fs) {
        return MakeSimpleSchemaGateway(
            ISimpleSchemaGateway::TPtr(
                new TSimpleSchemaGateway(std::move(fs))));
    }

} // namespace NSQLComplete
