#include "synchronization.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NLsp {

namespace {

class TTextDocuments final: public ITextDocuments {
    static_assert(
        sizeof(TTextDocumentIdentifier) == sizeof(TDocumentUri),
        "Assume a document is identified only by a URI");

public:
    void Open(TDidOpenTextDocumentParams params) override {
        const TDocumentUri& id = params.TextDocument.Uri;
        TTextDocumentItemPtr& item = Items_[id];

        TTextDocumentVersion incoming = params.TextDocument.Version;
        TTextDocumentVersion existing = Version(item);
        if (incoming < existing) {
            throw TLspException::Conflict(incoming, existing);
        }

        item = std::make_shared<TTextDocumentItem>(std::move(params.TextDocument));
    }

    void Change(TDidChangeTextDocumentParams params) override {
        const TDocumentUri& id = params.TextDocument.Uri;
        TTextDocumentItemPtr& item = Items_[id];
        if (!item) {
            throw TLspException::UnknownUri(id);
        }

        TTextDocumentVersion incoming = params.TextDocument.Version;
        TTextDocumentVersion existing = Version(item);
        if (incoming < existing) {
            throw TLspException::Conflict(incoming, existing);
        }

        TMaybe<TString> text;
        if (auto changes = std::move(params.ContentChanges); !changes.empty()) {
            auto& back = changes.back();

            if (back.IsIncremental()) {
                throw TLspException::Unsupported()
                    << "incremental change, "
                    << "use full";
            }

            text = std::move(back.Text);
        }

        item = std::make_shared<TTextDocumentItem>(TTextDocumentItem{
            .Uri = std::move(params.TextDocument.Uri),
            .LanguageId = item->LanguageId,
            .Version = params.TextDocument.Version,
            .Text = std::move(text).GetOrElse(item->Text),
        });
    }

    void Close(const TDidCloseTextDocumentParams& params) override {
        Items_.erase(params.TextDocument.Uri);
    }

    TTextDocumentItemPtr Find(const TTextDocumentIdentifier& id) const override {
        const TTextDocumentItemPtr* item = Items_.FindPtr(id.Uri);
        if (!item) {
            throw TLspException::UnknownUri(id.Uri);
        }

        return *item;
    }

private:
    static TTextDocumentVersion Version(const TTextDocumentItemPtr& item) {
        return item ? item->Version : Min<TTextDocumentVersion>();
    }

    THashMap<TDocumentUri, TTextDocumentItemPtr> Items_;
};

} // namespace

ITextDocuments::TPtr MakeTextDocuments() {
    return new TTextDocuments();
}

} // namespace NLsp
