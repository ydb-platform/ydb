#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>
#include <yql/essentials/tools/yql_language_server/lsp/message/synchronization.h>

namespace NLsp {

class ITextDocument {
public:
    virtual void Change(TTextDocumentVersion v, TMaybe<TString> t) = 0;
    virtual TTextDocumentVersion Version() const = 0;
};

template <typename T>
concept CTextDocument =
    std::derived_from<T, ITextDocument> &&
    std::derived_from<T, TThrRefBase>;

template <CTextDocument T>
using TTextDocumentFactory = std::function<TIntrusivePtr<T>(TTextDocumentItem)>;

template <CTextDocument T>
class TTextDocuments: public TThrRefBase {
    static_assert(
        sizeof(TTextDocumentIdentifier) == sizeof(TDocumentUri),
        "Assume a document is identified only by a URI");

public:
    using TPtr = TIntrusivePtr<TTextDocuments>;
    using TItemPtr = TIntrusivePtr<T>;

    explicit TTextDocuments(TTextDocumentFactory<T> factory)
        : Factory_(std::move(factory))
    {
    }

    void Open(TDidOpenTextDocumentParams params) {
        const TDocumentUri& id = params.TextDocument.Uri;
        TItemPtr& item = Items_[id];

        TTextDocumentVersion incoming = params.TextDocument.Version;
        TTextDocumentVersion existing = Version(item);
        if (incoming < existing) {
            throw TLspException::Conflict(incoming, existing);
        }

        item = Factory_(std::move(params.TextDocument));
    }

    void Change(TDidChangeTextDocumentParams params) {
        const TDocumentUri& id = params.TextDocument.Uri;
        TItemPtr& item = Items_[id];
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

        item->Change(params.TextDocument.Version, std::move(text));
    }

    void Close(const TDidCloseTextDocumentParams& params) {
        Items_.erase(params.TextDocument.Uri);
    }

    /// @return non-null
    TItemPtr Find(const TTextDocumentIdentifier& id) const {
        const TItemPtr* item = Items_.FindPtr(id.Uri);
        if (!item) {
            throw TLspException::UnknownUri(id.Uri);
        }

        Y_ENSURE(*item);
        return *item;
    }

private:
    static TTextDocumentVersion Version(const TItemPtr& item) {
        return item ? item->Version() : Min<TTextDocumentVersion>();
    }

    THashMap<TDocumentUri, TItemPtr> Items_;
    TTextDocumentFactory<T> Factory_;
};

} // namespace NLsp
