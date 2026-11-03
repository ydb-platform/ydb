#pragma once

#include "flat_sausage_gut.h"

namespace NKikimr::NSharedCache {

class TMetadataAccount;

// Metadata and Globs stay charged until their final allocation/object owner releases them
class TMetadataAccounting {
public:
    TMetadataAccounting();
    ~TMetadataAccounting();
    TMetadataAccounting(const TMetadataAccounting&) = delete;
    TMetadataAccounting& operator=(const TMetadataAccounting&) = delete;
    TMetadataAccounting(TMetadataAccounting&&) noexcept;
    TMetadataAccounting& operator=(TMetadataAccounting&&) noexcept;

    TMetadataAccounting Share() const noexcept;
    // The caller owns the buffer/descriptor throughout registration
    void AccountBuffer(const TSharedData& buffer) const;
    void AccountMetadata(const NPageCollection::IPageCollection& collection) const;
    ui64 GetMemoryUsage() const noexcept;

private:
    explicit TMetadataAccounting(TIntrusivePtr<TMetadataAccount> account) noexcept;
    void AccountObject(std::atomic<TMetadataRecord*>& records, ui64 bytes) const;

    TIntrusivePtr<TMetadataAccount> Account_;
};

} // namespace NKikimr::NSharedCache
