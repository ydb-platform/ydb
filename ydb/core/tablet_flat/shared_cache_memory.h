#pragma once

#include "flat_sausage_gut.h"

namespace NKikimr::NSharedCache {

// Allocation-owned tags correct overlap between metadata references and page charges.
// Copies of TSharedData share the tag without a separate allocation identity index.
class TMetadataBuffers {
public:
    TMetadataBuffers();
    ~TMetadataBuffers();
    TMetadataBuffers(const TMetadataBuffers&) = delete;
    TMetadataBuffers& operator=(const TMetadataBuffers&) = delete;

    void UpdateMetadataReferences(const TSharedData& buffer, i64 delta);
    void UpdatePageCharge(const TSharedData& buffer, i64 delta);
    i64 GetMemoryUsage() const noexcept;

private:
    class TAccount;
    class TBufferTag;
    void Update(const TSharedData& buffer, i64 referencesDelta, i64 pageBytesDelta);

    TIntrusivePtr<TAccount> Account;
};

} // namespace NKikimr::NSharedCache
