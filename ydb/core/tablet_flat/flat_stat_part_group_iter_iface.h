#pragma once

#include "flat_part_iface.h"
#include "ydb/core/scheme/scheme_tablecell.h"

namespace NKikimr::NTable {

struct TChanneledDataSize {
    ui64 Size = 0;
    TVector<ui64> ByChannel = { };

    void Add(ui64 size, ui8 channel) {
        Size += size;
        if (!(channel < ByChannel.size())) {
            ByChannel.resize(channel + 1);
        }
        ByChannel[channel] += size;
    }
};

struct TDataStats {
    ui64 RowCount = 0;
    TChanneledDataSize DataSize = { };
};

struct IStatsPartGroupIterator {
    virtual EReady Start() = 0;
    virtual EReady Next() = 0;
    virtual void AddLastDeltaDataSize(TChanneledDataSize& dataSize) = 0;

    virtual bool IsValid() const = 0;

    virtual TRowId GetEndRowId() const = 0;
    virtual TPageId GetPageId() const = 0;
    virtual TRowId GetRowId() const = 0;

    virtual TPos GetKeyCellsCount() const = 0;
    virtual TCell GetKeyCell(TPos index) const = 0;

    virtual ~IStatsPartGroupIterator() = default;
};

THolder<IStatsPartGroupIterator> CreateStatsPartGroupIterator(const TPart* part, IPages* env, NPage::TGroupId groupId);
    
}
