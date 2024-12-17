#pragma once

#include "flat_page_btree_index.h"
#include "flat_part_iface.h"

namespace NKikimr::NTable::NPage {

    class TBtreeIndexNodeWriter {
        using THeader = TBtreeIndexNode::THeader;
        using TIsNullBitmap = TBtreeIndexNode::TIsNullBitmap;
        using TShortChild = TBtreeIndexNode::TShortChild;
        using TChild = TBtreeIndexNode::TChild;

    public:
        TBtreeIndexNodeWriter(TIntrusiveConstPtr<TPartScheme> scheme, TGroupId groupId)
            : Scheme(std::move(scheme))
            , GroupId(groupId)
            , GroupInfo(Scheme->GetLayout(groupId))
        {
            if (GroupId.IsMain()) {
                // TODO: some main groups without nulls and var-sized cells also may use fixed format
                FixedKeySize = TBtreeIndexNode::THeader::MaxFixedKeySize;
            } else {
                FixedKeySize = 0;
                for (TPos pos : xrange(GroupInfo.KeyTypes.size())) {
                    Y_ABORT_UNLESS(GroupInfo.ColsKeyIdx[pos].IsFixed);
                    FixedKeySize += GroupInfo.ColsKeyIdx[pos].FixedSize;
                }
                Y_ABORT_UNLESS(FixedKeySize < TBtreeIndexNode::THeader::MaxFixedKeySize, "FixedKeySize is out of bounds");
            }
        }

        bool IsFixedFormat() const noexcept
        {
            return FixedKeySize != TBtreeIndexNode::THeader::MaxFixedKeySize;
        }

        bool IsShortChildFormat() const noexcept
        {
            return !GroupId.IsMain();
        }

        void AddKey(TCellsRef cells) {
            AddKey(SerializeKey(cells));
        }

        void AddKey(TString&& key) {
            KeysSize += key.size();
            Keys.emplace_back(std::move(key));
        }

        void AddChild(TChild child) {
            Y_ABORT_UNLESS(child.GetErasedRowCount() == 0 || !IsShortChildFormat(), "Short format can't have ErasedRowCount");
            Children.push_back(child);
        }

        void EnsureEmpty() {
            Y_ABORT_UNLESS(!Keys);
            Y_ABORT_UNLESS(!KeysSize);
            Y_ABORT_UNLESS(!Children);
            Y_ABORT_UNLESS(!Ptr);
            Y_ABORT_UNLESS(!End);
        }

        void Reset() {
            Keys.clear();
            KeysSize = 0;
            Children.clear();
            Ptr = 0;
            End = 0;
        }

        TString SerializeKey(TCellsRef cells) {
            Y_ABORT_UNLESS(cells.size() <= GroupInfo.KeyTypes.size());

            TString buf;
            buf.ReserveAndResize(CalcKeySize(cells));
            Ptr = buf.Detach();
            End = buf.end();

            PlaceKey(cells);

            Y_ABORT_UNLESS(Ptr == End);
            NSan::CheckMemIsInitialized(buf.data(), buf.size());
            Ptr = 0;
            End = 0;

            return buf;
        }

        TSharedData Finish() {
            Y_ABORT_UNLESS(Keys.size());
            Y_ABORT_UNLESS(Children.size() == Keys.size() + 1);

            size_t pageSize = CalcPageSize();
            TSharedData buf = TSharedData::Uninitialized(pageSize);
            Ptr = buf.mutable_begin();
            End = buf.end();

            WriteUnaligned<TLabel>(Advance(sizeof(TLabel)), TLabel::Encode(EPage::BTreeIndex, TBtreeIndexNode::FormatVersion, pageSize));

            auto &header = Place<THeader>();
            header.KeysCount = Keys.size();
            Y_ABORT_UNLESS(KeysSize < Max<TPgSize>(), "KeysSize is out of bounds");
            header.KeysSize = KeysSize;
            header.IsShortChildFormat = IsShortChildFormat();
            header.FixedKeySize = FixedKeySize;

            if (!IsFixedFormat()) {
                size_t keyOffset = Ptr - buf.mutable_begin() + sizeof(TRecordsEntry) * Keys.size();
                for (const auto &key : Keys) {
                    auto &meta = Place<TRecordsEntry>();
                    Y_ABORT_UNLESS(keyOffset < Max<TPgSize>(), "Key offset is out of bounds");
                    meta.Offset = keyOffset;
                    keyOffset += key.size();
                }
                Y_ABORT_UNLESS(Ptr == buf.mutable_begin() + sizeof(TLabel) + sizeof(THeader) + sizeof(TRecordsEntry) * Keys.size());
            }

            for (auto &key : Keys) {
                PlaceBytes(std::move(key));
            }
            Y_ABORT_UNLESS(Ptr == buf.mutable_begin() + 
                sizeof(TLabel) + sizeof(THeader) + 
                (IsFixedFormat() ? 0 : sizeof(TRecordsEntry) * Keys.size()) + 
                KeysSize);
            Keys.clear();
            KeysSize = 0;

            for (auto &child : Children) {
                PlaceChild(child);
            }
            Children.clear();

            Y_ABORT_UNLESS(Ptr == End);
            NSan::CheckMemIsInitialized(buf.data(), buf.size());
            Ptr = 0;
            End = 0;

            return buf;
        };

        size_t CalcPageSize() const {
            return CalcPageSize(KeysSize, Keys.size());
        }

        size_t CalcPageSize(size_t keysSize, size_t keysCount) const {
            return
                sizeof(TLabel) + sizeof(THeader) +
                (IsFixedFormat() ? 0 : sizeof(TRecordsEntry) * keysCount) +
                keysSize +
                (IsShortChildFormat() ? sizeof(TShortChild) : sizeof(TChild)) * (keysCount + 1);
        }

        size_t GetKeysCount() const {
            return Keys.size();
        }

        TPgSize CalcKeySizeWithMeta(TCellsRef cells) const noexcept {
            return 
                sizeof(TRecordsEntry) + 
                CalcKeySize(cells) + 
                (IsShortChildFormat() ? sizeof(TShortChild) : sizeof(TChild));
        }

    private:
        TPgSize CalcKeySize(TCellsRef cells) const noexcept
        {
            if (IsFixedFormat()) {
                return FixedKeySize;
            }

            TPgSize size = TIsNullBitmap::Length(GroupInfo.ColsKeyIdx.size());

            for (TPos pos : xrange(cells.size())) {
                if (const auto &val = cells[pos]) {
                    size += GroupInfo.ColsKeyIdx[pos].FixedSize; // fixed data or data ref
                    if (!GroupInfo.ColsKeyIdx[pos].IsFixed) {
                        size += cells[pos].Size();
                    }
                }
            }

            return size;
        }

        void PlaceKey(TCellsRef cells) 
        {
            if (IsFixedFormat()) {
                for (TPos pos : xrange(cells.size())) {
                    Y_ABORT_UNLESS(cells[pos], "Can't have null cells in fixed format");
                    const auto &info = GroupInfo.ColsKeyIdx[pos];
                    Y_ABORT_UNLESS(info.IsFixed, "Can't have non-fixed cells in fixed format");
                    Y_ABORT_UNLESS(cells[pos].Size() == info.FixedSize, "invalid fixed cell size)");
                    memcpy(Advance(cells[pos].Size()), cells[pos].Data(), cells[pos].Size());
                }
                return;
            }

            auto *isNullBitmap = TDeref<TIsNullBitmap>::At(Ptr);

            // mark all cells as non-null
            Zero(isNullBitmap->Length(GroupInfo.ColsKeyIdx.size()));

            auto* keyCellsPtr = Ptr;

            for (TPos pos : xrange(cells.size())) {
                if (const auto &val = cells[pos]) {
                    const auto &info = GroupInfo.ColsKeyIdx[pos];
                    Zero(info.FixedSize);
                } else {
                    isNullBitmap->SetNull(pos);
                }
            }
            for (TPos pos : xrange(cells.size(), GroupInfo.ColsKeyIdx.size())) {
                isNullBitmap->SetNull(pos);
            }

            for (TPos pos : xrange(cells.size())) {
                if (const auto &cell = cells[pos]) {
                    Y_DEBUG_ABORT_UNLESS(!isNullBitmap->IsNull(pos));
                    const auto &info = GroupInfo.ColsKeyIdx[pos];
                    PlaceCell(info, cell, keyCellsPtr);
                    keyCellsPtr += info.FixedSize;
                } else {
                    Y_DEBUG_ABORT_UNLESS(isNullBitmap->IsNull(pos));
                }
            }
        }

        void PlaceCell(const TPartScheme::TColumn& info, TCell value, char* keyCellsPtr)
        {
            if (info.IsFixed) {
                Y_ABORT_UNLESS(value.Size() == info.FixedSize, "invalid fixed cell size)");
                memcpy(keyCellsPtr, value.Data(), value.Size());
            } else {
                auto *ref = TDeref<TDataRef>::At(keyCellsPtr);
                ref->Offset = Ptr - keyCellsPtr;
                ref->Size = value.Size();
                std::copy(value.Data(), value.Data() + value.Size(), Advance(value.Size()));
            }
        }

        void PlaceBytes(TString&& data) noexcept
        {
            std::copy(data.data(), data.data() + data.size(), Advance(data.size()));
        }

        void PlaceChild(const TChild& child) noexcept
        {
            if (IsShortChildFormat()) {
                Y_DEBUG_ABORT_UNLESS(child.GetGroupDataSize() == 0);
                Y_DEBUG_ABORT_UNLESS(child.GetErasedRowCount() == 0);
                Place<TShortChild>() = TShortChild{child.GetPageId(), child.GetRowCount(), child.GetDataSize()};
            } else {
                Place<TChild>() = child;
            }
        }

        template<typename T>
        T& Place() noexcept
        {
            return *reinterpret_cast<T*>(Advance(TPgSizeOf<T>::Value));
        }

        void Zero(size_t size) noexcept
        {
            auto *from = Advance(size);
            std::fill(from, Ptr, 0);
        }

        char* Advance(size_t size) noexcept
        {
            auto newPtr = Ptr + size;
            Y_ABORT_UNLESS(newPtr <= End);
            return std::exchange(Ptr, newPtr);
        }

    public:
        const TIntrusiveConstPtr<TPartScheme> Scheme;
        const TGroupId GroupId;
        const TPartScheme::TGroupInfo& GroupInfo;

    private:
        size_t FixedKeySize;

        TVector<TString> Keys;
        size_t KeysSize = 0;

        TVector<TChild> Children;

        char* Ptr = 0;
        const char* End = 0;
    };

    class TBtreeIndexBuilder {
    public:
        using TShortChild = TBtreeIndexNode::TShortChild;
        using TChild = TBtreeIndexNode::TChild;

    private:
        struct TLevel {
            void PushKey(TString&& key) {
                KeysSize += key.size();
                Keys.emplace_back(std::move(key));
            }

            TString PopKey() {
                Y_ABORT_UNLESS(Keys);
                TString key = std::move(Keys.front());
                KeysSize -= key.size();
                Keys.pop_front();
                return std::move(key);
            }

            void PushChild(TChild child) {
                Children.push_back(child);
            }

            TChild PopChild() {
                Y_ABORT_UNLESS(Children);
                TChild result = Children.front();
                Children.pop_front();
                return result;
            }

            size_t GetKeysSize() const {
                return KeysSize;
            }

            size_t GetKeysCount() const {
                return Keys.size();
            }

            size_t GetChildrenCount() const {
                return Children.size();
            }

        private:
            size_t KeysSize = 0;
            TDeque<TString> Keys;
            TDeque<TChild> Children;
        };

    public:
        TBtreeIndexBuilder(TIntrusiveConstPtr<TPartScheme> scheme, TGroupId groupId,
                ui32 nodeTargetSize, ui32 nodeKeysMin, ui32 nodeKeysMax)
            : Scheme(std::move(scheme))
            , GroupId(groupId)
            , GroupInfo(Scheme->GetLayout(groupId))
            , Writer(Scheme, groupId)
            , Levels(1)
            , NodeTargetSize(nodeTargetSize)
            , NodeKeysMin(nodeKeysMin)
            , NodeKeysMax(nodeKeysMax)
        {
            Y_ABORT_UNLESS(NodeTargetSize > 0);
            Y_ABORT_UNLESS(NodeKeysMin > 0);
            Y_ABORT_UNLESS(NodeKeysMax >= NodeKeysMin);
        }

        TPgSize CalcSize(TCellsRef cells) const {
            return Writer.CalcKeySizeWithMeta(cells);
        }

        // returns approximate value, doesn't include new child creation on Flush step
        ui64 EstimateBytesUsed() const {
            ui64 result = IndexSize; // written bytes
            for (const auto &lvl : Levels) {
                result += CalcPageSize(lvl);
            }
            return result;
        }

        void AddKey(TCellsRef cells) {
            Levels[0].PushKey(Writer.SerializeKey(cells));
        }

        void AddShortChild(TShortChild child) {
            AddChild(TChild{child.GetPageId(), child.GetRowCount(), child.GetDataSize(), 0, 0});
        }

        void AddChild(TChild child) {
            // aggregate in order to perform search by row id from any leaf node
            child.RowCount_ = (ChildRowCount += child.GetRowCount());
            child.DataSize_ = (ChildDataSize += child.GetDataSize());
            child.GroupDataSize_ = (ChildGroupDataSize += child.GetGroupDataSize());
            child.ErasedRowCount_ = (ChildErasedRowCount += child.GetErasedRowCount());

            Levels[0].PushChild(child);
        }
        
        void Flush(IPageWriter &pager) {
            for (ui32 levelIndex = 0; levelIndex < Levels.size(); levelIndex++) {
                bool hasChanges = false;

                // Note: in theory we may want to flush one level multiple times when different triggers are applicable
                while (CanFlush(levelIndex)) {
                    DoFlush(levelIndex, pager, false);
                    hasChanges = true;
                }

                if (!hasChanges) {
                    break; // no more changes
                }
            }
        }

        TBtreeIndexMeta Finish(IPageWriter &pager) {
            for (ui32 levelIndex = 0; levelIndex < Levels.size(); levelIndex++) {
                if (!Levels[levelIndex].GetKeysCount()) {
                    Y_ABORT_UNLESS(Levels[levelIndex].GetChildrenCount() == 1, "Should be root");
                    Y_ABORT_UNLESS(levelIndex + 1 == Levels.size(), "Should be root");
                    return {Levels[levelIndex].PopChild(), levelIndex, IndexSize};
                }

                DoFlush(levelIndex, pager, true);
            }

            Y_ABORT_UNLESS(false, "Should have returned root");
        }

        void Reset() {
            IndexSize = 0;
            Writer.Reset();
            Levels = { TLevel() };
            ChildRowCount = 0;
            ChildErasedRowCount = 0;
            ChildDataSize = 0;
            ChildGroupDataSize = 0;
        }

    private:
        bool CanFlush(ui32 levelIndex) {
            const ui64 waitFullNodes = 2;

            if (Levels[levelIndex].GetKeysCount() <= waitFullNodes * NodeKeysMin) {
                // node keys min restriction should be always satisfied
                return false;
            }

            // Note: size checks are approximate and flush might not produce 2 full-sized pages

            return 
                Levels[levelIndex].GetKeysCount() > waitFullNodes * NodeKeysMax ||
                CalcPageSize(Levels[levelIndex]) > waitFullNodes * NodeTargetSize;
        }

        void DoFlush(ui32 levelIndex, IPageWriter &pager, bool last) {
            Writer.EnsureEmpty();
            
            if (last) {
                // Note: for now we build last nodes from all remaining level's keys
                // we may to try splitting them more evenly later

                while (Levels[levelIndex].GetKeysCount()) {
                    Writer.AddChild(Levels[levelIndex].PopChild());
                    Writer.AddKey(Levels[levelIndex].PopKey());
                }
            } else {
                while (Writer.GetKeysCount() < NodeKeysMin || (
                    // can add more to writer if:
                        Levels[levelIndex].GetKeysCount() > 2 &&
                        Writer.GetKeysCount() < NodeKeysMax &&
                        Writer.CalcPageSize() < NodeTargetSize)) {
                    Writer.AddChild(Levels[levelIndex].PopChild());
                    Writer.AddKey(Levels[levelIndex].PopKey());
                }
            }
            auto lastChild = Levels[levelIndex].PopChild();
            Writer.AddChild(lastChild);

            auto page = Writer.Finish();
            IndexSize += page.size();
            auto pageId = pager.Write(std::move(page), EPage::BTreeIndex, 0);

            if (levelIndex + 1 == Levels.size()) {
                Levels.emplace_back();
                Y_ABORT_UNLESS(Levels.size() < Max<ui32>(), "Levels size is out of bounds");
            }
            lastChild.PageId_ = pageId;
            Levels[levelIndex + 1].PushChild(lastChild);
            if (!last) {
                Levels[levelIndex + 1].PushKey(Levels[levelIndex].PopKey());
            }

            if (last) {
                Y_ABORT_UNLESS(!Levels[levelIndex].GetKeysCount());
                Y_ABORT_UNLESS(!Levels[levelIndex].GetKeysSize());
                Y_ABORT_UNLESS(!Levels[levelIndex].GetChildrenCount());
            } else {
                Y_ABORT_UNLESS(Levels[levelIndex].GetKeysCount(), "Shouldn't leave empty levels");
            }
        }

        size_t CalcPageSize(const TLevel& level) const {
            return Writer.CalcPageSize(level.GetKeysSize(), level.GetKeysCount());
        }

    public:
        const TIntrusiveConstPtr<TPartScheme> Scheme;
        const TGroupId GroupId;
        const TPartScheme::TGroupInfo& GroupInfo;

    private:
        ui64 IndexSize = 0;

        TBtreeIndexNodeWriter Writer;
        TVector<TLevel> Levels; // from bottom to top

        const ui32 NodeTargetSize;
        const ui32 NodeKeysMin;
        const ui32 NodeKeysMax;

        TRowId ChildRowCount = 0;
        TRowId ChildErasedRowCount = 0;
        ui64 ChildDataSize = 0;
        ui64 ChildGroupDataSize = 0;
    };

}
