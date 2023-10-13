#pragma once

#include "flat_page_btree_index.h"

namespace NKikimr::NTable::NPage {

    class TBtreeIndexNodeWriter {
        using THeader = TBtreeIndexNode::THeader;
        using TKey = TBtreeIndexNode::TKey;
        using TChild = TBtreeIndexNode::TChild;

    public:
        TBtreeIndexNodeWriter(TIntrusiveConstPtr<TPartScheme> scheme, TGroupId groupId)
            : Scheme(std::move(scheme))
            , GroupId(groupId)
            , GroupInfo(Scheme->GetLayout(groupId))
        {
        }

        // TODO: pass serialized key + size from TBtreeIndexBuilder
        void AddKey(TString serializedKey) {
            TSerializedCellVec key(serializedKey);
            KeysSize += CalcSize(key.GetCells());
            // TODO: serialize to page bytes directly
            Keys.push_back(serializedKey);
        }

        void AddChild(TChild child) {
            Children.push_back(child);
        }

        TSharedData Finish() {
            Y_ABORT_UNLESS(Keys.size());
            Y_ABORT_UNLESS(Children.size() == Keys.size() + 1);

            size_t childSize = sizeof(TChild);
            size_t pageSize =
                    sizeof(TLabel) + sizeof(THeader) +
                    KeysSize +
                    sizeof(TRecordsEntry) * Keys.size() +
                    childSize * Children.size();

            TSharedData buf = TSharedData::Uninitialized(pageSize);
            Ptr = buf.mutable_begin();
            End = buf.end();

            WriteUnaligned<TLabel>(Advance(Ptr, sizeof(TLabel)), TLabel::Encode(EPage::BTreeIndex, 0, pageSize));

            auto &header = Place<THeader>();
            header.KeysCount = Keys.size();
            Y_ABORT_UNLESS(KeysSize < Max<TPgSize>(), "KeysSize is out of bounds");
            header.KeysSize = KeysSize;

            char* keyOffsetPtr = Ptr + KeysSize;
            for (const auto &key : Keys) {
                auto &meta = Place<TRecordsEntry>(keyOffsetPtr);
                size_t offset = Ptr - buf.mutable_begin();
                Y_ABORT_UNLESS(offset < Max<TPgSize>(), "Key offset is out of bounds");
                meta.Offset = offset;

                TSerializedCellVec cells(key);
                PlaceKey(cells.GetCells());
            }
            Y_ABORT_UNLESS(Ptr == buf.mutable_begin() + sizeof(TLabel) + sizeof(THeader) + KeysSize);
            Ptr = keyOffsetPtr;
            Keys.clear();
            KeysSize = 0;

            PlaceVector(Children);

            Y_ABORT_UNLESS(Ptr == End);
            NSan::CheckMemIsInitialized(buf.data(), buf.size());
            Ptr = 0;
            End = 0;

            return buf;
        };

    private:
        TPgSize CalcSize(TCellsRef key) const noexcept
        {
            Y_ABORT_UNLESS(key.size() <= GroupInfo.KeyTypes.size());

            TPgSize size = TKey::NullBitmapLength(GroupInfo.ColsKeyIdx.size());

            for (TPos pos : xrange(key.size())) {
                if (const auto &val = key[pos]) {
                    size += GroupInfo.ColsKeyIdx[pos].FixedSize; // fixed data or data ref
                    if (!GroupInfo.ColsKeyIdx[pos].IsFixed) {
                        size += key[pos].Size();
                    }
                }
            }

            return size;
        }

        void PlaceKey(TCellsRef cells) 
        {
            const TPos count = GroupInfo.ColsKeyIdx.size();
            Y_ABORT_UNLESS(cells.size() <= count);

            auto *key = TDeref<TKey>::At(Ptr);

            // mark all cells as non-null
            Zero(key->NullBitmapLength(count));

            auto* keyCellsPtr = Ptr;

            for (TPos pos : xrange(cells.size())) {
                if (const auto &val = cells[pos]) {
                    const auto &info = GroupInfo.ColsKeyIdx[pos];
                    Zero(info.FixedSize);
                } else {
                    key->SetNull(pos);
                }
            }
            for (TPos pos : xrange(cells.size(), GroupInfo.ColsKeyIdx.size())) {
                key->SetNull(pos);
            }

            for (TPos pos : xrange(cells.size())) {
                if (const auto &cell = cells[pos]) {
                    Y_DEBUG_ABORT_UNLESS(!key->IsNull(pos));
                    const auto &info = GroupInfo.ColsKeyIdx[pos];
                    PlaceCell(info, cell, keyCellsPtr);
                    keyCellsPtr += info.FixedSize;
                } else {
                    Y_DEBUG_ABORT_UNLESS(key->IsNull(pos));
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

        template<typename T>
        void PlaceVector(TVector<T> &vector) noexcept
        {
            auto *dst = reinterpret_cast<T*>(Advance(Ptr, sizeof(T)*vector.size()));
            std::copy(vector.begin(), vector.end(), dst);
            vector.clear();
        }

        template<typename T>
        T& Place()
        {
            return *reinterpret_cast<T*>(Advance(TPgSizeOf<T>::Value));
        }

        template<typename T>
        T& Place(char*& ptr)
        {
            return *reinterpret_cast<T*>(Advance(ptr, TPgSizeOf<T>::Value));
        }

        void Zero(size_t size) noexcept
        {
            auto *from = Advance(Ptr, size);
            std::fill(from, Ptr, 0);
        }

        char* Advance(char*& ptr, size_t size) noexcept
        {
            auto newPtr = ptr + size;
            Y_ABORT_UNLESS(newPtr <= End);
            return std::exchange(ptr, newPtr);
        }

        char* Advance(size_t size) noexcept
        {
            auto newPtr = Ptr + size;
            Y_ABORT_UNLESS(newPtr <= End);
            return std::exchange(Ptr, newPtr);
        }

    public:
        const TIntrusiveConstPtr<TPartScheme> Scheme;

    private:
        const TGroupId GroupId;
        const TPartScheme::TGroupInfo& GroupInfo;

        TVector<TString> Keys;
        size_t KeysSize = 0;

        TVector<TChild> Children;

        char* Ptr = 0;
        const char* End = 0;
    };

}
