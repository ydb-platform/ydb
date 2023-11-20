#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <ydb/library/yql/public/udf/udf_value.h>


namespace NKikimr {
    namespace NMiniKQL {
        template <typename T>
        struct TListChunk {
        private:
            using TSelf = TListChunk<T>;

            static void PlacementNew(T* ptr, ui64 count) {
                T* end = ptr + count;
                for (; ptr != end; ptr++) {
                    new (ptr) T;
                }
            }

        public:

            void CheckMagic() {
                Y_DEBUG_ABORT_UNLESS(Magic_ == TListChunkMagic);
            }

            static TSelf* AllocateChunk(ui64 length) {
                const auto block = TWithDefaultMiniKQLAlloc::AllocWithSize(length * sizeof(T) + sizeof(TListChunk));
                const auto ptr = new (block) TListChunk(length);
                PlacementNew(reinterpret_cast<T*>(ptr + 1), length);
                return ptr;
            }

            TListChunk(ui64 size)
                : Magic_(TListChunkMagic)
                , Refs_(1)
                , DataEnd_(DataBegin() + size)
            {
            }

            ~TListChunk() {
                CheckMagic();
                for (auto it = DataBegin(); it != DataEnd(); it++) {
                    it->~T();
                }
                TWithDefaultMiniKQLAlloc::FreeWithSize(
                    static_cast<void*>(this), sizeof(TListChunk) + sizeof(T) * (DataEnd() - DataBegin()));
            }

            inline T* DataBegin() {
                CheckMagic();
                return reinterpret_cast<T*>(this + 1);
            }

            inline const T* DataEnd() {
                CheckMagic();
                return DataEnd_;
            }

            ui64 Size() {
                CheckMagic();
                return DataEnd() - DataBegin();
            }

            void Ref() {
                CheckMagic();
                Refs_++;
            }

            void UnRef() {
                CheckMagic();
                if (--Refs_ == 0) {
                    this->~TListChunk();
                }
            }

        private:
            static const ui32 TListChunkMagic = 12322846;
            const ui32 Magic_;
            ui32 Refs_;
            const T* DataEnd_;

        };

        template <typename T, ui64 MinChunkSize = 48>
        class TListRepresentation {
        public:
            using TSelf = TListRepresentation<T, MinChunkSize>;
            using TChunk = TListChunk<T>;

        private:
            enum Type {
                Normal,
                Freezed
            };

            void NewNormal(const T* begin1, const T* end1, const T* begin2, const T* end2) {
                Type_ = Type::Normal;
                ui64 oldLength = (end1 - begin1) + (end2 - begin2);
                ui64 newLength = std::max((oldLength << 1) - 1, (MinChunkSize + sizeof(T) - 1) / sizeof(T) + 1);
                Chunk_ = TChunk::AllocateChunk(newLength);
                Begin_ = Chunk_->DataBegin() + ((newLength - oldLength) >> 2);
                End_ = std::copy(begin2, end2, std::copy(begin1, end1, Begin_));
            }

            TListRepresentation(TChunk* chunk, T* begin, T* end, Type type)
                : Chunk_(chunk)
                , Begin_(begin)
                , End_(end)
                , Type_(type)
            {
                if (Chunk_) {
                    Chunk_->Ref();
                }
            }

            TListRepresentation(T* begin1, T* end1, T* begin2, T* end2)
            {
                NewNormal(begin1, end1, begin2, end2);
            }

        public:

            struct TIterator {
                TIterator()
                    : Owner_(nullptr)
                    , Position_(nullptr)
                {}

                TIterator(const TListRepresentation& owner)
                    : Owner_(&owner)
                    , Position_(owner.Begin_)
                {
                }

                TIterator(const TIterator& other)
                    : Owner_(other.Owner_)
                    , Position_(other.Position_)
                {}

                TIterator& operator=(const TIterator& other)
                {
                    Owner_ = other.Owner_;
                    Position_ = other.Position_;
                    return *this;
                }

                bool AtEnd() const {
                    return Position_ == Owner_->End_;
                }

                const T& Current() const {
                    return *Position_;
                }

                // use with care, list may be shared
                T& MutableCurrent() {
                    return *Position_;
                }

                void Next() {
                    Position_++;
                }

                const TListRepresentation* Owner_;
                T* Position_;
            };

            struct TReverseIterator {
                TReverseIterator()
                    : Owner_(nullptr)
                    , Position_(nullptr)
                {
                }

                TReverseIterator(const TListRepresentation& owner)
                    : Owner_(&owner)
                    , Position_(owner.End_)
                {
                }

                TReverseIterator(const TIterator& other)
                    : Owner_(other.Owner_)
                    , Position_(other.Position_)
                {
                }

                TReverseIterator& operator=(const TReverseIterator& other)
                {
                    Owner_ = other.Owner_;
                    Position_ = other.Position_;
                    return *this;
                }

                bool AtEnd() const {
                    return Position_ == Owner_->Begin_;
                }

                const T& Current() const {
                    return *(Position_ - 1);
                }

                // use with care, list may be shared
                T& MutableCurrent() {
                    return *(Position_ - 1);
                }

                void Next() {
                    Position_--;
                }

            private:
                const TListRepresentation* Owner_;
                T* Position_;
            };

            TListRepresentation()
                : Chunk_(nullptr)
                , Begin_(nullptr)
                , End_(nullptr)
                , Type_(Type::Freezed)
            {
            }

            ~TListRepresentation() {
                if (Chunk_) {
                    Chunk_->UnRef();
                }
            }

            TListRepresentation(const TSelf& other)
                : Chunk_(other.Chunk_)
                , Begin_(other.Begin_)
                , End_(other.End_)
                , Type_(other.Type_)
            {
                other.Type_ = Type::Freezed;
                if (Chunk_) {
                    Chunk_->Ref();
                }
            }

            TListRepresentation(TSelf&& other)
                : Chunk_(other.Chunk_)
                , Begin_(other.Begin_)
                , End_(other.End_)
                , Type_(other.Type_)
            {
                other.Chunk_ = nullptr;
                other.Begin_ = nullptr;
                other.End_ = nullptr;
                other.Type_ = Type::Freezed;
            }

            void operator=(const TSelf& other) {
                if (this != &other) {

                    if (other.Chunk_) {
                        other.Chunk_->Ref();
                    }

                    if (Chunk_) {
                        Chunk_->UnRef();
                    }

                    Chunk_ = other.Chunk_;
                    Begin_ = other.Begin_;
                    End_ = other.End_;
                    Type_ = other.Type_;

                    other.Type_ = Type::Freezed;
                }
            }

            void operator=(TSelf&& other) {
                if (Chunk_) {
                    Chunk_->UnRef();
                }

                Chunk_ = other.Chunk_;
                Begin_ = other.Begin_;
                End_ = other.End_;
                Type_ = other.Type_;

                other.Chunk_ = nullptr;
                other.Begin_ = nullptr;
                other.End_ = nullptr;
                other.Type_ = Type::Freezed;
            }

            inline void FromSingleElement(T&& element) {
                Type_ = Type::Normal;
                ui64 chunkLength = (MinChunkSize + sizeof(T) - 1) / sizeof(T);
                Chunk_ = TChunk::AllocateChunk(chunkLength);
                Begin_ = Chunk_->DataBegin() + (chunkLength >> 2);
                End_ = Begin_ + 1;
                *Begin_ = std::move(element);
            }

            TListRepresentation(T&& element)
            {
                FromSingleElement(std::move(element));
            }

            TListRepresentation(T&& element, const TSelf& that)
            {
                if (!that.Chunk_) {
                    FromSingleElement(std::move(element));
                    return;
                }
                if ((that.Type_ == Type::Normal) && (that.Begin_ != that.Chunk_->DataBegin())) {
                    Type_ = Type::Normal;
                    that.Type_ = Type::Freezed;
                    Chunk_ = that.Chunk_;
                    Chunk_->Ref();
                    Begin_ = that.Begin_;
                    End_ = that.End_;
                    *(--Begin_) = std::move(element);
                } else {
                    NewNormal(&element, &element + 1, that.Begin_, that.End_);
                }
            }

            TListRepresentation(const TSelf& that, T&& element)
            {
                if (!that.Chunk_) {
                    FromSingleElement(std::move(element));
                    return;
                }
                if ((that.Type_ == Type::Normal) && (that.End_ != that.Chunk_->DataEnd())) {
                    Type_ = Type::Normal;
                    that.Type_ = Type::Freezed;
                    Chunk_ = that.Chunk_;
                    Chunk_->Ref();
                    Begin_ = that.Begin_;
                    End_ = that.End_ + 1;
                    *(that.End_) = std::move(element);
                } else {
                    NewNormal(that.Begin_, that.End_, &element, &element + 1);
                }
            }

            ui64 GetLength() const {
                return End_ - Begin_;
            }

            TIterator GetIterator() const {
                return TIterator(*this);
            }

            TReverseIterator GetReverseIterator() const {
                return TReverseIterator(*this);
            }

            TSelf Append(T&& right) const {
                return TSelf(*this, std::move(right));
            }

            TSelf Prepend(T&& left) const {
                return TSelf(std::move(left), *this);
            }

            TSelf MassPrepend(T* begin, T* end) const {
                if ((Type_ == Type::Normal) && (Chunk_->DataBegin() + (end - begin) <= Begin_)) {
                    Type_ = Type::Freezed;
                    return TSelf(Chunk_, std::copy_backward(begin, end, Begin_), End_, Type::Normal);
                } else {
                    return TSelf(begin, end, Begin_, End_);
                }
            }

            TSelf MassAppend(T* begin, T* end) const {
                if ((Type_ == Type::Normal) && (End_ + (end - begin) <= Chunk_->DataEnd())) {
                    Type_ = Type::Freezed;
                    return TSelf(Chunk_, Begin_, std::copy(begin, end, End_), Type::Normal);
                } else {
                    return TSelf(Begin_, End_, begin, end);
                }
            }

            TSelf Extend(const TSelf& right) const {
                ui64 thisLength = GetLength();
                ui64 rightLength = right.GetLength();

                if (!thisLength)
                    return TSelf(right);

                if (!rightLength)
                    return TSelf(*this);

                if (Type_ == Type::Freezed) {
                    if (right.Type_ == Type::Freezed) {
                        return TSelf(Begin_, End_, right.Begin_, right.End_);
                    } else {
                        return right.MassPrepend(Begin_, End_);
                    }
                } else if ((right.Type_ == Type::Freezed) || (thisLength > rightLength)) {
                    return MassAppend(right.Begin_, right.End_);
                } else {
                    return right.MassPrepend(Begin_, End_);
                }
            }

            TSelf SkipFromBegin(ui64 count) const {
                Y_DEBUG_ABORT_UNLESS((count > 0) && (count < GetLength()));
                return TSelf(Chunk_, Begin_ + count, End_, Type::Freezed);
            }

            TSelf SkipFromEnd(ui64 count) const {
                Y_DEBUG_ABORT_UNLESS((count > 0) && (count < GetLength()));
                return TSelf(Chunk_, Begin_, End_ - count, Type::Freezed);
            }

            T GetItemByIndex(ui64 index) const {
                Y_DEBUG_ABORT_UNLESS((index >= 0) && (index < GetLength()));
                return Begin_[index];
            }

            T* GetItems() const {
                return Begin_;
            }

        private:
            TChunk* Chunk_;
            T* Begin_;
            T* End_;
            mutable Type Type_;

        };


        using TDefaultListRepresentation = TListRepresentation<NUdf::TUnboxedValue>;

    }
}
