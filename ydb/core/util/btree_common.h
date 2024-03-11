#pragma once

#include <util/generic/ylimits.h>
#include <util/system/compiler.h>

#include <cstddef>

namespace NKikimr {

    /**
     * Calculates page layout based on a static page size
     */
    template<size_t PageSize = 512>
    class TBTreeLayout {
        /**
         * Aligns size to alignment for constexpr calculations
         */
        static constexpr size_t AlignSizeUp(size_t size, size_t alignment) {
            return size + ((-size) & (alignment - 1));
        }

    public:
        template<class THeader, class TKey, class TEdge>
        class TInnerLayout {
            static constexpr size_t CalcKeysOffset(size_t capacity) { Y_UNUSED(capacity); return AlignSizeUp(sizeof(THeader), alignof(TKey)); }
            static constexpr size_t CalcKeysEnd(size_t capacity) { return CalcKeysOffset(capacity) + sizeof(TKey) * capacity; }
            static constexpr size_t CalcEdgesOffset(size_t capacity) { return AlignSizeUp(CalcKeysEnd(capacity), alignof(TEdge)); }
            static constexpr size_t CalcEdgesEnd(size_t capacity) { return CalcEdgesOffset(capacity) + sizeof(TEdge) * (capacity + 1); }

            static constexpr size_t CalcCapacity() {
                if (sizeof(THeader) >= PageSize) {
                    return 0;
                }
                size_t capacity = (PageSize - sizeof(THeader)) / (sizeof(TKey) + sizeof(TEdge)); // upper estimate
                while (capacity > 0) {
                    if (CalcEdgesEnd(capacity) <= PageSize) {
                        break;
                    }
                    --capacity;
                }
                return capacity;
            }

        public:
            enum : size_t {
                Capacity = CalcCapacity(),
                KeysOffset = CalcKeysOffset(Capacity),
                EdgesOffset = CalcEdgesOffset(Capacity),
            };
        };

        template<class THeader, class TKey, class TValue>
        class TLeafLayout {
            static constexpr size_t CalcKeysOffset(size_t capacity) { Y_UNUSED(capacity); return AlignSizeUp(sizeof(THeader), alignof(TKey)); }
            static constexpr size_t CalcKeysEnd(size_t capacity) { return CalcKeysOffset(capacity) + sizeof(TKey) * capacity; }
            static constexpr size_t CalcValuesOffset(size_t capacity) { return AlignSizeUp(CalcKeysEnd(capacity), alignof(TValue)); }
            static constexpr size_t CalcValuesEnd(size_t capacity) { return CalcValuesOffset(capacity) + sizeof(TValue) * capacity; }

            static constexpr size_t CalcCapacity() {
                if (sizeof(THeader) >= PageSize) {
                    return 0;
                }
                size_t capacity = (PageSize - sizeof(THeader)) / (sizeof(TKey) + sizeof(TValue)); // upper estimate
                while (capacity > 0) {
                    if (CalcValuesEnd(capacity) <= PageSize) {
                        break;
                    }
                    --capacity;
                }
                return capacity;
            }

        public:
            enum : size_t {
                Capacity = CalcCapacity(),
                KeysOffset = CalcKeysOffset(Capacity),
                ValuesOffset = CalcValuesOffset(Capacity),
            };
        };
    };

}   // namespace NKikimr
