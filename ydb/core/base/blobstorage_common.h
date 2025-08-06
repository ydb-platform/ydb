#pragma once

#include <ydb/core/base/id_wrapper.h>

namespace NKikimr {

    struct TGroupIdTag;
    using TGroupId = TIdWrapper<ui32, TGroupIdTag>;

    struct TBridgePileTag;
    struct TBridgePileId : TIdWrapper<ui32, TBridgePileTag> {
        TBridgePileId() = default;
        TBridgePileId(const TBridgePileId&) = default;

        TBridgePileId& operator =(const TBridgePileId&) = default;

        using TIdWrapper::ToString;

        static constexpr TBridgePileId Min() { return FromValue(::Min<Type>()); }
        static constexpr TBridgePileId Max() { return FromValue(::Max<Type>()); }

        friend std::strong_ordering operator <=>(const TBridgePileId&, const TBridgePileId&) = default;

        template<typename TProto>
        void CopyToProto(TProto *message, void (TProto::*pfn)(Type value)) const {
            if (*this) {
                std::invoke(pfn, message, GetRawId());
            }
        }

        template<typename TType, typename TProto>
        static constexpr TBridgePileId FromProto(const TType *message, TProto (TType::*pfn)() const) {
            return FromValue(std::invoke(pfn, message));
        }

        static constexpr TBridgePileId FromPileIndex(size_t index) noexcept { return FromValue(index + 1); }
        static constexpr TBridgePileId FromLocalDb(ui32 value) noexcept { return FromValue(value); }

        explicit operator bool() const { return GetRawId() != 0; }

        size_t GetPileIndex() const {
            Y_DEBUG_ABORT_UNLESS(*this);
            return GetRawId() - 1;
        }

        ui32 GetLocalDb() const { return GetRawId(); }
        size_t Hash() const { return GetRawId(); }

    private:
        explicit constexpr TBridgePileId(Type value) : TIdWrapper(TIdWrapper::FromValue(value)) {}

        Type GetRawId() const {
            return TIdWrapper::GetRawId();
        }

        static constexpr TBridgePileId FromValue(Type value) noexcept {
            return TBridgePileId(value);
        }
    };
    
    inline bool IsDynamicGroup(TGroupId groupId) {
        return groupId.GetRawId() & 0x80000000;
    }
}

template<>
struct std::hash<NKikimr::TBridgePileId> { size_t operator()(NKikimr::TBridgePileId id) const { return id.Hash(); } };

template<>
struct THash<NKikimr::TBridgePileId> : std::hash<NKikimr::TBridgePileId> {};
