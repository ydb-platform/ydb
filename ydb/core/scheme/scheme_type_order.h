#pragma once 
 
#include "scheme_type_id.h" 
 
namespace NKikimr { 
namespace NScheme { 
 
enum class EOrder : ui16 { 
    Ascending = 0, 
    Descending = 1, 
}; 
 
class TTypeIdOrder { 
public: 
    TTypeIdOrder() noexcept { 
        TypeId_ = 0; 
        Descending_ = 0; 
    } 
 
    /** 
     * This allows implicit conversions from TTypeId 
     */ 
    TTypeIdOrder(TTypeId typeId, EOrder order = EOrder::Ascending) noexcept { 
        Set(typeId, order); 
    } 
 
    /** 
     * This allows implicit conversions from TTypeId 
     */ 
    TTypeIdOrder& operator=(TTypeId typeId) noexcept { 
        return Set(typeId); 
    } 
 
    TTypeId GetTypeId() const noexcept { return TypeId_; } 
    EOrder GetOrder() const noexcept { return EOrder(Descending_); } 
    bool IsAscending() const noexcept { return !Descending_; } 
    bool IsDescending() const noexcept { return Descending_; } 
 
    TTypeIdOrder& Set(TTypeId typeId, EOrder order = EOrder::Ascending) noexcept { 
        Y_VERIFY(typeId <= 0x7FFF, "Type id is out of bounds"); 
 
        TypeId_ = typeId; 
        Descending_ = ui16(order); 
 
        return *this; 
    } 
 
private: 
    ui16 TypeId_ : 15; 
    ui16 Descending_ : 1; 
}; 
 
} 
} 
