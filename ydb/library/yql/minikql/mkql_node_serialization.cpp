#include "mkql_node_serialization.h"
#include "mkql_node.h"
#include "mkql_node_visitor.h"
#include "mkql_type_ops.h"

#include <ydb/library/yql/minikql/pack_num.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/packedtypes/zigzag.h>

#include <util/generic/algorithm.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NDetail;

namespace {
    static const char KindMask = 0x0f;
    static const char TypeMask = 0x7f;
    static_assert(KindMask == char(TType::EKind::ReservedKind), "Kind should be encoded in 4 bit");
    static const char UserMarker1 = 0x10;
    static const char UserMarker2 = 0x20;
    static const char UserMarker3 = 0x40;
    static const char TypeMarker = '\x80';
    static const char SystemMask = KindMask;
    static const char CommandMask = '\xf0';
    static const ui32 NameRefMark = 0x01;
    static const ui32 RequiresNextPass = 0x80000000u;
    static const ui32 AllPassesDone = 0xFFFFFFFFu;

    enum class ESystemCommand {
        Begin = 0x10,
        End = 0x20,
        Ref = 0x30,
        BeginNotImmediate = 0x40,
        LastCommand = 0xf0
    };

    inline ui32 GetBitmapBytes(ui32 count) {
        return (count + 7) / 8;
    }

    inline void SetBitmapBit(char* bitmap, ui32 index) {
        bitmap[index / 8] |= (1 << (index & 7));
    }

    inline bool GetBitmapBit(const char* bitmap, ui32 index) {
        return (bitmap[index / 8] & (1 << (index & 7))) != 0;
    }

    class TPrepareWriteNodeVisitor : public TEmptyNodeVisitor {
    public:
        using TEmptyNodeVisitor::Visit;

        void Visit(TStructType& node) override {
            for (ui32 i = 0, e = node.GetMembersCount(); i < e; ++i) {
                auto memberName = node.GetMemberNameStr(i);
                AddName(memberName);
            }
        }

        void Visit(TCallableType& node) override {
            auto name = node.GetNameStr();
            AddName(name);
        }

        void Visit(TResourceType& node) override {
            auto name = node.GetTagStr();
            AddName(name);
        }

        THashMap<TInternName, ui32>& GetNames() {
            return Names;
        }

        TVector<TInternName>& GetNameOrder() {
            return NameOrder;
        }


    private:
        void AddName(const TInternName& name) {
            auto iter = Names.emplace(name, 0);
            if (iter.second) {
                NameOrder.emplace_back(name);
            }
            ++iter.first->second;
        }

    private:
        THashMap<TInternName, ui32> Names;
        TVector<TInternName> NameOrder;
    };

    class TWriter {
    public:
        friend class TPreVisitor;
        friend class TPostVisitor;

        class TPreVisitor : public INodeVisitor {
        public:
            TPreVisitor(TWriter& owner)
                : Owner(owner)
                , IsProcessed0(false)
            {
            }

            void Visit(TTypeType& node) override {
                Y_UNUSED(node);
                Owner.Write(TypeMarker | (char)TType::EKind::Type);
                IsProcessed0 = true;
            }

            void Visit(TVoidType& node) override {
                Y_UNUSED(node);
                Owner.Write(TypeMarker | (char)TType::EKind::Void);
                IsProcessed0 = true;
            }

            void Visit(TNullType& node) override {
                Y_UNUSED(node);
                Owner.Write(TypeMarker | (char)TType::EKind::Null);
                IsProcessed0 = true;
            }

            void Visit(TEmptyListType& node) override {
                Y_UNUSED(node);
                Owner.Write(TypeMarker | (char)TType::EKind::EmptyList);
                IsProcessed0 = true;
            }

            void Visit(TEmptyDictType& node) override {
                Y_UNUSED(node);
                Owner.Write(TypeMarker | (char)TType::EKind::EmptyDict);
                IsProcessed0 = true;
            }

            void Visit(TDataType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Data);
                Owner.WriteVar32(node.GetSchemeType());
                if (NUdf::TDataType<NUdf::TDecimal>::Id == node.GetSchemeType()) {
                    const auto& params = static_cast<TDataDecimalType&>(node).GetParams();
                    Owner.Write(params.first);
                    Owner.Write(params.second);
                }
                IsProcessed0 = false;
            }

            void Visit(TPgType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Pg);
                Owner.WriteVar32(node.GetTypeId());
                IsProcessed0 = false;
            }

            void Visit(TStructType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Struct);
                Owner.WriteVar32(node.GetMembersCount());
                for (ui32 i = node.GetMembersCount(); i-- > 0;) {
                    auto memberType = node.GetMemberType(i);
                    Owner.AddChildNode(*memberType);
                }

                IsProcessed0 = false;
            }

            void Visit(TListType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::List);
                auto itemType = node.GetItemType();
                Owner.AddChildNode(*itemType);
                IsProcessed0 = false;
            }

            void Visit(TStreamType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Stream);
                auto itemType = node.GetItemType();
                Owner.AddChildNode(*itemType);
                IsProcessed0 = false;
            }

            void Visit(TFlowType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Flow);
                auto itemType = node.GetItemType();
                Owner.AddChildNode(*itemType);
                IsProcessed0 = false;
            }

            void Visit(TBlockType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Block);
                auto itemType = node.GetItemType();
                Owner.AddChildNode(*itemType);
                IsProcessed0 = false;
            }

            void Visit(TMultiType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Multi);
                Owner.WriteVar32(node.GetElementsCount());
                for (ui32 i = node.GetElementsCount(); i-- > 0;) {
                    auto elementType = node.GetElementType(i);
                    Owner.AddChildNode(*elementType);
                }

                IsProcessed0 = false;
            }

            void Visit(TTaggedType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Tagged);
                auto baseType = node.GetBaseType();
                Owner.AddChildNode(*baseType);
                IsProcessed0 = false;
            }

            void Visit(TOptionalType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Optional);
                auto itemType = node.GetItemType();
                Owner.AddChildNode(*itemType);
                IsProcessed0 = false;
            }

            void Visit(TDictType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Dict);
                auto keyType = node.GetKeyType();
                auto payloadType = node.GetPayloadType();
                Owner.AddChildNode(*payloadType);
                Owner.AddChildNode(*keyType);
                IsProcessed0 = false;
            }

            void Visit(TCallableType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Callable
                    | (node.IsMergeDisabled() ? UserMarker2 : 0) | (node.GetPayload() ? UserMarker3 : 0));
                Owner.WriteVar32(node.GetArgumentsCount());
                auto returnType = node.GetReturnType();
                if (node.GetPayload()) {
                    Owner.AddChildNode(*node.GetPayload());
                }

                for (ui32 i = node.GetArgumentsCount(); i-- > 0;) {
                    auto argumentType = node.GetArgumentType(i);
                    Owner.AddChildNode(*argumentType);
                }

                Owner.AddChildNode(*returnType);
                IsProcessed0 = false;
            }

            void Visit(TAnyType& node) override {
                Y_UNUSED(node);
                Owner.Write(TypeMarker | (char)TType::EKind::Any);
                IsProcessed0 = true;
            }

            void Visit(TTupleType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Tuple);
                Owner.WriteVar32(node.GetElementsCount());
                for (ui32 i = node.GetElementsCount(); i-- > 0;) {
                    auto elementType = node.GetElementType(i);
                    Owner.AddChildNode(*elementType);
                }

                IsProcessed0 = false;
            }

            void Visit(TResourceType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Resource);
                auto tag = node.GetTagStr();
                Owner.WriteName(tag);
                IsProcessed0 = false;
            }

            void Visit(TVariantType& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write(TypeMarker | (char)TType::EKind::Variant);
                auto underlyingType = node.GetUnderlyingType();
                Owner.AddChildNode(*underlyingType);
                IsProcessed0 = false;
            }

            void Visit(TVoid& node) override {
                Y_UNUSED(node);
                Owner.Write((char)TType::EKind::Void);
                IsProcessed0 = true;
            }

            void Visit(TNull& node) override {
                Y_UNUSED(node);
                Owner.Write((char)TType::EKind::Null);
                IsProcessed0 = true;
            }

            void Visit(TEmptyList& node) override {
                Y_UNUSED(node);
                Owner.Write((char)TType::EKind::EmptyList);
                IsProcessed0 = true;
            }

            void Visit(TEmptyDict& node) override {
                Y_UNUSED(node);
                Owner.Write((char)TType::EKind::EmptyDict);
                IsProcessed0 = true;
            }

            void Visit(TDataLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write((char)TType::EKind::Data);
                auto type = node.GetType();
                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TStructLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write((char)TType::EKind::Struct);
                auto type = node.GetType();
                Y_DEBUG_ABORT_UNLESS(node.GetValuesCount() == type->GetMembersCount());
                for (ui32 i = node.GetValuesCount(); i-- > 0; ) {
                    auto value = node.GetValue(i);
                    Owner.AddChildNode(*value.GetNode());
                }

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TListLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write((char)TType::EKind::List);
                auto type = node.GetType();
                Owner.WriteVar32(node.GetItemsCount());

                for (ui32 i = node.GetItemsCount(); i > 0; --i) {
                    auto item = node.GetItems()[i - 1];
                    Owner.AddChildNode(*item.GetNode());
                }

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TOptionalLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                TRuntimeNode item;
                if (node.HasItem())
                    item = node.GetItem();

                Owner.Write((char)TType::EKind::Optional | (item.GetNode() ? UserMarker1 : 0) | (item.IsImmediate() ? UserMarker2 : 0));
                auto type = node.GetType();
                if (item.GetNode()) {
                    Owner.AddChildNode(*item.GetNode());
                }

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TDictLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write((char)TType::EKind::Dict);
                auto type = node.GetType();
                Owner.WriteVar32(node.GetItemsCount());
                for (ui32 i = node.GetItemsCount(); i-- > 0;) {
                    auto item = node.GetItem(i);
                    Owner.AddChildNode(*item.second.GetNode());
                    Owner.AddChildNode(*item.first.GetNode());
                }

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TCallable& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write((char)TType::EKind::Callable | (node.HasResult() ? UserMarker1 : 0) |
                    ((node.GetUniqueId() != 0) ? UserMarker2 : 0));

                auto type = node.GetType();
                if (node.HasResult()) {
                    auto result = node.GetResult();
                    Owner.AddChildNode(*result.GetNode());
                } else {
                    Y_DEBUG_ABORT_UNLESS(node.GetInputsCount() == type->GetArgumentsCount());
                    for (ui32 i = node.GetInputsCount(); i-- > 0;) {
                        auto input = node.GetInput(i);
                        Owner.AddChildNode(*input.GetNode());
                    }
                }

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TAny& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                TRuntimeNode item;
                if (node.HasItem())
                    item = node.GetItem();

                Owner.Write((char)TType::EKind::Any | (item.GetNode() ? UserMarker1 : 0) | (item.IsImmediate() ? UserMarker2 : 0));
                if (item.GetNode()) {
                    Owner.AddChildNode(*item.GetNode());
                }

                IsProcessed0 = false;
            }

            void Visit(TTupleLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                Owner.Write((char)TType::EKind::Tuple);
                auto type = node.GetType();
                Y_DEBUG_ABORT_UNLESS(node.GetValuesCount() == type->GetElementsCount());
                for (ui32 i = node.GetValuesCount(); i-- > 0;) {
                    auto value = node.GetValue(i);
                    Owner.AddChildNode(*value.GetNode());
                }

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            void Visit(TVariantLiteral& node) override {
                if (node.GetCookie() != 0) {
                    Owner.WriteReference(node);
                    IsProcessed0 = true;
                    return;
                }

                TRuntimeNode item = node.GetItem();

                Owner.Write((char)TType::EKind::Variant | (item.IsImmediate() ? UserMarker1 : 0));
                auto type = node.GetType();
                Owner.AddChildNode(*item.GetNode());

                Owner.AddChildNode(*type);
                IsProcessed0 = false;
            }

            bool IsProcessed() const {
                return IsProcessed0;
            }

        private:
            TWriter& Owner;
            bool IsProcessed0;
        };

        class TPostVisitor : public INodeVisitor {
        public:
            TPostVisitor(TWriter& owner)
                : Owner(owner)
            {
            }

            void Visit(TTypeType& node) override {
                Y_UNUSED(node);
            }

            void Visit(TVoidType& node) override {
                Y_UNUSED(node);
            }

            void Visit(TNullType& node) override {
                Y_UNUSED(node);
            }

            void Visit(TEmptyListType& node) override {
                Y_UNUSED(node);
            }

            void Visit(TEmptyDictType& node) override {
                Y_UNUSED(node);
            }

            void Visit(TDataType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TPgType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TStructType& node) override {
                for (ui32 i = 0, e = node.GetMembersCount(); i < e; ++i) {
                    auto memberName = node.GetMemberNameStr(i);
                    Owner.WriteName(memberName);
                }

                Owner.RegisterReference(node);
            }

            void Visit(TListType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TStreamType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TFlowType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TBlockType& node) override {
                Owner.Write(static_cast<ui8>(node.GetShape()));
                Owner.RegisterReference(node);
            }

            void Visit(TMultiType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TTaggedType& node) override {
                auto tag = node.GetTagStr();
                Owner.WriteName(tag);
                Owner.RegisterReference(node);
            }

            void Visit(TOptionalType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TDictType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TCallableType& node) override {
                auto name = node.GetNameStr();
                Owner.WriteName(name);
                Owner.WriteVar32(node.GetOptionalArgumentsCount());
                Owner.RegisterReference(node);
            }

            void Visit(TAnyType& node) override {
                Y_UNUSED(node);
            }

            void Visit(TTupleType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TResourceType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TVariantType& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TVoid& node) override {
                Y_UNUSED(node);
            }

            void Visit(TNull& node) override {
                Y_UNUSED(node);
            }

            void Visit(TEmptyList& node) override {
                Y_UNUSED(node);
            }

            void Visit(TEmptyDict& node) override {
                Y_UNUSED(node);
            }

            void Visit(TDataLiteral& node) override {
                const auto type = node.GetType();
                if (type->GetSchemeType() != 0) {
                    const auto& value = node.AsValue();
                    switch (type->GetSchemeType()) {
                    case NUdf::TDataType<bool>::Id:
                        Owner.Write(value.Get<bool>());
                        break;
                    case NUdf::TDataType<ui8>::Id:
                        Owner.Write(value.Get<ui8>());
                        break;
                    case NUdf::TDataType<i8>::Id:
                        Owner.Write((ui8)value.Get<i8>());
                        break;
                    case NUdf::TDataType<i16>::Id:
                        Owner.WriteVar32(ZigZagEncode(value.Get<i16>()));
                        break;
                    case NUdf::TDataType<ui16>::Id:
                        Owner.WriteVar32(value.Get<ui16>());
                        break;
                    case NUdf::TDataType<i32>::Id:
                        Owner.WriteVar32(ZigZagEncode(value.Get<i32>()));
                        break;
                    case NUdf::TDataType<ui32>::Id:
                        Owner.WriteVar32(value.Get<ui32>());
                        break;
                    case NUdf::TDataType<float>::Id: {
                            const auto v = value.Get<float>();
                            Owner.WriteMany(&v, sizeof(v));
                            break;
                        }
                    case NUdf::TDataType<i64>::Id:
                        Owner.WriteVar64(ZigZagEncode(value.Get<i64>()));
                        break;
                    case NUdf::TDataType<ui64>::Id:
                        Owner.WriteVar64(value.Get<ui64>());
                        break;
                    case NUdf::TDataType<double>::Id: {
                            const auto v = value.Get<double>();
                            Owner.WriteMany(&v, sizeof(v));
                            break;
                        }
                    case NUdf::TDataType<NUdf::TDate>::Id:
                        Owner.WriteVar32(value.Get<NUdf::TDataType<NUdf::TDate>::TLayout>());
                        break;
                    case NUdf::TDataType<NUdf::TDatetime>::Id:
                        Owner.WriteVar32(value.Get<NUdf::TDataType<NUdf::TDatetime>::TLayout>());
                        break;
                    case NUdf::TDataType<NUdf::TTimestamp>::Id:
                        Owner.WriteVar64(value.Get<NUdf::TDataType<NUdf::TTimestamp>::TLayout>());
                        break;
                    case NUdf::TDataType<NUdf::TTzDate>::Id: {
                        Owner.WriteVar32(value.Get<NUdf::TDataType<NUdf::TTzDate>::TLayout>());
                        Owner.WriteVar32(value.GetTimezoneId());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
                        Owner.WriteVar32(value.Get<NUdf::TDataType<NUdf::TTzDatetime>::TLayout>());
                        Owner.WriteVar32(value.GetTimezoneId());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
                        Owner.WriteVar64(value.Get<NUdf::TDataType<NUdf::TTzTimestamp>::TLayout>());
                        Owner.WriteVar32(value.GetTimezoneId());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TInterval>::Id:
                        Owner.WriteVar64(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TInterval>::TLayout>()));
                        break;
                    case NUdf::TDataType<NUdf::TDate32>::Id:
                        Owner.WriteVar32(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TDate32>::TLayout>()));
                        break;
                    case NUdf::TDataType<NUdf::TDatetime64>::Id:
                        Owner.WriteVar64(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TDatetime64>::TLayout>()));
                        break;
                    case NUdf::TDataType<NUdf::TTimestamp64>::Id:
                        Owner.WriteVar64(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TTimestamp64>::TLayout>()));
                        break;
                    case NUdf::TDataType<NUdf::TInterval64>::Id:
                        Owner.WriteVar64(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TInterval64>::TLayout>()));
                        break;
                    case NUdf::TDataType<NUdf::TTzDate32>::Id: {
                        Owner.WriteVar32(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TTzDate32>::TLayout>()));
                        Owner.WriteVar32(value.GetTimezoneId());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
                        Owner.WriteVar64(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TTzDatetime64>::TLayout>()));
                        Owner.WriteVar32(value.GetTimezoneId());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
                        Owner.WriteVar64(ZigZagEncode(value.Get<NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout>()));
                        Owner.WriteVar32(value.GetTimezoneId());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TUuid>::Id: {
                        const auto v = value.AsStringRef();
                        Owner.WriteMany(v.Data(), v.Size());
                        break;
                    }
                    case NUdf::TDataType<NUdf::TDecimal>::Id:
                        Owner.WriteMany(static_cast<const char*>(value.GetRawPtr()), sizeof(NYql::NDecimal::TInt128) - 1U);
                        break;
                    default: {
                            const auto& buffer = value.AsStringRef();
                            Owner.WriteVar32(buffer.Size());
                            Owner.WriteMany(buffer.Data(), buffer.Size());
                        }
                    }
                }

                Owner.RegisterReference(node);
            }

            void Visit(TStructLiteral& node) override {
                auto type = node.GetType();
                Y_DEBUG_ABORT_UNLESS(node.GetValuesCount() == type->GetMembersCount());
                TStackVec<char> immediateFlags(GetBitmapBytes(node.GetValuesCount()));
                for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
                    auto value = node.GetValue(i);
                    if (value.IsImmediate()) {
                        SetBitmapBit(immediateFlags.data(), i);
                    }
                }

                Owner.WriteMany(immediateFlags.data(), immediateFlags.size());
                Owner.RegisterReference(node);
            }

            void Visit(TListLiteral& node) override {
                TStackVec<char> immediateFlags(GetBitmapBytes(node.GetItemsCount()));
                for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
                    auto item = node.GetItems()[i];
                    if (item.IsImmediate()) {
                        SetBitmapBit(immediateFlags.data(), i);
                    }
                }

                Owner.WriteMany(immediateFlags.data(), immediateFlags.size());
                Owner.RegisterReference(node);
            }

            void Visit(TOptionalLiteral& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TDictLiteral& node) override {
                TStackVec<char> immediateFlags(GetBitmapBytes(node.GetItemsCount() * 2));
                for (ui32 i = 0, e = node.GetItemsCount(); i < e; ++i) {
                    auto item = node.GetItem(i);
                    if (item.first.IsImmediate()) {
                        SetBitmapBit(immediateFlags.data(), 2 * i);
                    }

                    if (item.second.IsImmediate()) {
                        SetBitmapBit(immediateFlags.data(), 2 * i + 1);
                    }
                }

                Owner.WriteMany(immediateFlags.data(), immediateFlags.size());
                Owner.RegisterReference(node);
            }

            void Visit(TCallable& node) override {
                if (node.HasResult()) {
                    Owner.Write(node.GetResult().IsImmediate() ? 1 : 0);
                } else {
                    auto type = node.GetType();
                    Y_DEBUG_ABORT_UNLESS(node.GetInputsCount() == type->GetArgumentsCount());
                    TStackVec<char> immediateFlags(GetBitmapBytes(node.GetInputsCount()));
                    for (ui32 i = 0, e = node.GetInputsCount(); i < e; ++i) {
                        auto input = node.GetInput(i);
                        if (input.IsImmediate()) {
                            SetBitmapBit(immediateFlags.data(), i);
                        }
                    }

                    Owner.WriteMany(immediateFlags.data(), immediateFlags.size());
                }

                if (node.GetUniqueId() != 0)
                    Owner.WriteVar32(node.GetUniqueId());

                Owner.RegisterReference(node);
            }

            void Visit(TAny& node) override {
                Owner.RegisterReference(node);
            }

            void Visit(TTupleLiteral& node) override {
                auto type = node.GetType();
                Y_DEBUG_ABORT_UNLESS(node.GetValuesCount() == type->GetElementsCount());
                TStackVec<char> immediateFlags(GetBitmapBytes(node.GetValuesCount()));
                for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
                    auto value = node.GetValue(i);
                    if (value.IsImmediate()) {
                        SetBitmapBit(immediateFlags.data(), i);
                    }
                }

                Owner.WriteMany(immediateFlags.data(), immediateFlags.size());
                Owner.RegisterReference(node);
            }

            void Visit(TVariantLiteral& node) override {
                Owner.WriteVar32(node.GetIndex());
                Owner.RegisterReference(node);
            }

        private:
            TWriter& Owner;
        };

        TWriter(THashMap<TInternName, ui32>& names, TVector<TInternName>& nameOrder)
        {
            Names.swap(names);
            NameOrder.swap(nameOrder);
        }

        void Write(TRuntimeNode node) {
            Begin(node.IsImmediate());
            TPreVisitor preVisitor(*this);
            TPostVisitor postVisitor(*this);
            Stack.push_back(std::make_pair(node.GetNode(), false));
            while (!Stack.empty()) {
                auto& nodeAndFlag = Stack.back();
                if (!nodeAndFlag.second) {
                    nodeAndFlag.second = true;
                    auto prevSize = Stack.size();
                    nodeAndFlag.first->Accept(preVisitor);
                    if (preVisitor.IsProcessed()) { // ref or small node, some nodes have been added
                        Y_DEBUG_ABORT_UNLESS(prevSize == Stack.size());
                        Stack.pop_back();
                        continue;
                    }

                    Y_DEBUG_ABORT_UNLESS(prevSize <= Stack.size());
                } else {
                    auto prevSize = Stack.size();
                    nodeAndFlag.first->Accept(postVisitor);
                    Y_DEBUG_ABORT_UNLESS(prevSize == Stack.size());
                    Stack.pop_back();
                }
            }

            End();
        }

        TString GetOutput() const {
            return Out;
        }

    private:
        void Begin(bool isImmediate) {
            Write(SystemMask | (isImmediate ? (char)ESystemCommand::Begin : (char)ESystemCommand::BeginNotImmediate));
            for (auto it = Names.begin(); it != Names.end();) {
                if (it->second <= 1) {
                    Names.erase(it++);
                } else {
                    ++it;
                }
            }

            WriteVar32(Names.size());
            ui32 nameIndex = 0;
            for (const auto& orderedName: NameOrder) {
                auto it = Names.find(orderedName);
                if (it == Names.end()) {
                    continue;
                }
                const auto& name = it->first;
                WriteVar32(name.Str().size());
                WriteMany(name.Str().data(), name.Str().size());
                it->second = nameIndex++;
            }

            Y_DEBUG_ABORT_UNLESS(nameIndex == Names.size());
        }

        void End() {
            Write(SystemMask | (char)ESystemCommand::End);
        }

        void AddChildNode(TNode& node) {
            Stack.push_back(std::make_pair(&node, false));
        }

        void RegisterReference(TNode& node) {
            Y_DEBUG_ABORT_UNLESS(node.GetCookie() == 0);
            node.SetCookie(++ReferenceCount);
        }

        void WriteReference(TNode& node) {
            Write(SystemMask | (char)ESystemCommand::Ref);
            Y_DEBUG_ABORT_UNLESS(node.GetCookie() != 0);
            Y_DEBUG_ABORT_UNLESS(node.GetCookie() <= ReferenceCount);
            WriteVar32(node.GetCookie() - 1);
        }

        void WriteName(TInternName name) {
            auto it = Names.find(name);
            if (it == Names.end()) {
                WriteVar32(name.Str().size() << 1);
                WriteMany(name.Str().data(), name.Str().size());
            } else {
                WriteVar32((it->second << 1) | NameRefMark);
            }
        }

        Y_FORCE_INLINE void Write(char c) {
            Out.append(c);
        }

        Y_FORCE_INLINE void WriteMany(const void* buf, size_t len) {
            Out.AppendNoAlias((const char*)buf, len);
        }

        Y_FORCE_INLINE void WriteVar32(ui32 value) {
            char buf[MAX_PACKED32_SIZE];
            Out.AppendNoAlias(buf, Pack32(value, buf));
        }

        Y_FORCE_INLINE void WriteVar64(ui64 value) {
            char buf[MAX_PACKED64_SIZE];
            Out.AppendNoAlias(buf, Pack64(value, buf));
        }

    private:
        TString Out;
        ui32 ReferenceCount = 0;
        THashMap<TInternName, ui32> Names;
        TVector<TInternName> NameOrder;
        TVector<std::pair<TNode*, bool>> Stack;
    };

    class TReader {
    public:
        TReader(const TStringBuf& buffer, const TTypeEnvironment& env)
            : Current(buffer.data())
            , End(buffer.data() + buffer.size())
            , Env(env)
        {
        }

        TRuntimeNode Deserialize() {
            char header = Read();
            if ((header & SystemMask) != SystemMask)
                ThrowCorrupted();

            if ((header & CommandMask) != (char)ESystemCommand::Begin
                && (header & CommandMask) != (char)ESystemCommand::BeginNotImmediate)
                ThrowCorrupted();

            const bool notImmediate = ((header & CommandMask) == (char)ESystemCommand::BeginNotImmediate);
            const ui32 namesCount = ReadVar32();
            for (ui32 i = 0; i < namesCount; ++i) {
                LoadName();
            }

            const char* lastPos = Current;
            CtxStack.push_back(TNodeContext());
            while (!CtxStack.empty()) {
                auto& last = CtxStack.back();
                if (!last.Start) {
                    last.Start = Current;
                } else {
                    Current = last.Start;
                }

                if (last.NextPass == AllPassesDone) {
                    Y_DEBUG_ABORT_UNLESS(last.ChildCount <= NodeStack.size());
                    Reverse(NodeStack.end() - last.ChildCount, NodeStack.end());
                    auto newNode = ReadNode();
                    if (Current > lastPos) {
                        lastPos = Current;
                    }

                    NodeStack.push_back(std::make_pair(newNode, Current));
                    CtxStack.pop_back();
                    continue;
                }

                const ui32 res = TryReadNode(last.NextPass);
                if (res & RequiresNextPass) {
                    ++last.NextPass;
                } else {
                    last.NextPass = AllPassesDone;
                }

                const ui32 childCount = res & ~RequiresNextPass;
                last.ChildCount += childCount;
                if (childCount != 0) {
                    CtxStack.insert(CtxStack.end(), childCount, TNodeContext());
                } else {
                    Y_DEBUG_ABORT_UNLESS(res == 0);
                }
            }

            Current = lastPos;
            if (NodeStack.size() != 1)
                ThrowCorrupted();

            TNode* node = PopNode();

            char footer = Read();
            if (footer != (SystemMask | (char)ESystemCommand::End))
                ThrowCorrupted();

            if (Current != End)
                ThrowCorrupted();

            return TRuntimeNode(node, !notImmediate);
        }

    private:
        [[noreturn]] static void ThrowNoData() {
            THROW yexception() << "No more data in buffer";
        }

        [[noreturn]] static void ThrowCorrupted() {
            THROW yexception() << "Serialized data is corrupted";
        }

        Y_FORCE_INLINE char Read() {
            if (Current == End)
                ThrowNoData();

            return *Current++;
        }

        Y_FORCE_INLINE const char* ReadMany(ui32 count) {
            if (Current + count > End)
                ThrowNoData();

            const char* result = Current;
            Current += count;
            return result;
        }

        Y_FORCE_INLINE ui32 ReadVar32() {
            ui32 result = 0;
            size_t count = Unpack32(Current, End - Current, result);
            if (!count) {
                ThrowCorrupted();
            }
            Current += count;
            return result;
        }

        Y_FORCE_INLINE ui64 ReadVar64() {
            ui64 result = 0;
            size_t count = Unpack64(Current, End - Current, result);
            if (!count) {
                ThrowCorrupted();
            }
            Current += count;
            return result;
        }

        // eats header and returns count of child nodes
        ui32 TryReadNode(ui32 pass) {
            char code = Read();
            if ((code & KindMask) == SystemMask) {
                return TryReadSystemObject(code);
            } else if (code & TypeMarker) {
                return TryReadType(code);
            } else {
                return TryReadLiteral(code, pass);
            }
        }

        TNode* PopNode() {
            if (NodeStack.empty())
                ThrowCorrupted();

            auto nodeAndFinish = NodeStack.back();
            NodeStack.pop_back();
            Current = nodeAndFinish.second;
            return nodeAndFinish.first;
        }

        TNode* PeekNode(ui32 index) {
            if (index >= NodeStack.size())
                ThrowCorrupted();

            auto nodeAndFinish = NodeStack[NodeStack.size() - 1 - index];
            Current = nodeAndFinish.second;
            return nodeAndFinish.first;
        }

        TNode* ReadNode() {
            char code = Read();
            if ((code & KindMask) == SystemMask) {
                return ReadSystemObject(code);
            } else if (code & TypeMarker) {
                return ReadType(code);
            } else {
                return ReadLiteral(code);
            }
        }

        ui32 TryReadType(char code) {
            switch ((TType::EKind)(code & KindMask)) {
            case TType::EKind::Type:
                return 0;
            case TType::EKind::Void: // and EmptyList, EmptyDict
                return 0;
            case TType::EKind::Data:
                return 0;
            case TType::EKind::Struct:
                return TryReadStructType();
            case TType::EKind::List:
                return 1;
            case TType::EKind::Stream:
                return 1;
            case TType::EKind::Optional: // and Tagged
                return 1;
            case TType::EKind::Dict:
                return 2;
            case TType::EKind::Callable:
                return TryReadCallableType(code);
            case TType::EKind::Any:
                return 0;
            case TType::EKind::Tuple: // and Multi
                return TryReadTupleOrMultiType();
            case TType::EKind::Resource:
                return 0;
            case TType::EKind::Variant:
                return 1;
            case TType::EKind::Flow:
                return 1;
            case TType::EKind::Null:
                return 0;
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadType(char code) {
            switch ((TType::EKind)(code & KindMask)) {
            case TType::EKind::Type:
                return ReadTypeType();
            case TType::EKind::Void:
                return ReadVoidOrEmptyListOrEmptyDictType(code);
            case TType::EKind::Data:
                return ReadDataOrPgType(code);
            case TType::EKind::Struct:
                return ReadStructType();
            case TType::EKind::List:
                return ReadListType();
            case TType::EKind::Stream:
                return ReadStreamType();
            case TType::EKind::Optional:
                return ReadOptionalOrTaggedType(code);
            case TType::EKind::Dict:
                return ReadDictType();
            case TType::EKind::Callable:
                return ReadCallableType(code);
            case TType::EKind::Any:
                return ReadAnyType();
            case TType::EKind::Tuple:
                return ReadTupleOrMultiType(code);
            case TType::EKind::Resource:
                return ReadResourceType();
            case TType::EKind::Variant:
                return ReadVariantType();
            case TType::EKind::Flow:
                return ReadFlowOrBlockType(code);
            case TType::EKind::Null:
                return ReadNullType();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadTypeType() {
            auto node = Env.GetTypeOfTypeLazy();
            return node;
        }

        TNode* ReadVoidOrEmptyListOrEmptyDictType(char code) {
            switch ((TType::EKind)(code & TypeMask)) {
            case TType::EKind::Void: return Env.GetTypeOfVoidLazy();
            case TType::EKind::EmptyList: return Env.GetTypeOfEmptyListLazy();
            case TType::EKind::EmptyDict: return Env.GetTypeOfEmptyDictLazy();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadNullType() {
            auto node = Env.GetTypeOfNullLazy();
            return node;
        }

        TNode* ReadDataType() {
            const auto schemeType = ReadVar32();
            if (NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
                const ui8 precision = Read();
                const ui8 scale = Read();
                Nodes.emplace_back(TDataDecimalType::Create(precision, scale, Env));
            } else {
                Nodes.emplace_back(TDataType::Create(static_cast<NUdf::TDataTypeId>(schemeType), Env));
            }
            return Nodes.back();
        }

        TNode* ReadPgType() {
            const auto typeId = ReadVar32();
            Nodes.emplace_back(TPgType::Create(typeId, Env));
            return Nodes.back();
        }

        ui32 TryReadKeyType(char code) {
            const bool isRoot = (code & UserMarker1) != 0;
            const bool hasStaticValue = (code & UserMarker2) != 0;
            if (isRoot) {
                return 0;
            } else {
                return hasStaticValue ? 2 : 1;
            }
        }

        ui32 TryReadStructType() {
            const ui32 membersCount = ReadVar32();
            return membersCount;
        }

        ui32 TryReadTupleOrMultiType() {
            const ui32 elementsCount = ReadVar32();
            return elementsCount;
        }

        TNode* ReadStructType() {
            const ui32 membersCount = ReadVar32();
            TStackVec<TStructMember> members(membersCount);
            for (ui32 i = 0; i < membersCount; ++i) {
                auto memberTypeNode = PopNode();
                if (memberTypeNode->GetType()->GetKind() != TType::EKind::Type)
                    ThrowCorrupted();
                members[i].Type = static_cast<TType*>(memberTypeNode);
            }

            for (ui32 i = 0; i < membersCount; ++i) {
                members[i].Name = ReadName();
            }

            auto node = TStructType::Create(membersCount, members.data(), Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadTupleOrMultiType(char code) {
            const ui32 elementsCount = ReadVar32();
            TStackVec<TType*> elements(elementsCount);
            for (ui32 i = 0; i < elementsCount; ++i) {
                auto elementTypeNode = PopNode();
                if (elementTypeNode->GetType()->GetKind() != TType::EKind::Type)
                    ThrowCorrupted();
                auto elementType = static_cast<TType*>(elementTypeNode);
                elements[i] = elementType;
            }

            TNode* node = nullptr;
            switch ((TType::EKind)(code & TypeMask)) {
                case TType::EKind::Tuple:
                    node = TTupleType::Create(elementsCount, elements.data(), Env);
                    break;
                case TType::EKind::Multi:
                    node = TMultiType::Create(elementsCount, elements.data(), Env);
                    break;
                default:
                    ThrowCorrupted();
            }

            Nodes.push_back(node);
            return node;
        }

        TNode* ReadListType() {
            auto itemTypeNode = PopNode();
            if (itemTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto itemType = static_cast<TType*>(itemTypeNode);
            auto node = TListType::Create(itemType, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadStreamType() {
            auto itemTypeNode = PopNode();
            if (itemTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto itemType = static_cast<TType*>(itemTypeNode);
            auto node = TStreamType::Create(itemType, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadFlowOrBlockType(char code) {
            switch ((TType::EKind)(code & TypeMask)) {
                case TType::EKind::Flow: return ReadFlowType();
                case TType::EKind::Block: return ReadBlockType();
                default:
                    ThrowCorrupted();
            }
        }

        TNode* ReadFlowType() {
            auto itemTypeNode = PopNode();
            if (itemTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto itemType = static_cast<TType*>(itemTypeNode);
            auto node = TFlowType::Create(itemType, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadBlockType() {
            auto itemTypeNode = PopNode();
            if (itemTypeNode->GetType()->GetKind() != TType::EKind::Type) {
                ThrowCorrupted();
            }

            const auto shapeChar = Read();
            if (shapeChar != static_cast<char>(TBlockType::EShape::Scalar) &&
                shapeChar != static_cast<char>(TBlockType::EShape::Many))
            {
                ThrowCorrupted();
            }
            const auto shape = static_cast<TBlockType::EShape>(shapeChar);

            auto itemType = static_cast<TType*>(itemTypeNode);
            auto node = TBlockType::Create(itemType, shape, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadTaggedType() {
            auto baseTypeNode = PopNode();
            if (baseTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto tag = ReadName();
            auto baseType = static_cast<TType*>(baseTypeNode);
            auto node = TTaggedType::Create(baseType, tag, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadOptionalType() {
            auto itemTypeNode = PopNode();
            if (itemTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto itemType = static_cast<TType*>(itemTypeNode);
            auto node = TOptionalType::Create(itemType, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadOptionalOrTaggedType(char code) {
            switch ((TType::EKind)(code & TypeMask)) {
            case TType::EKind::Optional: return ReadOptionalType();
            case TType::EKind::Tagged: return ReadTaggedType();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadDataOrPgType(char code) {
            switch ((TType::EKind)(code & TypeMask)) {
            case TType::EKind::Data: return ReadDataType();
            case TType::EKind::Pg: return ReadPgType();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadDictType() {
            auto keyTypeNode = PopNode();
            if (keyTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto keyType = static_cast<TType*>(keyTypeNode);
            auto payloadTypeNode = PopNode();
            if (payloadTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto payloadType = static_cast<TType*>(payloadTypeNode);
            auto node = TDictType::Create(keyType, payloadType, Env);
            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadCallableType(char code) {
            const bool hasPayload = (code & UserMarker3) != 0;
            const ui32 argumentsCount = ReadVar32();
            return 1 + argumentsCount + (hasPayload ? 1 : 0);
        }

        TNode* ReadCallableType(char code) {
            const bool isMergeDisabled = (code & UserMarker2) != 0;
            const bool hasPayload = (code & UserMarker3) != 0;
            const ui32 argumentsCount = ReadVar32();

            auto returnTypeNode = PopNode();
            if (returnTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();
            auto returnType = static_cast<TType*>(returnTypeNode);

            TStackVec<TType*> arguments(argumentsCount);
            for (ui32 i = 0; i < argumentsCount; ++i) {
                auto argumentTypeNode = PopNode();
                if (argumentTypeNode->GetType()->GetKind() != TType::EKind::Type)
                    ThrowCorrupted();

                auto argumentType = static_cast<TType*>(argumentTypeNode);
                arguments[i] = argumentType;
            }

            TNode* payload = nullptr;
            if (hasPayload) {
                payload = PopNode();
            }

            auto name = ReadName();
            const ui32 optArgsCount = ReadVar32();
            auto node = TCallableType::Create(returnType, name, argumentsCount, arguments.data(), payload, Env);
            if (isMergeDisabled)
                node->DisableMerge();
            node->SetOptionalArgumentsCount(optArgsCount);

            Nodes.push_back(node);
            return node;
        }

        TNode* ReadAnyType() {
            auto node = Env.GetAnyTypeLazy();
            return node;
        }

        TNode* ReadResourceType() {
            auto tag = ReadName();
            auto node = TResourceType::Create(tag, Env);
            Nodes.push_back(node);
            return node;
        }

        TNode* ReadVariantType() {
            auto underlyingTypeNode = PopNode();
            if (underlyingTypeNode->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            auto underlyingType = static_cast<TType*>(underlyingTypeNode);
            auto node = TVariantType::Create(underlyingType, Env);
            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadLiteral(char code, ui32 pass) {
            switch ((TType::EKind)(code & KindMask)) {
            case TType::EKind::Type:
                ThrowCorrupted();
                break;
            case TType::EKind::Void:
                return 0;
            case TType::EKind::Data:
                return 1;
            case TType::EKind::Struct:
                return TryReadStructLiteral(pass);
            case TType::EKind::List:
                return TryReadListLiteral();
            case TType::EKind::Optional:
                return TryReadOptionalLiteral(code);
            case TType::EKind::Dict:
                return TryReadDictLiteral();
            case TType::EKind::Callable:
                return TryReadCallable(code, pass);
            case TType::EKind::Any:
                return TryReadAny(code);
            case TType::EKind::Tuple:
                return TryReadTupleLiteral(pass);
            case TType::EKind::Variant:
                return 2;
            case TType::EKind::Null:
                return 0;
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadLiteral(char code) {
            switch ((TType::EKind)(code & KindMask)) {
            case TType::EKind::Type:
                ThrowCorrupted();
                break;
            case TType::EKind::Void:
                return ReadVoid(code);
            case TType::EKind::Data:
                return ReadDataLiteral();
            case TType::EKind::Struct:
                return ReadStructLiteral();
            case TType::EKind::List:
                return ReadListLiteral();
            case TType::EKind::Optional:
                return ReadOptionalLiteral(code);
            case TType::EKind::Dict:
                return ReadDictLiteral();
            case TType::EKind::Callable:
                return ReadCallable(code);
            case TType::EKind::Any:
                return ReadAny(code);
            case TType::EKind::Tuple:
                return ReadTupleLiteral();
            case TType::EKind::Variant:
                return ReadVariantLiteral(code);
            case TType::EKind::Null:
                return ReadNull();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadVoid(char code) {
            switch ((TType::EKind)(code & TypeMask)) {
            case TType::EKind::Void: return Env.GetVoidLazy();
            case TType::EKind::EmptyList: return Env.GetEmptyListLazy();
            case TType::EKind::EmptyDict: return Env.GetEmptyDictLazy();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadNull() {
            auto node = Env.GetNullLazy();
            return node;
        }

        TNode* ReadDataLiteral() {
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Data)
                ThrowCorrupted();

            auto dataType = static_cast<TDataType*>(type);
            NUdf::TUnboxedValuePod value;
            switch (dataType->GetSchemeType()) {
            case 0:
                break;
            case NUdf::TDataType<bool>::Id:
                value = NUdf::TUnboxedValuePod((bool)Read());
                break;
            case NUdf::TDataType<i8>::Id:
            {
                value = NUdf::TUnboxedValuePod((i8)Read());
                break;
            }
            case NUdf::TDataType<ui8>::Id:
            {
                value = NUdf::TUnboxedValuePod((ui8)Read());
                break;
            }
            case NUdf::TDataType<i16>::Id:
            {
                value = NUdf::TUnboxedValuePod((i16)ZigZagDecode((ui16)ReadVar32()));
                break;
            }
            case NUdf::TDataType<ui16>::Id:
            {
                value = NUdf::TUnboxedValuePod((ui16)ReadVar32());
                break;
            }
            case NUdf::TDataType<i32>::Id:
            {
                value = NUdf::TUnboxedValuePod((i32)ZigZagDecode(ReadVar32()));
                break;
            }
            case NUdf::TDataType<ui32>::Id:
            {
                value = NUdf::TUnboxedValuePod((ui32)ReadVar32());
                break;
            }
            case NUdf::TDataType<float>::Id:
            {
                value = NUdf::TUnboxedValuePod(ReadUnaligned<float>(reinterpret_cast<const float*>(ReadMany(sizeof(float)))));
                break;
            }
            case NUdf::TDataType<i64>::Id:
            {
                value = NUdf::TUnboxedValuePod((i64)ZigZagDecode(ReadVar64()));
                break;
            }
            case NUdf::TDataType<ui64>::Id:
            {
                value = NUdf::TUnboxedValuePod((ui64)ReadVar64());
                break;
            }
            case NUdf::TDataType<double>::Id:
            {
                value = NUdf::TUnboxedValuePod(ReadUnaligned<double>(reinterpret_cast<const double*>(ReadMany(sizeof(double)))));
                break;
            }
            case NUdf::TDataType<NUdf::TDate>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TDate>::TLayout>(ReadVar32()));
                break;
            }
            case NUdf::TDataType<NUdf::TDatetime>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TDatetime>::TLayout>(ReadVar32()));
                break;
            }
            case NUdf::TDataType<NUdf::TTimestamp>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(ReadVar64()));
                break;
            }
            case NUdf::TDataType<NUdf::TInterval>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TInterval>::TLayout>(ZigZagDecode(ReadVar64())));
                break;
            }
            case NUdf::TDataType<NUdf::TTzDate>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTzDate>::TLayout>(ReadVar32()));
                const ui32 tzId = ReadVar32();
                MKQL_ENSURE(tzId <= Max<ui16>() && IsValidTimezoneId((ui16)tzId), "Unknown timezone: " << tzId);
                value.SetTimezoneId((ui16)tzId);
                break;
            }
            case NUdf::TDataType<NUdf::TTzDatetime>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTzDatetime>::TLayout>(ReadVar32()));
                const ui32 tzId = ReadVar32();
                MKQL_ENSURE(tzId <= Max<ui16>() && IsValidTimezoneId((ui16)tzId), "Unknown timezone: " << tzId);
                value.SetTimezoneId((ui16)tzId);
                break;
            }
            case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTzTimestamp>::TLayout>(ReadVar64()));
                const ui32 tzId = ReadVar32();
                MKQL_ENSURE(tzId <= Max<ui16>() && IsValidTimezoneId((ui16)tzId), "Unknown timezone: " << tzId);
                value.SetTimezoneId((ui16)tzId);
                break;
            }
            case NUdf::TDataType<NUdf::TDate32>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TDate32>::TLayout>(ZigZagDecode(ReadVar32())));
                break;
            }
            case NUdf::TDataType<NUdf::TDatetime64>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TDatetime64>::TLayout>(ZigZagDecode(ReadVar64())));
                break;
            }
            case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp64>::TLayout>(ZigZagDecode(ReadVar64())));
                break;
            }
            case NUdf::TDataType<NUdf::TInterval64>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TInterval64>::TLayout>(ZigZagDecode(ReadVar64())));
                break;
            }
            case NUdf::TDataType<NUdf::TTzDate32>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTzDate32>::TLayout>(ZigZagDecode(ReadVar32())));
                const ui32 tzId = ReadVar32();
                MKQL_ENSURE(tzId <= Max<ui16>() && IsValidTimezoneId((ui16)tzId), "Unknown timezone: " << tzId);
                value.SetTimezoneId((ui16)tzId);
                break;
            }
            case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTzDatetime64>::TLayout>(ZigZagDecode(ReadVar64())));
                const ui32 tzId = ReadVar32();
                MKQL_ENSURE(tzId <= Max<ui16>() && IsValidTimezoneId((ui16)tzId), "Unknown timezone: " << tzId);
                value.SetTimezoneId((ui16)tzId);
                break;
            }
            case NUdf::TDataType<NUdf::TTzTimestamp64>::Id:
            {
                value = NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout>(ZigZagDecode(ReadVar64())));
                const ui32 tzId = ReadVar32();
                MKQL_ENSURE(tzId <= Max<ui16>() && IsValidTimezoneId((ui16)tzId), "Unknown timezone: " << tzId);
                value.SetTimezoneId((ui16)tzId);
                break;
            }
            case NUdf::TDataType<NUdf::TUuid>::Id:
            {
                const char* buffer = ReadMany(16);
                value = Env.NewStringValue(NUdf::TStringRef(buffer, 16));
                break;
            }
            case NUdf::TDataType<NUdf::TDecimal>::Id:
            {
                value = NUdf::TUnboxedValuePod::Zero();
                const char* buffer = ReadMany(sizeof(NYql::NDecimal::TInt128) - 1U);
                std::memcpy(static_cast<char*>(value.GetRawPtr()), buffer, sizeof(NYql::NDecimal::TInt128) - 1U);
                break;
            }
            default:
                const ui32 size = ReadVar32();
                const char* buffer = ReadMany(size);
                value = Env.NewStringValue(NUdf::TStringRef(buffer, size));
                break;
            }

            const auto node = TDataLiteral::Create(value, dataType, Env);
            Nodes.emplace_back(node);
            return node;
        }

        ui32 TryReadKeyLiteral(char code) {
            const bool isRoot = (code & UserMarker1) != 0;
            const bool hasValue = (code & UserMarker2) != 0;

            if (isRoot) {
                return 1;
            } else {
                return hasValue ? 3 : 2;
            }
        }

        ui32 TryReadStructLiteral(ui32 pass) {
            if (pass == 0) {
                return 1 + RequiresNextPass;
            }

            auto type = PeekNode(0);
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Struct)
                ThrowCorrupted();

            auto structType = static_cast<TStructType*>(type);
            const ui32 valuesCount = structType->GetMembersCount();
            return valuesCount;
        }

        TNode* ReadStructLiteral() {
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Struct)
                ThrowCorrupted();

            auto structType = static_cast<TStructType*>(type);
            const ui32 valuesCount = structType->GetMembersCount();
            TStackVec<TRuntimeNode> values(valuesCount);
            for (ui32 i = 0; i < valuesCount; ++i) {
                auto item = PopNode();
                values[i] = TRuntimeNode(item, false);
            }

            const char* immediateFlags = ReadMany(GetBitmapBytes(valuesCount));
            for (ui32 i = 0; i < valuesCount; ++i) {
                values[i] = TRuntimeNode(values[i].GetNode(), GetBitmapBit(immediateFlags, i));
            }

            auto node = TStructLiteral::Create(valuesCount, values.data(), structType, Env);
            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadTupleLiteral(ui32 pass) {
            if (pass == 0) {
                return 1 + RequiresNextPass;
            }

            auto type = PeekNode(0);
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Tuple)
                ThrowCorrupted();

            auto tupleType = static_cast<TTupleType*>(type);
            const ui32 valuesCount = tupleType->GetElementsCount();
            return valuesCount;
        }

        TNode* ReadTupleLiteral() {
            auto type = PopNode();
            if (!type->GetType()->IsType())
                ThrowCorrupted();

            if (!static_cast<const TType&>(*type).IsTuple())
                ThrowCorrupted();

            auto tupleType = static_cast<TTupleType*>(type);
            const ui32 valuesCount = tupleType->GetElementsCount();
            TStackVec<TRuntimeNode> values(valuesCount);
            for (ui32 i = 0; i < valuesCount; ++i) {
                auto item = PopNode();
                values[i] = TRuntimeNode(item, false);
            }

            const char* immediateFlags = ReadMany(GetBitmapBytes(valuesCount));
            for (ui32 i = 0; i < valuesCount; ++i) {
                values[i] = TRuntimeNode(values[i].GetNode(), GetBitmapBit(immediateFlags, i));
            }

            auto node = TTupleLiteral::Create(valuesCount, values.data(), tupleType, Env);
            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadListLiteral() {
            const ui32 itemsCount = ReadVar32();
            return 1 + itemsCount;
        }

        TNode* ReadListLiteral() {
            const ui32 itemsCount = ReadVar32();
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::List)
                ThrowCorrupted();

            auto listType = static_cast<TListType*>(type);
            TVector<TRuntimeNode> items;
            items.reserve(itemsCount);
            for (ui32 i = 0; i < itemsCount; ++i) {
                auto item = PopNode();
                items.push_back(TRuntimeNode(item, false));
            }

            const char* immediateFlags = ReadMany(GetBitmapBytes(itemsCount));
            for (ui32 i = 0; i < itemsCount; ++i) {
                auto& x = items[i];
                x = TRuntimeNode(x.GetNode(), GetBitmapBit(immediateFlags, i));
            }

            auto node = TListLiteral::Create(items.data(), items.size(), listType, Env);
            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadOptionalLiteral(char code) {
            const bool hasItem = (code & UserMarker1) != 0;
            return hasItem ? 2 : 1;
        }

        TNode* ReadOptionalLiteral(char code) {
            const bool hasItem = (code & UserMarker1) != 0;
            const bool isItemImmediate = (code & UserMarker2) != 0;
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Optional)
                ThrowCorrupted();

            auto optionalType = static_cast<TOptionalType*>(type);
            TNode* node;
            if (hasItem) {
                auto item = PopNode();
                node = TOptionalLiteral::Create(TRuntimeNode(item, isItemImmediate), optionalType, Env);
            } else {
                node = TOptionalLiteral::Create(optionalType, Env);
            }

            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadDictLiteral() {
            const ui32 itemsCount = ReadVar32();
            return 1 + 2 * itemsCount;
        }

        TNode* ReadDictLiteral() {
            const ui32 itemsCount = ReadVar32();
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Dict)
                ThrowCorrupted();

            auto dictType = static_cast<TDictType*>(type);
            TStackVec<std::pair<TRuntimeNode, TRuntimeNode>> items(itemsCount);
            for (ui32 i = 0; i < itemsCount; ++i) {
                auto key = PopNode();
                auto payload = PopNode();
                items[i].first = TRuntimeNode(key, false);
                items[i].second = TRuntimeNode(payload, false);
            }

            const char* immediateFlags = ReadMany(GetBitmapBytes(itemsCount * 2));
            for (ui32 i = 0; i < itemsCount; ++i) {
                items[i].first = TRuntimeNode(items[i].first.GetNode(), GetBitmapBit(immediateFlags, 2 * i));
                items[i].second = TRuntimeNode(items[i].second.GetNode(), GetBitmapBit(immediateFlags, 2 * i + 1));
            }

            auto node = TDictLiteral::Create(itemsCount, items.data(), dictType, Env);
            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadCallable(char code, ui32 pass) {
            const bool hasResult = (code & UserMarker1) != 0;
            if (hasResult) {
                return 2;
            }

            if (pass == 0) {
                return 1 | RequiresNextPass;
            }

            auto type = PeekNode(0);
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Callable)
                ThrowCorrupted();

            auto callableType = static_cast<TCallableType*>(type);
            const ui32 inputsCount = callableType->GetArgumentsCount();
            return inputsCount;
        }

        TNode* ReadCallable(char code) {
            const bool hasResult = (code & UserMarker1) != 0;
            const bool hasUniqueId = (code & UserMarker2) != 0;
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Callable)
                ThrowCorrupted();

            auto callableType = static_cast<TCallableType*>(type);
            TCallable* node;
            if (hasResult) {
                auto resultNode = PopNode();

                const bool isResultImmediate = (Read() != 0);
                node = TCallable::Create(TRuntimeNode(resultNode, isResultImmediate), callableType, Env);
            } else {
                const ui32 inputsCount = callableType->GetArgumentsCount();
                TStackVec<TRuntimeNode> inputs(inputsCount);
                for (ui32 i = 0; i < inputsCount; ++i) {
                    auto input = PopNode();
                    inputs[i] = TRuntimeNode(input, false);
                }

                const char* immediateFlags = ReadMany(GetBitmapBytes(inputsCount));
                for (ui32 i = 0; i < inputsCount; ++i) {
                    inputs[i] = TRuntimeNode(inputs[i].GetNode(), GetBitmapBit(immediateFlags, i));
                }

                node = TCallable::Create(inputsCount, inputs.data(), callableType, Env);
            }

            if (hasUniqueId) {
                node->SetUniqueId(ReadVar32());
            }

            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadAny(char code) {
            const bool hasItem = (code & UserMarker1) != 0;
            return hasItem ? 1 : 0;
        }

        TNode* ReadAny(char code) {
            const bool hasItem = (code & UserMarker1) != 0;
            const bool isItemImmediate = (code & UserMarker2) != 0;
            TAny* node = TAny::Create(Env);
            if (hasItem) {
                auto item = PopNode();
                node->SetItem(TRuntimeNode(item, isItemImmediate));
            }

            Nodes.push_back(node);
            return node;
        }

        TNode* ReadVariantLiteral(char code) {
            const bool isItemImmediate = (code & UserMarker1) != 0;
            auto type = PopNode();
            if (type->GetType()->GetKind() != TType::EKind::Type)
                ThrowCorrupted();

            if (static_cast<const TType&>(*type).GetKind() != TType::EKind::Variant)
                ThrowCorrupted();

            auto variantType = static_cast<TVariantType*>(type);
            auto item = PopNode();
            ui32 index = ReadVar32();
            TNode* node = TVariantLiteral::Create(TRuntimeNode(item, isItemImmediate), index, variantType, Env);

            Nodes.push_back(node);
            return node;
        }

        ui32 TryReadSystemObject(char code) {
            switch (code & CommandMask) {
            case (ui32)ESystemCommand::Ref:
                return 0;
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadSystemObject(char code) {
            switch (code & CommandMask) {
            case (ui32)ESystemCommand::Ref:
                return ReadReference();
            default:
                ThrowCorrupted();
            }
        }

        TNode* ReadReference() {
            const ui32 index = ReadVar32();
            if (index >= Nodes.size())
                ThrowCorrupted();

            return Nodes[index];
        }

        void LoadName() {
            const ui32 length = ReadVar32();
            const char* buffer = ReadMany(length);
            TStringBuf name(buffer, buffer + length);
            Names.push_back(name);
        }

        TStringBuf ReadName() {
            const ui32 nameDescriptor = ReadVar32();
            if (nameDescriptor & NameRefMark) {
                const ui32 nameIndex = nameDescriptor >> 1;
                if (nameIndex >= Names.size())
                    ThrowCorrupted();
                return Names[nameIndex];
            } else {
                const ui32 length = nameDescriptor >> 1;
                const char* buffer = ReadMany(length);
                return TStringBuf(buffer, buffer + length);
            }
        }

    private:
        struct TNodeContext {
            const char* Start;
            ui32 NextPass;
            ui32 ChildCount;

            TNodeContext()
                : Start(nullptr)
                , NextPass(0)
                , ChildCount(0)
            {
            }
        };

        const char* Current;
        const char* const End;
        const TTypeEnvironment& Env;
        TVector<TNode*> Nodes;
        TVector<TStringBuf> Names;
        TVector<TNodeContext> CtxStack;
        TVector<std::pair<TNode*, const char*>> NodeStack;
    };
}

TString SerializeNode(TNode* node, const TTypeEnvironment& env) noexcept{
    return SerializeRuntimeNode(TRuntimeNode(node, true), env);
}

TString SerializeRuntimeNode(TExploringNodeVisitor& explorer, TRuntimeNode node, const TTypeEnvironment& env) noexcept{
    Y_UNUSED(env);
    TPrepareWriteNodeVisitor preparer;
    for (auto node : explorer.GetNodes()) {
        node->Accept(preparer);
    }

    auto& names = preparer.GetNames();
    auto& nameOrder = preparer.GetNameOrder();
    TWriter writer(names, nameOrder);
    writer.Write(node);
    for (auto node : explorer.GetNodes()) {
        node->SetCookie(0);
    }

    return writer.GetOutput();
}

TString SerializeRuntimeNode(TRuntimeNode node, const TTypeEnvironment& env) noexcept{
    TExploringNodeVisitor explorer;
    explorer.Walk(node.GetNode(), env);
    return SerializeRuntimeNode(explorer, node, env);
}

TNode* DeserializeNode(const TStringBuf& buffer, const TTypeEnvironment& env) {
    return DeserializeRuntimeNode(buffer, env).GetNode();
}

TRuntimeNode DeserializeRuntimeNode(const TStringBuf& buffer, const TTypeEnvironment& env) {
    TReader reader(buffer, env);
    return reader.Deserialize();
}

}
}
