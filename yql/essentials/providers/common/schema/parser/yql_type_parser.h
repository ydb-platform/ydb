#pragma once

#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>

namespace NYql {
namespace NCommon {

class TYqlTypeYsonSaverBase {
public:
    typedef NYson::TYsonConsumerBase TConsumer;

    TYqlTypeYsonSaverBase(TConsumer& writer, bool extendedForm)
        : Writer(writer)
        , ExtendedForm(extendedForm)
    {
    }

protected:
    void SaveTypeHeader(TStringBuf name);
    void SaveType();
    void SaveVoidType();
    void SaveNullType();
    void SaveUnitType();
    void SaveGenericType();
    void SaveEmptyListType();
    void SaveEmptyDictType();
    void SaveDataType(const TStringBuf& dataType);
    void SavePgType(const TStringBuf& pgType);
    void SaveDataTypeParams(const TStringBuf& dataType, const TStringBuf& paramOne, const TStringBuf& paramTwo);
    void SaveResourceType(const TStringBuf& tag);

protected:
    NYson::TYsonConsumerBase& Writer;
    const bool ExtendedForm;
};


template <typename TDerived>
class TYqlTypeYsonSaverImpl: public TYqlTypeYsonSaverBase {
    typedef TYqlTypeYsonSaverImpl<TDerived> TSelf;

public:
    TYqlTypeYsonSaverImpl(TConsumer& writer, bool extendedForm)
        : TYqlTypeYsonSaverBase(writer, extendedForm)
    {
    }

    template <typename TType>
    void Save(TType* type) {
        static_cast<TDerived*>(this)->Save(type);
    }

protected:
    template <typename TTaggedType>
    void SaveTaggedType(const TTaggedType& taggedType) {
        SaveTypeHeader("TaggedType");
        Writer.OnListItem();
        Writer.OnStringScalar(taggedType.GetTag());
        Writer.OnListItem();
        TSelf baseType(Writer, ExtendedForm);
        baseType.Save(taggedType.GetBaseType());
        Writer.OnEndList();
    }

    template <typename TStructType>
    void SaveStructType(const TStructType& structType) {
        SaveTypeHeader("StructType");
        Writer.OnListItem();
        Writer.OnBeginList();
        for (ui32 i = 0, e = structType.GetMembersCount(); i < e; ++i) {
            Writer.OnListItem();
            Writer.OnBeginList();
            Writer.OnListItem();
            Writer.OnStringScalar(structType.GetMemberName(i));
            Writer.OnListItem();
            TSelf value(Writer, ExtendedForm);
            value.Save(structType.GetMemberType(i));
            Writer.OnEndList();
        }
        Writer.OnEndList();
        Writer.OnEndList();
    }

    template <typename TListType>
    void SaveListType(const TListType& listType) {
        SaveTypeHeader("ListType");
        Writer.OnListItem();
        TSelf item(Writer, ExtendedForm);
        item.Save(listType.GetItemType());
        Writer.OnEndList();
    }

    template <typename TStreamType>
    void SaveStreamType(const TStreamType& streamType) {
        SaveTypeHeader("StreamType");
        Writer.OnListItem();
        TSelf item(Writer, ExtendedForm);
        item.Save(streamType.GetItemType());
        Writer.OnEndList();
    }

    template <typename TOptionalType>
    void SaveOptionalType(const TOptionalType& optionalType) {
        SaveTypeHeader("OptionalType");
        Writer.OnListItem();
        TSelf item(Writer, ExtendedForm);
        item.Save(optionalType.GetItemType());
        Writer.OnEndList();
    }

    template <typename TDictType>
    void SaveDictType(const TDictType& dictType) {
        SaveTypeHeader("DictType");
        Writer.OnListItem();
        TSelf key(Writer, ExtendedForm);
        key.Save(dictType.GetKeyType());
        Writer.OnListItem();
        TSelf val(Writer, ExtendedForm);
        val.Save(dictType.GetPayloadType());
        Writer.OnEndList();
    }

    template <typename TTupleType>
    void SaveTupleType(const TTupleType& tupleType) {
        SaveTypeHeader("TupleType");
        Writer.OnListItem();
        Writer.OnBeginList();
        for (ui32 i = 0, e = tupleType.GetElementsCount(); i < e; ++i) {
            Writer.OnListItem();
            TSelf element(Writer, ExtendedForm);
            element.Save(tupleType.GetElementType(i));
        }
        Writer.OnEndList();
        Writer.OnEndList();
    }

    template <typename TCallableType>
    void SaveCallableType(const TCallableType& callableType) {
        SaveTypeHeader("CallableType");
        Writer.OnListItem();
        // main settings
        Writer.OnBeginList();
        if (callableType.GetOptionalArgsCount() > 0 || !callableType.GetPayload().empty()) {
            Writer.OnListItem();
            Writer.OnUint64Scalar(callableType.GetOptionalArgsCount());
        }

        if (!callableType.GetPayload().empty()) {
            Writer.OnListItem();
            Writer.OnStringScalar(callableType.GetPayload());
        }

        Writer.OnEndList();
        // ret
        Writer.OnListItem();
        Writer.OnBeginList();
        Writer.OnListItem();
        TSelf ret(Writer, ExtendedForm);
        ret.Save(callableType.GetReturnType());
        Writer.OnEndList();
        // args
        Writer.OnListItem();
        Writer.OnBeginList();
        for (ui32 i = 0, e = callableType.GetArgumentsCount(); i < e; ++i) {
            Writer.OnListItem();
            Writer.OnBeginList();
            Writer.OnListItem();
            TSelf arg(Writer, ExtendedForm);
            arg.Save(callableType.GetArgumentType(i));
            if (!callableType.GetArgumentName(i).empty()) {
                Writer.OnListItem();
                Writer.OnStringScalar(callableType.GetArgumentName(i));
            }

            if (callableType.GetArgumentFlags(i) != 0) {
                Writer.OnListItem();
                Writer.OnUint64Scalar(callableType.GetArgumentFlags(i));
            }

            Writer.OnEndList();
        }

        Writer.OnEndList();
        Writer.OnEndList();
    }

    template <typename TVariantType>
    void SaveVariantType(const TVariantType& variantType) {
        SaveTypeHeader("VariantType");
        Writer.OnListItem();
        TSelf item(Writer, ExtendedForm);
        item.Save(variantType.GetUnderlyingType());
        Writer.OnEndList();
    }
};

template <typename TLoader>
TMaybe<typename TLoader::TType> DoLoadTypeFromYson(TLoader& loader, const NYT::TNode& node, ui32 level) {
    if (!node.IsList() || node.Size() < 1 || !node[0].IsString()) {
        loader.Error("Invalid type scheme");
        return Nothing();
    }
    auto typeName = node[0].AsString();
    if (typeName == "VoidType") {
        return loader.LoadVoidType(level);
    } else if (typeName == "NullType") {
        return loader.LoadNullType(level);
    } else if (typeName == "UnitType") {
        return loader.LoadUnitType(level);
    } else if (typeName == "GenericType") {
        return loader.LoadGenericType(level);
    } else if (typeName == "EmptyListType") {
        return loader.LoadEmptyListType(level);
    } else if (typeName == "EmptyDictType") {
        return loader.LoadEmptyDictType(level);
    } else if (typeName == "DataType") {
        if ((node.Size() != 2 && node.Size() != 4) || !node[1].IsString()) {
            loader.Error("Invalid data type scheme");
            return Nothing();
        }
        if (node.Size() == 2) {
            return loader.LoadDataType(node[1].AsString(), level);
        }

        if (!node[2].IsString() || !node[3].IsString()) {
            loader.Error("Invalid data type scheme");
            return Nothing();
        }

        return loader.LoadDataTypeParams(node[1].AsString(), node[2].AsString(), node[3].AsString(), level);
    } else if (typeName == "PgType") {
        if (node.Size() != 2 || !node[1].IsString()) {
            loader.Error("Invalid pg type scheme");
            return Nothing();
        }
        return loader.LoadPgType(node[1].AsString(), level);
    } else if (typeName == "ResourceType") {
        if (node.Size() != 2 || !node[1].IsString()) {
            loader.Error("Invalid resource type scheme");
            return Nothing();
        }
        return loader.LoadResourceType(node[1].AsString(), level);
    } else if (typeName == "TaggedType") {
        if (node.Size() != 3 || !node[1].IsString()) {
            loader.Error("Invalid tagged type scheme");
            return Nothing();
        }
        auto baseType = DoLoadTypeFromYson(loader, node[2], level); // Don't increase level type for tagged type
        if (!baseType) {
            return Nothing();
        }
        return loader.LoadTaggedType(*baseType, node[1].AsString(), level);
    } else if (typeName == "ErrorType") {
        if (node.Size() != 5 || !node[1].IsInt64() || !node[2].IsInt64() || !node[3].IsString() || !node[4].IsString()) {
            loader.Error("Invalid error type scheme");
            return Nothing();
        }
        return loader.LoadErrorType(node[1].AsInt64(), node[2].AsInt64(), node[3].AsString(), node[4].AsString(), level);
    } else if (typeName == "StructType") {
        if (node.Size() != 2 || !node[1].IsList()) {
            loader.Error("Invalid struct type scheme");
            return Nothing();
        }
        TVector<std::pair<TString, typename TLoader::TType>> members;
        for (auto& member : node[1].AsList()) {
            if (!member.IsList() || member.Size() != 2 || !member[0].IsString()) {
                loader.Error("Invalid struct type scheme");
                return Nothing();
            }

            auto name = member[0].AsString();
            auto memberType = DoLoadTypeFromYson(loader, member[1], level + 1);
            if (!memberType) {
                return Nothing();
            }
            members.push_back(std::make_pair(name, *memberType));
        }
        return loader.LoadStructType(members, level);
    } else if (typeName == "ListType") {
        if (node.Size() != 2) {
            loader.Error("Invalid list type scheme");
            return Nothing();
        }
        auto itemType = DoLoadTypeFromYson(loader, node[1], level + 1);
        if (!itemType) {
            return Nothing();
        }
        return loader.LoadListType(*itemType, level);
    } else if (typeName == "StreamType") {
        if (node.Size() != 2) {
            loader.Error("Invalid list type scheme");
            return Nothing();
        }
        auto itemType = DoLoadTypeFromYson(loader, node[1], level + 1);
        if (!itemType) {
            return Nothing();
        }
        return loader.LoadStreamType(*itemType, level);

    } else if (typeName == "OptionalType") {
        if (node.Size() != 2) {
            loader.Error("Invalid optional type scheme");
            return Nothing();
        }
        auto itemType = DoLoadTypeFromYson(loader, node[1], level + 1);
        if (!itemType) {
            return Nothing();
        }
        return loader.LoadOptionalType(*itemType, level);
    } else if (typeName == "TupleType") {
        if (node.Size() != 2 || !node[1].IsList()) {
            loader.Error("Invalid tuple type scheme");
            return Nothing();
        }
        TVector<typename TLoader::TType> elements;
        for (auto& item: node[1].AsList()) {
            auto itemType = DoLoadTypeFromYson(loader, item, level + 1);
            if (!itemType) {
                return Nothing();
            }
            elements.push_back(*itemType);
        }
        return loader.LoadTupleType(elements, level);
    } else if (typeName == "DictType") {
        if (node.Size() != 3) {
            loader.Error("Invalid dict type scheme");
            return Nothing();
        }
        auto keyType = DoLoadTypeFromYson(loader, node[1], level + 1);
        auto valType = DoLoadTypeFromYson(loader, node[2], level + 1);
        if (!keyType || !valType) {
            return Nothing();
        }
        return loader.LoadDictType(*keyType, *valType, level);
    } else if (typeName == "CallableType") {
        if (node.Size() != 4 || !node[1].IsList() || !node[2].IsList() || !node[3].IsList()) {
            loader.Error("Invalid callable type scheme");
            return Nothing();
        }
        ui32 optionalCount = 0;
        TString payload;
        if (!node[1].AsList().empty()) {
            auto& list = node[1].AsList();
            if (!list[0].IsUint64()) {
                loader.Error("Invalid callable type scheme");
                return Nothing();
            }
            optionalCount = list[0].AsUint64();
            if (list.size() > 1) {
                if (!list[1].IsString()) {
                    loader.Error("Invalid callable type scheme");
                    return Nothing();
                }
                payload = list[1].AsString();
            }
            if (list.size() > 2) {
                loader.Error("Invalid callable type scheme");
                return Nothing();
            }
        }

        if (node[2].AsList().size() != 1) {
            loader.Error("Invalid callable type scheme");
            return Nothing();
        }
        auto returnType = DoLoadTypeFromYson(loader, node[2].AsList()[0], level + 1);
        if (!returnType) {
            return Nothing();
        }

        TVector<typename TLoader::TType> argTypes;
        TVector<TString> argNames;
        TVector<ui64> argFlags;
        for (auto& item: node[3].AsList()) {
            if (!item.IsList() || item.AsList().size() < 1 || item.AsList().size() > 3) {
                loader.Error("Invalid callable type scheme");
                return Nothing();
            }

            auto argType = DoLoadTypeFromYson(loader, item.AsList()[0], level + 1);
            if (!argType) {
                return Nothing();
            }
            argTypes.push_back(*argType);
            if (item.AsList().size() > 1 && item.AsList()[1].IsString()) {
                argNames.push_back(item.AsList()[1].AsString());
            } else {
                argNames.emplace_back();
            }
            if (item.AsList().size() > 1 && item.AsList()[1].IsUint64()) {
                argFlags.push_back(item.AsList()[1].AsUint64());
            } else if (item.AsList().size() > 2) {
                if (!item.AsList()[2].IsUint64()) {
                    loader.Error("Invalid callable type scheme");
                    return Nothing();
                }
                argFlags.push_back(item.AsList()[2].AsUint64());
            } else {
                argFlags.emplace_back();
            }
        }

        return loader.LoadCallableType(*returnType, argTypes, argNames, argFlags, optionalCount, payload, level);
    } else if (typeName == "VariantType") {
        if (node.Size() != 2) {
            loader.Error("Invalid variant type scheme");
            return Nothing();
        }
        auto underlyingType = DoLoadTypeFromYson(loader, node[1], level + 1);
        if (!underlyingType) {
            return Nothing();
        }
        return loader.LoadVariantType(*underlyingType, level);
    }
    loader.Error("unsupported type: " + typeName);
    return Nothing();
}

bool ParseYson(NYT::TNode& res, const TStringBuf yson, IOutputStream& err);

} // namespace NCommon
} // namespace NYql
