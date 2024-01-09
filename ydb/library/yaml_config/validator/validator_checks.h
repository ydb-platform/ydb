#pragma once

#include "fwd.h"

#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <util/generic/maybe.h>

// context is attached to some path

// node wrapper should have context, because of ctx["field1"]["field2"].
// If field1 exists and field2 doesn't, then NodeWrapper would be responsible
// for error notification. And in general it may be good for node wrapper to
// know in what context it exists

// TODO: make something like this
// (MapContext ctx) {
//     ctx.ExpectEqual(ctx["field"], ctx["Another_field"]["2"], "must be equal");
//     // ExpectEqual(const TNodeWrapper&, const TNodeWrapper&) -> checks types and values (only for scalars)
//     // if (ctx["field"])
//     ctx["field"] // error if field is not presented?
//     if (ctx.Has("field1")) {
//         c.ExpectToExist(ctx["field2"]["field3"]);
//         //Access to methods by this path(when node is not presented) - error
//     } else {
//         ctx.ExpectGreater(ctx["field3"].Int64()/*.Value*/, 10);
//     }
// }

namespace NKikimr::NYamlConfig::NValidator {
namespace NDetail {

template <typename TThis>
class TNodeWrapperCommonOps {
public:
    TNodeWrapperCommonOps(TCheckContext& context, NFyaml::TNodeRef node, TString pathFromCheckNode);
    TNodeWrapperCommonOps(const TNodeWrapperCommonOps<TThis>& other);

    bool Exists() const;

    TNodeWrapperCommonOps<TThis>& operator=(const TNodeWrapperCommonOps<TThis>& other);

protected:
    TCheckContext& Context_;
    NFyaml::TNodeRef Node_;
    TString PathFromCheckNode_;

    // if true, then has no validator, node type and can be anything
    bool IsOpaqueChild() const;
    void ThrowIfNullNode() const;

private:
    const TThis& AsDerived() const;
    TThis& AsDerived();
};

template <typename TThis>
TNodeWrapperCommonOps<TThis>::TNodeWrapperCommonOps(
    TCheckContext& context,
    NFyaml::TNodeRef node,
    TString pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , PathFromCheckNode_(pathFromCheckNode) {}

template <typename TThis>
TNodeWrapperCommonOps<TThis>::TNodeWrapperCommonOps(const TNodeWrapperCommonOps<TThis>& other)
    : Context_(other.Context_)
    , Node_(other.Node_)
    , PathFromCheckNode_(other.PathFromCheckNode_) {}

template <typename TThis>
TNodeWrapperCommonOps<TThis>& TNodeWrapperCommonOps<TThis>::operator=(const TNodeWrapperCommonOps<TThis>& other) {
    Context_ = other.Context_;
    Node_ = other.Node_;
    PathFromCheckNode_ = other.PathFromCheckNode_;
    return *this;
}

template <typename TThis>
void TNodeWrapperCommonOps<TThis>::ThrowIfNullNode() const {
    if (!Node_) {
        throw TCheckException() <<
            "Node \"" +
            PathFromCheckNode_ +
            "\" is not presented";
    }
}

template <typename TThis>
bool TNodeWrapperCommonOps<TThis>::IsOpaqueChild() const {
    return !AsDerived().Validator_;
}

template <typename TThis>
bool TNodeWrapperCommonOps<TThis>::Exists() const {
    return Node_ != nullptr;
}

template <typename TThis>
const TThis& TNodeWrapperCommonOps<TThis>::AsDerived() const {
    return static_cast<const TThis&>(*this);
}

template <typename TThis>
TThis& TNodeWrapperCommonOps<TThis>::AsDerived() {
    return static_cast<TThis&>(*this);
}

} // namespace NDetail

class TNodeWrapper : public NDetail::TNodeWrapperCommonOps<TNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TNodeWrapper>;

public:
    TNodeWrapper(
        TCheckContext& context,
        NFyaml::TNodeRef node,
        TValidator* validator,
        TMaybe<ENodeType> nodeType,
        const TString& pathFromCheckNode);

    TGenericNodeWrapper Generic();
    TMapNodeWrapper Map();
    TArrayNodeWrapper Array();
    TInt64NodeWrapper Int64();
    TStringNodeWrapper String();
    TBoolNodeWrapper Bool();
    TEnumNodeWrapper Enum();
    TString Scalar();

    TMaybe<ENodeType> ValidatorType();

    bool IsMap();
    bool IsArray();
    bool IsInt64();
    bool IsString();
    bool IsBool();
    bool IsScalar();

protected:
    TValidator* Validator_;
    TMaybe<ENodeType> NodeType_;
};

class TGenericNodeWrapper : public NDetail::TNodeWrapperCommonOps<TGenericNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TGenericNodeWrapper>;

// TODO: make some functionality for generic wrapper
public:
    TGenericNodeWrapper(
        TGenericCheckContext& context,
        NFyaml::TNodeRef node,
        TGenericValidator* validator,
        const TString& pathFromCheckNode);
    
    operator TNodeWrapper();

private:
    TGenericValidator* Validator_;
};

class TMapNodeWrapper : public NDetail::TNodeWrapperCommonOps<TMapNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TMapNodeWrapper>;

public:
    TMapNodeWrapper(
        TMapCheckContext& context,
        NFyaml::TNodeRef node,
        TMapValidator* validator,
        const TString& pathFromCheckNode);
    
    operator TNodeWrapper();
    
    TNodeWrapper operator[](const TString& field);
    TNodeWrapper At(const TString& field);
    bool Has(const TString& field);

private:
    TMapValidator* Validator_;

    NFyaml::TMapping Map();
};

class TArrayNodeWrapper : public NDetail::TNodeWrapperCommonOps<TArrayNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TArrayNodeWrapper>;

public:
    TArrayNodeWrapper(
        TArrayCheckContext& context,
        NFyaml::TNodeRef node,
        TArrayValidator* validator,
        const TString& pathFromCheckNode);

    int Length();

    TNodeWrapper operator[](size_t index);

    operator TNodeWrapper();

private:
    TArrayValidator* Validator_;

    NFyaml::TSequence Sequence();
};

class TInt64NodeWrapper : public NDetail::TNodeWrapperCommonOps<TInt64NodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TInt64NodeWrapper>;

public:
    TInt64NodeWrapper(
        TInt64CheckContext& context,
        NFyaml::TNodeRef node,
        TInt64Validator* validator,
        const TString& pathFromCheckNode);

    i64 Value() const;
    operator i64() const;

    operator TNodeWrapper();

private:
    TInt64Validator* Validator_;
};

class TStringNodeWrapper : public NDetail::TNodeWrapperCommonOps<TStringNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TStringNodeWrapper>;

public:
    TStringNodeWrapper(
        TStringCheckContext& context,
        NFyaml::TNodeRef node,
        TStringValidator* validator,
        const TString& pathFromCheckNode);

    TString Value() const;
    operator TString() const;

    operator TNodeWrapper();

private:
    TStringValidator* Validator_;
};

class TBoolNodeWrapper : public NDetail::TNodeWrapperCommonOps<TBoolNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TBoolNodeWrapper>;

public:
    TBoolNodeWrapper(
        TBoolCheckContext& context,
        NFyaml::TNodeRef node,
        TBoolValidator* validator,
        const TString& pathFromCheckNode);

    bool Value() const;
    operator bool() const;

    operator TNodeWrapper();

private:
    TBoolValidator* Validator_;
};

class TEnumNodeWrapper : public NDetail::TNodeWrapperCommonOps<TEnumNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

    using TBase = NDetail::TNodeWrapperCommonOps<TEnumNodeWrapper>;

public:
    TEnumNodeWrapper(
        TEnumCheckContext& context,
        NFyaml::TNodeRef node,
        TEnumValidator* validator,
        const TString& pathFromCheckNode);

    TString Value() const;
    operator TString() const;

    operator TNodeWrapper();

private:
    TEnumValidator* Validator_;
};


class TCheckContext {
    friend class TMapNodeWrapper; // for access to node path
    template <typename TThis, typename TContext>
    friend class NDetail::TValidatorCommonOps;
    template <typename>
    friend class NDetail::TNodeWrapperCommonOps;

public:
    TCheckContext(
        NFyaml::TNodeRef node,
        const TString& checkNodePath);

    void Expect(bool condition, TString error);
    void Expect(bool condition);
    void AddError(TString error);
    void Fail(TString error);
    void Fail();
    void Assert(bool condition, TString error);
    void Assert(bool condition);

    virtual ~TCheckContext() = default;

protected:
    TVector<TString> Errors_;
    NFyaml::TNodeRef Node_;
    TString CheckNodePath_;
    bool someErrorOccured_ = false;
};

class TGenericCheckContext : public TCheckContext {
    friend class TGenericValidator;
    friend class TGenericNodeWrapper;

public:
    TGenericCheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TGenericValidator* validator);

    TGenericNodeWrapper Node();

private:
    TGenericValidator* Validator_;
};

class TMapCheckContext : public TCheckContext {
    friend class TMapValidator;
    friend class TMapNodeWrapper;

public:
    TMapCheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TMapValidator* validator);

    TMapNodeWrapper Node();

private:
    TMapValidator* Validator_;
};

class TArrayCheckContext : public TCheckContext {
    friend class TArrayValidator;
    friend class TArrayNodeWrapper;

public:
    TArrayCheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TArrayValidator* validator);

    TArrayNodeWrapper Node();

private:
    TArrayValidator* Validator_;
};

class TInt64CheckContext : public TCheckContext {
    friend class TInt64Validator;
    friend class TInt64NodeWrapper;

public:
    TInt64CheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TInt64Validator* validator);

    TInt64NodeWrapper Node();

private:
    TInt64Validator* Validator_;
};

class TStringCheckContext : public TCheckContext {
    friend class TStringNodeWrapper;

public:
    TStringCheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TStringValidator* validator);

    TStringNodeWrapper Node();

private:
    TStringValidator* Validator_;
};

class TBoolCheckContext : public TCheckContext {
    friend class TBoolNodeWrapper;

public:
    TBoolCheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TBoolValidator* validator);

    TBoolNodeWrapper Node();

private:
    TBoolValidator* Validator_;
};

class TEnumCheckContext : public TCheckContext {
    friend class TEnumNodeWrapper;

public:
    TEnumCheckContext(NFyaml::TNodeRef node,
        const TString& checkNodePath,
        TEnumValidator* validator);

    TEnumNodeWrapper Node();

private:
    TEnumValidator* Validator_;
};

} // namespace NKikimr::NYamlConfig::NValidator
