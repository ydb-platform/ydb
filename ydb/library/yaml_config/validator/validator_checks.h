#pragma once

#include "fwd.h"

#include <library/cpp/yaml/fyamlcpp/fyamlcpp.h>

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

namespace NYamlConfig::NValidator {
namespace NDetail {

template <typename TThis>
class TNodeWrapperCommonOps {
public:
    bool Exists();

protected:
    // if true, then has no validator, node type and can be anything
    bool IsOpaqueChild();
    void ThrowIfNullNode();

private:
    TThis& AsDerived();
};

template <typename TThis>
void TNodeWrapperCommonOps<TThis>::ThrowIfNullNode() {
    if (!AsDerived().Node_) {
        throw yexception() <<
            "Node \"" +
            AsDerived().PathFromCheckNode_ +
            "\" is not presented";
    }
}

template <typename TThis>
bool TNodeWrapperCommonOps<TThis>::IsOpaqueChild() {
    return !AsDerived().Validator_;
}

template <typename TThis>
bool TNodeWrapperCommonOps<TThis>::Exists() {
    return AsDerived().Node_ != nullptr;
}

template <typename TThis>
TThis& TNodeWrapperCommonOps<TThis>::AsDerived() {
    return static_cast<TThis&>(*this);
}

} // namespace NDetail

class TNodeWrapper : public NDetail::TNodeWrapperCommonOps<TNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

public:
    TNodeWrapper(
        TCheckContext* context,
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

    TMaybe<ENodeType> ValidatorType();

    bool IsMap();
    bool IsArray();
    bool IsInt64();
    bool IsString();
    bool IsBool();
    bool IsScalar();

protected:
    TCheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TValidator* Validator_;
    TMaybe<ENodeType> NodeType_;
    TString PathFromCheckNode_;
};

class TGenericNodeWrapper : public NDetail::TNodeWrapperCommonOps<TGenericNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

// TODO: make some functionality for generic wrapper
public:
    TGenericNodeWrapper(
        TGenericCheckContext* context,
        NFyaml::TNodeRef node,
        TGenericValidator* validator,
        const TString& pathFromCheckNode);
    
    operator TNodeWrapper();

private:
    TGenericCheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TGenericValidator* Validator_;
    TString PathFromCheckNode_;
};

class TMapNodeWrapper : public NDetail::TNodeWrapperCommonOps<TMapNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

public:
    TMapNodeWrapper(
        TMapCheckContext* context,
        NFyaml::TNodeRef node,
        TMapValidator* validator,
        const TString& pathFromCheckNode);
    
    operator TNodeWrapper();
    
    TNodeWrapper operator[](const TString& field);
    TNodeWrapper At(const TString& field);
    bool Has(const TString& field);

private:
    TMapCheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TMapValidator* Validator_;
    TString PathFromCheckNode_;

    NFyaml::TMapping Map();
};

class TArrayNodeWrapper : public NDetail::TNodeWrapperCommonOps<TArrayNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

public:
    TArrayNodeWrapper(
        TArrayCheckContext* context,
        NFyaml::TNodeRef node,
        TArrayValidator* validator,
        const TString& pathFromCheckNode);

    int Length();

    TNodeWrapper operator[](size_t index);

    operator TNodeWrapper();

private:
    TArrayCheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TArrayValidator* Validator_;
    TString PathFromCheckNode_;

    NFyaml::TSequence Sequence();
};

class TInt64NodeWrapper : public NDetail::TNodeWrapperCommonOps<TInt64NodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

public:
    TInt64NodeWrapper(
        TInt64CheckContext* context,
        NFyaml::TNodeRef node,
        TInt64Validator* validator,
        const TString& pathFromCheckNode);

    i64 Value();
    operator i64();

    operator TNodeWrapper();

private:
    TInt64CheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TInt64Validator* Validator_;
    TString PathFromCheckNode_;
};

class TStringNodeWrapper : public NDetail::TNodeWrapperCommonOps<TStringNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

public:
    TStringNodeWrapper(
        TStringCheckContext* context,
        NFyaml::TNodeRef node,
        TStringValidator* validator,
        const TString& pathFromCheckNode);

    TString Value();
    operator TString();

    operator TNodeWrapper();

private:
    TStringCheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TStringValidator* Validator_;
    TString PathFromCheckNode_;
};

class TBoolNodeWrapper : public NDetail::TNodeWrapperCommonOps<TBoolNodeWrapper> {
    template <typename> friend class NDetail::TNodeWrapperCommonOps;

public:
    TBoolNodeWrapper(
        TBoolCheckContext* context,
        NFyaml::TNodeRef node,
        TBoolValidator* validator,
        const TString& pathFromCheckNode);

    bool Value();
    operator bool();

    operator TNodeWrapper();

private:
    TBoolCheckContext* Context_;
    NFyaml::TNodeRef Node_;
    TBoolValidator* Validator_;
    TString PathFromCheckNode_;
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

    virtual ~TCheckContext() = default;

protected:
    TVector<TString> Errors_;
    NFyaml::TNodeRef Node_;
    TString CheckNodePath_;
    bool someExpectFailed = false;
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

} // namespace NYamlConfig::NValidator
