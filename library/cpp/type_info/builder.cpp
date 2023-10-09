#include "builder.h"

#include "type_factory.h"

namespace NTi {
    TStructBuilderRaw::TStructBuilderRaw(IPoolTypeFactory& factory) noexcept
        : Factory_(&factory)
    {
    }

    TStructBuilderRaw::TStructBuilderRaw(IPoolTypeFactory& factory, TStructTypePtr prototype) noexcept
        : TStructBuilderRaw(factory, prototype.Get())
    {
    }

    TStructBuilderRaw::TStructBuilderRaw(IPoolTypeFactory& factory, const TStructType* prototype) noexcept
        : TStructBuilderRaw(factory)
    {
        Members_.reserve(prototype->GetMembers().size());

        if (prototype->GetFactory() == Factory_) {
            // members are in the same factory -- can reuse them.
            for (auto member : prototype->GetMembers()) {
                Members_.push_back(member);
            }
        } else {
            // members are in a different factory -- should copy them.
            for (auto member : prototype->GetMembers()) {
                AddMember(member.GetName(), member.GetTypeRaw());
            }
        }
    }

    TStructBuilderRaw& TStructBuilderRaw::SetName(TMaybe<TStringBuf> name) & noexcept {
        Name_ = Factory_->AllocateStringMaybe(name);
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::SetName(TMaybe<TStringBuf> name) && noexcept {
        return std::move(SetName(name));
    }

    bool TStructBuilderRaw::HasName() const noexcept {
        return Name_.Defined();
    }

    TMaybe<TStringBuf> TStructBuilderRaw::GetName() const noexcept {
        return Name_;
    }

    TStructBuilderRaw& TStructBuilderRaw::Reserve(size_t size) & noexcept {
        Members_.reserve(size);
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::Reserve(size_t size) && noexcept {
        return std::move(Reserve(size));
    }

    TStructBuilderRaw& TStructBuilderRaw::AddMember(TStringBuf name, TTypePtr type) & noexcept {
        return AddMember(name, type.Get());
    }

    TStructBuilderRaw TStructBuilderRaw::AddMember(TStringBuf name, TTypePtr type) && noexcept {
        return std::move(AddMember(name, type));
    }

    TStructBuilderRaw& TStructBuilderRaw::AddMember(TStringBuf name, const TType* type) & noexcept {
        Members_.emplace_back(Factory_->AllocateString(name), Factory_->Own(type));
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::AddMember(TStringBuf name, const TType* type) && noexcept {
        return std::move(AddMember(name, type));
    }

    TStructBuilderRaw& TStructBuilderRaw::AddMemberName(TStringBuf name) & noexcept {
        PendingMemberName_ = Factory_->AllocateString(name);
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::AddMemberName(TStringBuf name) && noexcept {
        return std::move(AddMemberName(name));
    }

    TStructBuilderRaw& TStructBuilderRaw::DiscardMemberName() & noexcept {
        PendingMemberName_.Clear();
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::DiscardMemberName() && noexcept {
        return std::move(DiscardMemberName());
    }

    bool TStructBuilderRaw::HasMemberName() const noexcept {
        return PendingMemberName_.Defined();
    }

    TMaybe<TStringBuf> TStructBuilderRaw::GetMemberName() const noexcept {
        return PendingMemberName_;
    }

    TStructBuilderRaw& TStructBuilderRaw::AddMemberType(TTypePtr type) & noexcept {
        return AddMemberType(type.Get());
    }

    TStructBuilderRaw TStructBuilderRaw::AddMemberType(TTypePtr type) && noexcept {
        return std::move(AddMemberType(type));
    }

    TStructBuilderRaw& TStructBuilderRaw::AddMemberType(const TType* type) & noexcept {
        PendingMemberType_ = Factory_->Own(type);
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::AddMemberType(const TType* type) && noexcept {
        return std::move(AddMemberType(type));
    }

    TStructBuilderRaw& TStructBuilderRaw::DiscardMemberType() & noexcept {
        PendingMemberType_.Clear();
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::DiscardMemberType() && noexcept {
        return std::move(DiscardMemberType());
    }

    bool TStructBuilderRaw::HasMemberType() const noexcept {
        return PendingMemberType_.Defined();
    }

    TMaybe<const TType*> TStructBuilderRaw::GetMemberType() const noexcept {
        return PendingMemberType_;
    }

    bool TStructBuilderRaw::CanAddMember() const noexcept {
        return HasMemberName() && HasMemberType();
    }

    TStructBuilderRaw& TStructBuilderRaw::AddMember() & noexcept {
        Y_ABORT_UNLESS(CanAddMember());
        Members_.emplace_back(*PendingMemberName_, *PendingMemberType_);
        DiscardMember();
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::AddMember() && noexcept {
        return std::move(AddMember());
    }

    TStructBuilderRaw& TStructBuilderRaw::DiscardMember() & noexcept {
        DiscardMemberName();
        DiscardMemberType();
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::DiscardMember() && noexcept {
        return std::move(DiscardMember());
    }

    TStructType::TMembers TStructBuilderRaw::GetMembers() const noexcept {
        return Members_;
    }

    TStructBuilderRaw& TStructBuilderRaw::Reset() & noexcept {
        Name_ = {};
        Members_.clear();
        return *this;
    }

    TStructBuilderRaw TStructBuilderRaw::Reset() && noexcept {
        return std::move(Reset());
    }

    TStructTypePtr TStructBuilderRaw::Build() {
        return BuildRaw()->AsPtr();
    }

    const TStructType* TStructBuilderRaw::BuildRaw() {
        return DoBuildRaw(Name_);
    }

    TVariantTypePtr TStructBuilderRaw::BuildVariant() {
        return BuildVariantRaw()->AsPtr();
    }

    const TVariantType* TStructBuilderRaw::BuildVariantRaw() {
        return Factory_->New<TVariantType>(Nothing(), Name_, DoBuildRaw(Nothing()));
    }

    const TStructType* TStructBuilderRaw::DoBuildRaw(TMaybe<TStringBuf> name) {
        auto members = Factory_->NewArray<TStructType::TMember>(Members_.size(), [this](TStructType::TMember* member, size_t i) {
            new (member) TStructType::TMember(Members_[i]);
        });

        auto sortedMembersArray = Factory_->AllocateArrayFor<size_t>(Members_.size());
        auto sortedMembers = TArrayRef(sortedMembersArray, members.size());
        TStructType::MakeSortedMembers(members, sortedMembers);

        return Factory_->New<TStructType>(Nothing(), name, members, sortedMembers);
    }

    TTupleBuilderRaw::TTupleBuilderRaw(IPoolTypeFactory& factory) noexcept
        : Factory_(&factory)
    {
    }

    TTupleBuilderRaw::TTupleBuilderRaw(IPoolTypeFactory& factory, TTupleTypePtr prototype) noexcept
        : TTupleBuilderRaw(factory, prototype.Get())
    {
    }

    TTupleBuilderRaw::TTupleBuilderRaw(IPoolTypeFactory& factory, const TTupleType* prototype) noexcept
        : TTupleBuilderRaw(factory)
    {
        Elements_.reserve(prototype->GetElements().size());

        if (prototype->GetFactory() == Factory_) {
            // elements are in the same factory -- can reuse them
            for (auto element : prototype->GetElements()) {
                Elements_.push_back(element);
            }
        } else {
            // items are in a different factory -- should copy them
            for (auto element : prototype->GetElements()) {
                AddElement(element.GetTypeRaw());
            }
        }
    }

    TTupleBuilderRaw& TTupleBuilderRaw::SetName(TMaybe<TStringBuf> name) & noexcept {
        Name_ = Factory_->AllocateStringMaybe(name);
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::SetName(TMaybe<TStringBuf> name) && noexcept {
        return std::move(SetName(name));
    }

    bool TTupleBuilderRaw::HasName() const noexcept {
        return Name_.Defined();
    }

    TMaybe<TStringBuf> TTupleBuilderRaw::GetName() const noexcept {
        return Name_;
    }

    TTupleBuilderRaw& TTupleBuilderRaw::Reserve(size_t size) & noexcept {
        Elements_.reserve(size);
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::Reserve(size_t size) && noexcept {
        return std::move(Reserve(size));
    }

    TTupleBuilderRaw& TTupleBuilderRaw::AddElement(TTypePtr type) & noexcept {
        return AddElement(type.Get());
    }

    TTupleBuilderRaw TTupleBuilderRaw::AddElement(TTypePtr type) && noexcept {
        return std::move(AddElement(type));
    }

    TTupleBuilderRaw& TTupleBuilderRaw::AddElement(const TType* type) & noexcept {
        Elements_.emplace_back(Factory_->Own(type));
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::AddElement(const TType* type) && noexcept {
        return std::move(AddElement(type));
    }

    TTupleBuilderRaw& TTupleBuilderRaw::AddElementType(TTypePtr type) & noexcept {
        return AddElementType(type.Get());
    }

    TTupleBuilderRaw TTupleBuilderRaw::AddElementType(TTypePtr type) && noexcept {
        return std::move(AddElementType(type));
    }

    TTupleBuilderRaw& TTupleBuilderRaw::AddElementType(const TType* type) & noexcept {
        PendingElementType_ = Factory_->Own(type);
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::AddElementType(const TType* type) && noexcept {
        return std::move(AddElementType(type));
    }

    TTupleBuilderRaw& TTupleBuilderRaw::DiscardElementType() & noexcept {
        PendingElementType_.Clear();
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::DiscardElementType() && noexcept {
        return std::move(DiscardElementType());
    }

    bool TTupleBuilderRaw::HasElementType() const noexcept {
        return PendingElementType_.Defined();
    }

    TMaybe<const TType*> TTupleBuilderRaw::GetElementType() const noexcept {
        return PendingElementType_;
    }

    bool TTupleBuilderRaw::CanAddElement() const noexcept {
        return HasElementType();
    }

    TTupleBuilderRaw& TTupleBuilderRaw::AddElement() & noexcept {
        Y_ABORT_UNLESS(CanAddElement());
        Elements_.emplace_back(*PendingElementType_);
        DiscardElement();
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::AddElement() && noexcept {
        return std::move(AddElement());
    }

    TTupleBuilderRaw& TTupleBuilderRaw::DiscardElement() & noexcept {
        DiscardElementType();
        return *this;
    }

    TTupleBuilderRaw TTupleBuilderRaw::DiscardElement() && noexcept {
        return std::move(DiscardElement());
    }

    TTupleBuilderRaw& TTupleBuilderRaw::Reset() & noexcept {
        Name_ = {};
        Elements_.clear();
        return *this;
    }

    TTupleType::TElements TTupleBuilderRaw::GetElements() const noexcept {
        return Elements_;
    }

    TTupleBuilderRaw TTupleBuilderRaw::Reset() && noexcept {
        return std::move(Reset());
    }

    TTupleTypePtr TTupleBuilderRaw::Build() {
        return BuildRaw()->AsPtr();
    }

    const TTupleType* TTupleBuilderRaw::BuildRaw() {
        return DoBuildRaw(Name_);
    }

    TVariantTypePtr TTupleBuilderRaw::BuildVariant() {
        return BuildVariantRaw()->AsPtr();
    }

    const TVariantType* TTupleBuilderRaw::BuildVariantRaw() {
        return Factory_->New<TVariantType>(Nothing(), Name_, DoBuildRaw(Nothing()));
    }

    const TTupleType* TTupleBuilderRaw::DoBuildRaw(TMaybe<TStringBuf> name) {
        auto items = Factory_->NewArray<TTupleType::TElement>(Elements_.size(), [this](TTupleType::TElement* element, size_t i) {
            new (element) TTupleType::TElement(Elements_[i]);
        });

        return Factory_->New<TTupleType>(Nothing(), name, items);
    }

    TTaggedBuilderRaw::TTaggedBuilderRaw(IPoolTypeFactory& factory) noexcept
        : Factory_(&factory)
    {
    }

    TTaggedBuilderRaw& TTaggedBuilderRaw::SetTag(TStringBuf tag) & noexcept {
        Tag_ = Factory_->AllocateString(tag);
        return *this;
    }

    TTaggedBuilderRaw TTaggedBuilderRaw::SetTag(TStringBuf tag) && noexcept {
        return std::move(SetTag(tag));
    }

    TTaggedBuilderRaw& TTaggedBuilderRaw::DiscardTag() & noexcept {
        Tag_.Clear();
        return *this;
    }

    TTaggedBuilderRaw TTaggedBuilderRaw::DiscardTag() && noexcept {
        return std::move(DiscardTag());
    }

    bool TTaggedBuilderRaw::HasTag() const noexcept {
        return Tag_.Defined();
    }

    TMaybe<TStringBuf> TTaggedBuilderRaw::GetTag() const noexcept {
        return Tag_;
    }

    TTaggedBuilderRaw& TTaggedBuilderRaw::SetItem(TTypePtr type) & noexcept {
        return SetItem(type.Get());
    }

    TTaggedBuilderRaw TTaggedBuilderRaw::SetItem(TTypePtr type) && noexcept {
        return std::move(SetItem(std::move(type)));
    }

    TTaggedBuilderRaw& TTaggedBuilderRaw::SetItem(const TType* type) & noexcept {
        Item_ = Factory_->Own(type);
        return *this;
    }

    TTaggedBuilderRaw TTaggedBuilderRaw::SetItem(const TType* type) && noexcept {
        return std::move(SetItem(type));
    }

    TTaggedBuilderRaw& TTaggedBuilderRaw::DiscardItem() & noexcept {
        Item_.Clear();
        return *this;
    }

    TTaggedBuilderRaw TTaggedBuilderRaw::DiscardItem() && noexcept {
        return std::move(DiscardItem());
    }

    bool TTaggedBuilderRaw::HasItem() const noexcept {
        return Item_.Defined();
    }

    TMaybe<const TType*> TTaggedBuilderRaw::GetItem() const noexcept {
        return Item_;
    }

    bool TTaggedBuilderRaw::CanBuild() const noexcept {
        return HasTag() && HasItem();
    }

    TTaggedBuilderRaw& TTaggedBuilderRaw::Reset() & noexcept {
        DiscardTag();
        DiscardItem();
        return *this;
    }

    TTaggedBuilderRaw TTaggedBuilderRaw::Reset() && noexcept {
        return std::move(Reset());
    }

    TTaggedTypePtr TTaggedBuilderRaw::Build() {
        return BuildRaw()->AsPtr();
    }

    const TTaggedType* TTaggedBuilderRaw::BuildRaw() {
        Y_ABORT_UNLESS(CanBuild());
        return Factory_->New<TTaggedType>(Nothing(), *Item_, *Tag_);
    }
}
