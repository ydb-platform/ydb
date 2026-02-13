#include "print_unboxed_value.h"
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <format>
#include <yql/essentials/public/udf/udf_data_type.h>

namespace NKikimr::NMiniKQL{
std::string_view AsSV(TStringBuf buf) {
  return static_cast<std::string_view>(buf);
}

template<typename T>
struct TPrint: IPrint {
  TPrint() {
    MKQL_ENSURE(false, "priting for this type is not supported, you have to manually add it");
  }
  TString Stringify(NYql::NUdf::TUnboxedValuePod) override {
    MKQL_ENSURE(false, "printing for this type is not supported");
  }

};

template<>
struct TPrint<char*>: IPrint {
    TString Stringify(NYql::NUdf::TUnboxedValuePod value) override {
      return TString{value.AsStringRef()}.Quote() + ":string";
    }
};


template<std::integral Number>
struct TPrint<Number>: IPrint {
    TString Stringify(NYql::NUdf::TUnboxedValuePod value) override {
      return std::format("{}", value.Get<Number>()) + ":integer";
    }
};

template<std::floating_point Float>
struct TPrint<Float>: IPrint {
    TString Stringify(NYql::NUdf::TUnboxedValuePod value) override {
      return std::format("{}", value.Get<Float>()) + ":fp";
    }
};


struct TTuplePrint: IPrint {
  TTuplePrint(const TType* type) {
    auto tupleType = static_cast<const NMiniKQL::TTupleType*>(type);
    for (auto* child: tupleType->GetElements()) {
      ChildPrinters_.push_back(MakePrinter(child));
    }
  }
  TString Stringify(NYql::NUdf::TUnboxedValuePod value) override {
    std::string res;
    
    res.push_back('<');
    for(ui32 index = 0; index < ChildPrinters_.size(); ++index ) {
      auto uv = value.GetElement(index);
      res += std::format("{}, ", ChildPrinters_[index]->Stringify(uv).ConstRef());
    }
    res.pop_back();
    res.pop_back();
    res.push_back('>');
    return res + ":tuple";
  }
  std::vector<IPrint::TPtr> ChildPrinters_;
};

struct OptionalPrint: IPrint {
  OptionalPrint(const TType* type)
  : ChildPrinter_(MakePrinter(AS_TYPE(TOptionalType, type)->GetItemType()))
  {}
  TString Stringify(NYql::NUdf::TUnboxedValuePod value) override {
    return [&]{
      if (!value) {
        return std::string{"None"};
      }else {
        return std::format("Some({})", ChildPrinter_->Stringify(value.GetOptionalValue()));
      }
    }() + ":optional";
    
  }
  IPrint::TPtr ChildPrinter_; 
};


IPrint::TPtr MakePrinter(const TType *type) {
    switch (type->GetKind()) {
        case TTypeBase::EKind::Data: {
            const TDataType* dt = AS_TYPE(TDataType, type);
            TMaybe<NUdf::EDataSlot> dataKind = dt->GetDataSlot();
            MKQL_ENSURE(dataKind.Defined(), "unimplemented print for undefined data slot");
#define MAKE_PRINT(xName, xTypeId, xType, xFeatures, xLayoutType, xParamsCount)    \
    case NYql::NUdf::EDataSlot::xName: \
        return new TPrint<xType>;
            switch (*dataKind) {
              using namespace NYql::NUdf;
              UDF_TYPE_ID_MAP(MAKE_PRINT)
            }

#undef MAKE_PRINT
        }
        case TTypeBase::EKind::Optional: {
          return new OptionalPrint(type);
        }
        case TTypeBase::EKind::Tuple: {
          return new TTuplePrint(type); 
        }
        case TTypeBase::EKind::Type:
        case TTypeBase::EKind::Variant:
        case TTypeBase::EKind::Void:
        case TTypeBase::EKind::Stream:
        case TTypeBase::EKind::Struct:
        case TTypeBase::EKind::List:
        case TTypeBase::EKind::Dict:
        case TTypeBase::EKind::Callable:
        case TTypeBase::EKind::Any:
        case TTypeBase::EKind::Resource:
        case TTypeBase::EKind::Flow:
        case TTypeBase::EKind::Null:
        case TTypeBase::EKind::ReservedKind:
        case TTypeBase::EKind::EmptyList:
        case TTypeBase::EKind::EmptyDict:
        case TTypeBase::EKind::Tagged:
        case TTypeBase::EKind::Block:
        case TTypeBase::EKind::Pg:
        case TTypeBase::EKind::Multi:
        case TTypeBase::EKind::Linear:
          MKQL_ENSURE(false, std::format("unimplemented print for {}", AsSV(type->GetKindAsStr())));
        }
}

}