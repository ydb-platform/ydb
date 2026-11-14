#include "table_digest.h"

#include <ydb/library/table_digest/json_document.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/digest/city.h>
#include <util/generic/yexception.h>
#include <util/system/guard.h>
#include <util/system/unaligned_mem.h>

#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <numeric>
#include <type_traits>

namespace NKikimr::NTableDigest {
namespace {

namespace NUdf = NYql::NUdf;

template <class T> NUdf::TUnboxedValuePod ReadValue(const TCell &cell) {
  if constexpr (std::is_same_v<T, bool>) {
    const ui8 value = cell.AsValue<ui8>();
    Y_ENSURE(value <= 1, "Invalid TableDigest Bool cell");
    return NUdf::TUnboxedValuePod(bool(value));
  } else {
    return NUdf::TUnboxedValuePod(cell.AsValue<T>());
  }
}

NUdf::TUnboxedValue CellToValue(const NScheme::TTypeInfo &type,
                                const TCell &cell) {
  using namespace NScheme;
  switch (type.GetTypeId()) {
#define DIGEST_FIXED_VALUE(type, layout)                                       \
  case NUdf::TDataType<type>::Id:                                              \
    return ReadValue<layout>(cell);
    KNOWN_FIXED_VALUE_TYPES(DIGEST_FIXED_VALUE)
#undef DIGEST_FIXED_VALUE
  case NTypeIds::Decimal: {
    Y_ENSURE(cell.Size() == 16,
             "Invalid TableDigest Decimal cell size: " << cell.Size());
    const auto coefficient = NYql::NDecimal::FromHalfs(
        ReadUnaligned<ui64>(cell.Data()),
        ReadUnaligned<i64>(cell.Data() + sizeof(ui64)));
    Y_ENSURE(NYql::NDecimal::IsNormal(coefficient,
                                      type.GetDecimalType().GetPrecision()) ||
                 coefficient == NYql::NDecimal::Inf() ||
                 coefficient == -NYql::NDecimal::Inf() ||
                 coefficient == NYql::NDecimal::Nan(),
             "Invalid Digest Decimal coefficient");
    return NUdf::TUnboxedValuePod(coefficient);
  }
  case NTypeIds::JsonDocument:
    return NMiniKQL::MakeString(
        NYql::NDigest::CanonicalJsonDocument(cell.AsBuf()));
  case NTypeIds::Uuid:
    Y_ENSURE(cell.Size() == 16,
             "Invalid TableDigest Uuid cell size: " << cell.Size());
    [[fallthrough]];
  case NTypeIds::String:
  case NTypeIds::Utf8:
  case NTypeIds::Yson:
  case NTypeIds::Json:
  case NTypeIds::DyNumber:
    return NMiniKQL::MakeString(NUdf::TStringRef(cell.Data(), cell.Size()));
  default:
    ythrow yexception() << "Unsupported TableDigest cell type: "
                        << type.GetTypeId();
  }
}

} // namespace

struct TAccumulator::TImpl {
  NMiniKQL::TScopedAlloc Alloc;
  NMiniKQL::TTypeEnvironment Env;
  NMiniKQL::TMemoryUsageInfo MemInfo;
  NMiniKQL::THolderFactory HolderFactory;
  std::unique_ptr<NMiniKQL::TValuePacker> Packer;
  TVector<size_t> ColumnOrder;

  explicit TImpl(TConstArrayRef<TColumn> columns)
      : Alloc(__LOCATION__), Env(Alloc), MemInfo("TableDigest"),
        HolderFactory(Alloc.Ref(), MemInfo)
  {
    ColumnOrder.resize(columns.size());
    std::iota(ColumnOrder.begin(), ColumnOrder.end(), size_t(0));
    std::sort(ColumnOrder.begin(), ColumnOrder.end(), [&](size_t a, size_t b) {
      return columns[a].Name < columns[b].Name;
    });
    TVector<std::pair<TString, NMiniKQL::TType *>> members;
    members.reserve(columns.size());
    for (const size_t index : ColumnOrder) {
      const auto &column = columns[index];
      Y_ENSURE(
          NScheme::NTypeIds::IsYqlType(column.Type.GetTypeId()),
          "Unsupported TableDigest cell type: " << column.Type.GetTypeId());
      NMiniKQL::TType *type;
      if (column.Type.GetTypeId() == NScheme::NTypeIds::Decimal) {
        const auto &decimal = column.Type.GetDecimalType();
        type = NMiniKQL::TDataDecimalType::Create(decimal.GetPrecision(),
                                                  decimal.GetScale(), Env);
      } else {
        const auto packedType =
            column.Type.GetTypeId() == NScheme::NTypeIds::JsonDocument
                ? NScheme::NTypeIds::String
                : column.Type.GetTypeId();
        type = NMiniKQL::TDataType::Create(packedType, Env);
      }
      if (column.Nullable) {
        type = NMiniKQL::TOptionalType::Create(type, Env);
      }
      members.emplace_back(column.Name, type);
    }
    const auto *rowType =
        NMiniKQL::TStructType::Create(members.data(), members.size(), Env);
    Packer = std::make_unique<NMiniKQL::TValuePacker>(true, rowType);
    Alloc.Release();
  }

  ~TImpl() { Alloc.Acquire(); }

  TStringBuf Pack(TConstArrayRef<TColumn> columns,
                  TConstArrayRef<TCell> cells) {
    TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
    NUdf::TUnboxedValue *items = nullptr;
    NUdf::TUnboxedValue row =
        HolderFactory.CreateDirectArrayHolder(columns.size(), items);
    for (size_t i = 0; i < ColumnOrder.size(); ++i) {
      const size_t index = ColumnOrder[i];
      const auto &column = columns[index];
      const auto &cell = cells[index];
      if (cell.IsNull()) {
        Y_ENSURE(column.Nullable,
                 "NULL in required Digest column: " << column.Name);
        continue;
      }
      auto value = CellToValue(column.Type, cell);
      items[i] =
          column.Nullable ? value.Release().MakeOptional() : value.Release();
    }
    return Packer->Pack(row);
  }
};

TAccumulator::TAccumulator(TVector<TColumn> columns,
                           NYql::NDigest::EAlgorithm algorithm)
    : Columns_(std::move(columns)), State_(algorithm),
      Impl_(std::make_unique<TImpl>(Columns_))
{}

TAccumulator::~TAccumulator() = default;

void TAccumulator::AddRow(TConstArrayRef<TCell> cells) {
  Y_ENSURE(cells.size() == Columns_.size(),
           "TableDigest cell count does not match schema");
  const auto bytes = Impl_->Pack(Columns_, cells);
  switch (State_.GetAlgorithm()) {
  case NYql::NDigest::EAlgorithm::CityHash64:
    State_.AddHash(
        NYql::NDigest::TWords{::CityHash64(bytes.data(), bytes.size())});
    break;
  case NYql::NDigest::EAlgorithm::CityHash128: {
    const auto hash = ::CityHash128(bytes.data(), bytes.size());
    State_.AddHash(NYql::NDigest::TWords{hash.first, hash.second});
    break;
  }
  case NYql::NDigest::EAlgorithm::Sha256: {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> hash;
    SHA256(reinterpret_cast<const unsigned char *>(bytes.data()), bytes.size(),
           hash.data());
    State_.AddHash(
        TStringBuf(reinterpret_cast<const char *>(hash.data()), hash.size()));
    break;
  }
  }
}

void TAccumulator::Merge(const NYql::NDigest::TState &other) {
  State_.Merge(other);
}

} // namespace NKikimr::NTableDigest
