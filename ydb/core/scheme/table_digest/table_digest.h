#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/library/table_digest/aggregate.h>

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>

#include <memory>

namespace NKikimr::NTableDigest {

// Descriptors and cells use the same caller-supplied order. Columns are packed
// in name order, matching StablePickle(TableRow()). Schema comparison belongs
// to the caller.
struct TColumn {
  TString Name;
  NScheme::TTypeInfo Type;
  bool Nullable = true;
};

// A reusable adapter for a shard scan or another native TCell consumer.
// Snapshot selection and range filtering belong to the caller that supplies the
// rows.
class TAccumulator {
public:
  explicit TAccumulator(TVector<TColumn> columns,
                        NYql::NDigest::EAlgorithm algorithm =
                            NYql::NDigest::EAlgorithm::CityHash64);
  ~TAccumulator();

  void AddRow(TConstArrayRef<TCell> cells);
  void Merge(const NYql::NDigest::TState &other);

  const NYql::NDigest::TState &GetState() const { return State_; }

private:
  TVector<TColumn> Columns_;
  NYql::NDigest::TState State_;
  struct TImpl;
  std::unique_ptr<TImpl> Impl_;
};

} // namespace NKikimr::NTableDigest
