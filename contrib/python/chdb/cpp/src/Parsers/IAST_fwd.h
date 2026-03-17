#pragma once

#include <algorithm>
#include <memory>
#include <absl/container/inlined_vector.h>

namespace DB_CHDB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
/// sizeof(absl::InlinedVector<ASTPtr, N>) == 8 + N * 16.
/// 7 elements take 120 Bytes which is ~128
using ASTs = absl::InlinedVector<ASTPtr, 7>;

}

namespace std
{

inline typename DB_CHDB::ASTs::size_type erase(DB_CHDB::ASTs & asts, const DB_CHDB::ASTPtr & element) /// NOLINT(cert-dcl58-cpp)
{
    auto old_size = asts.size();
    asts.erase(std::remove(asts.begin(), asts.end(), element), asts.end());
    return old_size - asts.size();
}

template <class Predicate>
inline typename DB_CHDB::ASTs::size_type erase_if(DB_CHDB::ASTs & asts, Predicate pred) /// NOLINT(cert-dcl58-cpp)
{
    auto old_size = asts.size();
    asts.erase(std::remove_if(asts.begin(), asts.end(), pred), asts.end());
    return old_size - asts.size();
}

}
