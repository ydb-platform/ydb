#pragma once

#include <ydb/library/actors/util/rope.h>

#include <memory>

namespace NYql {

TRope MakeReadOnlyRope(const std::shared_ptr<const void>& owner, const char* data, size_t size);

}