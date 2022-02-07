#pragma once

#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/node/node.h>

#include <util/stream/output.h>
#include <util/generic/strbuf.h>

#include <functional>

namespace NKikimr {
namespace NMiniKQL {

class TType;
class TStructType;
class TProgramBuilder;

}
}

namespace NYql {
namespace NCommon {

struct TCodecContext;

void WriteTypeToYson(NYson::TYsonConsumerBase& writer, const NKikimr::NMiniKQL::TType* type);
NYT::TNode TypeToYsonNode(const NKikimr::NMiniKQL::TType* type);
TString WriteTypeToYson(const NKikimr::NMiniKQL::TType* type, NYT::NYson::EYsonFormat format = NYT::NYson::EYsonFormat::Binary);

NKikimr::NMiniKQL::TType* ParseTypeFromYson(const TStringBuf yson, NKikimr::NMiniKQL::TProgramBuilder& builder, IOutputStream& err);
NKikimr::NMiniKQL::TType* ParseTypeFromYson(const NYT::TNode& node, NKikimr::NMiniKQL::TProgramBuilder& builder, IOutputStream& err);
NKikimr::NMiniKQL::TType* ParseOrderAwareTypeFromYson(const NYT::TNode& node, TCodecContext& ctx, IOutputStream& err);

} // namespace NCommon
} // namespace NYql
