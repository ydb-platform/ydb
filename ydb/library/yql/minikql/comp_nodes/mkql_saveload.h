#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>

#include <util/generic/strbuf.h>

#include <string_view>

namespace NKikimr {
namespace NMiniKQL {

Y_FORCE_INLINE void WriteByte(TString& out, ui8 value) { 
    out.append((char)value); 
} 
 
Y_FORCE_INLINE void WriteBool(TString& out, bool value) {
    out.append((char)value);
}

Y_FORCE_INLINE void WriteUi32(TString& out, ui32 value) {
    char buf[MAX_PACKED32_SIZE];
    out.AppendNoAlias(buf, Pack32(value, buf));
}

Y_FORCE_INLINE void WriteUi64(TString& out, ui64 value) {
    char buf[MAX_PACKED64_SIZE];
    out.AppendNoAlias(buf, Pack64(value, buf));
}

Y_FORCE_INLINE bool ReadBool(TStringBuf& in) {
    MKQL_ENSURE(in.size(), "Serialized state is corrupted");
    bool result = (bool)*in.data();
    in.Skip(1);
    return result;
}

Y_FORCE_INLINE ui8 ReadByte(TStringBuf& in) { 
    MKQL_ENSURE(in.size(), "Serialized state is corrupted");
    ui8 result = *in.data();
    in.Skip(1); 
    return result; 
} 
 
Y_FORCE_INLINE ui32 ReadUi32(TStringBuf& in) {
    ui32 result;
    auto count = Unpack32(in.data(), in.size(), result);
    MKQL_ENSURE(count, "Serialized state is corrupted");
    in.Skip(count);
    return result;
}

Y_FORCE_INLINE ui64 ReadUi64(TStringBuf& in) {
    ui64 result;
    auto count = Unpack64(in.data(), in.size(), result);
    MKQL_ENSURE(count, "Serialized state is corrupted");
    in.Skip(count);
    return result;
}

Y_FORCE_INLINE void WriteString(TString& out, std::string_view str) {
    WriteUi32(out, str.size());
    out.AppendNoAlias(str.data(), str.size());
}

Y_FORCE_INLINE std::string_view ReadString(TStringBuf& in) {
    const ui32 size = ReadUi32(in);
    MKQL_ENSURE(in.Size() >= size, "Serialized state is corrupted");
    TStringBuf head = in.Head(size);
    in = in.Tail(size);
    return head;
}

Y_FORCE_INLINE void WriteUnboxedValue(TString& out, const TValuePacker& packer, const NUdf::TUnboxedValue& value) {
    auto state = packer.Pack(value);
    WriteUi32(out, state.size());
    out.AppendNoAlias(state.data(), state.size());
}

Y_FORCE_INLINE NUdf::TUnboxedValue ReadUnboxedValue(TStringBuf& in, const TValuePacker& packer, TComputationContext& ctx) {
    auto size = ReadUi32(in);
    MKQL_ENSURE(size <= in.size(), "Serialized state is corrupted");
    auto value = packer.Unpack(TStringBuf(in.data(), in.data() + size), ctx.HolderFactory);
    in.Skip(size);
    return value;
}

} // namespace NMiniKQL
} // namespace NKikimr
