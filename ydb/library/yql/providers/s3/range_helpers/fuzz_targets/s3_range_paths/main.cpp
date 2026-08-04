#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

void ExercisePacked(TStringBuf packed, bool isTextEncoded) {
    if (packed.size() > MaxInputSize) {
        return;
    }

    try {
        NYql::NS3Details::TPathList paths;
        NYql::NS3Details::UnpackPathsList(packed, isTextEncoded, paths);
        TString repacked;
        bool repackedIsText = false;
        NYql::NS3Details::PackPathsList(paths, repacked, repackedIsText);
    } catch (...) {
    }
}

NYql::NS3::TRange MakeNestedRange(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    NYql::NS3::TRange range;
    auto* path = range.AddPaths();
    for (ui16 i = 0; i < depth; ++i) {
        path->SetName("d");
        path->SetIsDirectory(true);
        path = path->AddChildren();
    }
    path->SetName("leaf");
    path->SetRead(true);
    path->SetSize(provider.ConsumeIntegral<ui64>());
    return range;
}

void ExerciseGeneratedRange(FuzzedDataProvider& provider) {
    const auto range = MakeNestedRange(provider);
    ExercisePacked(range.SerializeAsString(), false);
}

void ExerciseGeneratedPathList(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    TString path;
    for (ui16 i = 0; i < depth; ++i) {
        if (!path.empty()) {
            path += '/';
        }
        path += 'd';
    }
    if (path.empty()) {
        path = "leaf";
    }

    try {
        NYql::NS3Details::TPathList paths;
        paths.emplace_back(path, provider.ConsumeIntegral<ui64>(), false, 0);
        TString packed;
        bool isTextEncoded = false;
        NYql::NS3Details::PackPathsList(paths, packed, isTextEncoded);
        ExercisePacked(packed, isTextEncoded);
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    const TStringBuf raw(reinterpret_cast<const char*>(data), size);
    ExercisePacked(raw, false);
    ExercisePacked(raw, true);

    FuzzedDataProvider provider(data, size);
    ExerciseGeneratedRange(provider);
    ExerciseGeneratedPathList(provider);

    return 0;
}
