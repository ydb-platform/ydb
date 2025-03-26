#include "yql_udf_resolver.h"

namespace NYql {

TResolveResult LoadRichMetadata(const IUdfResolver& resolver, const TVector<TUserDataBlock>& blocks, NUdf::ELogLevel logLevel) {
    TVector<IUdfResolver::TImport> imports;
    imports.reserve(blocks.size());
    std::transform(blocks.begin(), blocks.end(), std::back_inserter(imports), [](auto& b) {
        IUdfResolver::TImport import;
        import.Block = &b;
        // this field is not used later, but allows us to map importResult to import
        import.FileAlias = b.Data;
        return import;
    });

    return resolver.LoadRichMetadata(imports, logLevel);
}

TResolveResult LoadRichMetadata(const IUdfResolver& resolver, const TVector<TString>& paths, NUdf::ELogLevel logLevel) {
    TVector<TUserDataBlock> blocks;
    blocks.reserve(paths.size());
    std::transform(paths.begin(), paths.end(), std::back_inserter(blocks), [](auto& p) {
        TUserDataBlock b;
        b.Type = EUserDataType::PATH;
        b.Data = p;
        b.Usage.Set(EUserDataBlockUsage::Udf);
        return b;
    });

    return LoadRichMetadata(resolver, blocks, logLevel);
}

}
