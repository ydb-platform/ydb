#pragma once
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimrSchemeOp {
class TOlapColumnDescription;
}

namespace NKikimr::NArrow::NTest {

class TTestColumn {
private:
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(NScheme::TTypeInfo, Type);
    YDB_ACCESSOR_DEF(TString, StorageId);
public:
    TTestColumn(const TString& name, const NScheme::TTypeInfo& type)
        : Name(name)
        , Type(type) {

    }

    NKikimrSchemeOp::TOlapColumnDescription CreateColumn(const ui32 id) const;
    static std::vector<std::pair<TString, NScheme::TTypeInfo>> ConvertToPairs(const std::vector<TTestColumn>& columns);
    static THashMap<TString, NScheme::TTypeInfo> ConvertToHash(const std::vector<TTestColumn>& columns);
    static std::vector<TTestColumn> BuildFromPairs(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);
    static std::vector<TTestColumn> CropSchema(const std::vector<TTestColumn>& input, const ui32 size);
};

}

namespace NKikimr::NArrow {

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const std::vector<NTest::TTestColumn>& columns, const std::set<std::string>& notNullColumns = {});
std::shared_ptr<arrow::Schema> MakeArrowSchema(const std::vector<NTest::TTestColumn>& columns, const std::set<std::string>& notNullColumns = {});

}
