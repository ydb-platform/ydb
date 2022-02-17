#include "pagination.h"

using namespace NYdb;
using namespace NYdb::NTable;

struct TSchool {
    TString City;
    ui32 Number;
    TString Address;

    TSchool(const TString& city, ui32 number, const TString& address)
        : City(city)
        , Number(number)
        , Address(address) {}
};

TParams GetTablesDataParams() {
    TVector<TSchool> schoolsData = {
        TSchool("Орлов", 1, "Ст.Халтурина, 2"),
        TSchool("Орлов", 2, "Свободы, 4"),
        TSchool("Яранск", 1, "Гоголя, 25"),
        TSchool("Яранск", 2, "Кирова, 18"),
        TSchool("Яранск", 3, "Некрасова, 59"),
        TSchool("Кирс", 3, "Кирова, 6"),
        TSchool("Нолинск", 1, "Коммуны, 4"),
        TSchool("Нолинск", 2, "Федосеева, 2Б"),
        TSchool("Котельнич", 1, "Урицкого, 21"),
        TSchool("Котельнич", 2, "Октябрьская, 109"),
        TSchool("Котельнич", 3, "Советская, 153"),
        TSchool("Котельнич", 5, "Школьная, 2"),
        TSchool("Котельнич", 15, "Октябрьская, 91")
    };

    TParamsBuilder paramsBuilder;

    auto& Param = paramsBuilder.AddParam("$schoolsData");
    Param.BeginList();
    for (auto& school : schoolsData) {
        Param.AddListItem()
            .BeginStruct()
            .AddMember("city")
            .Utf8(school.City)
            .AddMember("number")
            .Uint32(school.Number)
            .AddMember("address")
            .Utf8(school.Address)
            .EndStruct();
    }
    Param.EndList();
    Param.Build();

    return paramsBuilder.Build();
}
