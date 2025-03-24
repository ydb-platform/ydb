#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <util/generic/hash.h>
#include <util/string/type.h>

#include <map>

namespace NYdb {
namespace NConsoleClient {

class TYqlParser {
public:
    // Получает типы параметров из текста YQL запроса
    // Возвращает map, где ключ - имя параметра (с $), значение - тип параметра
    static std::map<std::string, TType> GetParamTypes(const TString& queryText);

private:
    // Обрабатывает строковое представление типа и создает соответствующий тип
    static void ProcessType(const TString& typeStr, TTypeBuilder& builder);
};

} // namespace NConsoleClient
} // namespace NYdb 