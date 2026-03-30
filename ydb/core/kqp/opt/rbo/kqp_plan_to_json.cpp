#include "kqp_operator.h"
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_reader.h>

NJson::TJsonValue TOpRoot::GetExecutionJson() {
    // First construct the ResultSet and Stage JSON objects

    for (auto it : *this) {
        
    }
}

NJson::TJsonValue TOpRoot::GetExplainJson();

