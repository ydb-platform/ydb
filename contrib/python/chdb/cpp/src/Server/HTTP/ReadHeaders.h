#pragma once

#include <CHDBPoco/Net/MessageHeader.h>

namespace DB_CHDB
{

class ReadBuffer;

void readHeaders(
    CHDBPoco::Net::MessageHeader & headers, ReadBuffer & in, size_t max_fields_number, size_t max_name_length, size_t max_value_length);

}
