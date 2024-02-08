#pragma once

#ifndef YT_UNITTEST_LIB_H_
#error "Direct inclusion of this file is not allowed, use yt_unittest_lib.h"
#endif
#undef YT_UNITTEST_LIB_H_

#include <yt/cpp/mapreduce/interface/client.h>

namespace NYT::NTesting {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
TVector<TMessage> ReadProtoTable(const IClientBasePtr& client, const TString& tablePath)
{
    TVector<TMessage> result;
    auto reader = client->CreateTableReader<TMessage>(tablePath);
    for (; reader->IsValid(); reader->Next()) {
        result.push_back(reader->GetRow());
    }
    return result;
}

template <class TMessage>
void WriteProtoTable(const IClientBasePtr& client, const TString& tablePath, const std::vector<TMessage>& rowList)
{
    auto writer = client->CreateTableWriter<TMessage>(tablePath);
    for (const auto& row : rowList) {
        writer->AddRow(row);
    }
    writer->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Testing
