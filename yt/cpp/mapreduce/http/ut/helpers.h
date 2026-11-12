#pragma once

#include <yt/cpp/mapreduce/tests/native/test_helpers/simple_server.h>

#include <util/generic/strbuf.h>
#include <util/stream/fwd.h>

#include <memory>

void WriteDataFrame(TStringBuf string, IOutputStream* stream);
std::unique_ptr<TSimpleServer> CreateFramingEchoServer();
