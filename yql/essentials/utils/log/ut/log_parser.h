#pragma once

#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/log/log_level.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

#include <util/datetime/base.h>

namespace NYql::NLog {

struct TLogRow {
    TInstant Time;
    ELevel Level;
    TString ProcName;
    pid_t ProcId;
    ui64 ThreadId;
    EComponent Component;
    TString FileName;
    ui32 LineNumber;
    TString Path;
    TString Message;
};

TLogRow ParseLegacyLogRow(TStringBuf str);
TLogRow ParseJsonLogRow(TStringBuf str);

} // namespace NYql::NLog
