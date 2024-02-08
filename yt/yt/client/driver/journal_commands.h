#pragma once

#include "command.h"

#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadJournalCommand
    : public TTypedCommand<NApi::TJournalReaderOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReadJournalCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalReader;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteJournalCommand
    : public TTypedCommand<NApi::TJournalWriterOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TWriteJournalCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalWriter;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTruncateJournalCommand
    : public TTypedCommand<NApi::TTruncateJournalOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TTruncateJournalCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;
    i64 RowCount;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
