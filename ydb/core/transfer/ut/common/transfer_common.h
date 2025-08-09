#pragma once

#include <ydb/core/transfer/ut/common/utils.h>

using namespace NReplicationTest;

void KeyColumnFirst(const std::string& tableType);
void KeyColumnLast(const std::string& tableType);
void ComplexKey(const std::string& tableType);

void NullableColumn(const std::string& tableType);
void WriteNullToKeyColumn(const std::string& tableType);
void WriteNullToColumn(const std::string& tableType);

void Upsert_DifferentBatch(const std::string& tableType);
void Upsert_OneBatch(const std::string& tableType);

void ColumnType_Bool(const std::string& tableType);
void ColumnType_Date(const std::string& tableType);
void ColumnType_Int8(const std::string& tableType);
void ColumnType_Int16(const std::string& tableType);
void ColumnType_Int32(const std::string& tableType);
void ColumnType_Int64(const std::string& tableType);
void ColumnType_Double(const std::string& tableType);
void ColumnType_Utf8_LongValue(const std::string& tableType);
void ColumnType_Uuid(const std::string& tableType);

void MessageField_CreateTimestamp(const std::string& tableType);
void MessageField_MessageGroupId(const std::string& tableType);
void MessageField_Partition(const std::string& tableType);
void MessageField_ProducerId(const std::string& tableType);
void MessageField_SeqNo(const std::string& tableType);
void MessageField_WriteTimestamp(const std::string& tableType);

void ProcessingJsonMessage(const std::string& tableType);
void ProcessingCDCMessage(const std::string& tableType);

void ProcessingTargetTable(const std::string& tableType);
void ProcessingTargetTableOtherType(const std::string& tableType);
