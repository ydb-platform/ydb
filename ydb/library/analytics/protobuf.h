#pragma once

#include "data.h"
#include <ydb/library/analytics/protos/data.pb.h>

namespace NAnalytics {

inline void ToProtoBuf(const TTable& in, TTableData* tableData)
{
    for (const TRow& row : in) {
        TRowData* rowData = tableData->AddRows();
        if (row.Name) {
            rowData->SetName(row.Name);
        }
        for (const auto& kv : row) {
            TFieldData* fieldData = rowData->AddFields();
            fieldData->SetKey(kv.first);
            fieldData->SetValue(kv.second);
        }
    }
    for (const auto& av : in.Attributes) {
        TAttributeData* attrData = tableData->AddAttributes();
        attrData->SetAttribute(av.first);
        attrData->SetValue(av.second);
    }
}

inline TTable FromProtoBuf(const TTableData& tableData)
{
    TTable table;
    for (int i = 0; i < tableData.GetAttributes().size(); i++) {
        const TAttributeData& attrData = tableData.GetAttributes(i);
        table.Attributes[attrData.GetAttribute()] = attrData.GetValue();
    }

    for (int i = 0; i < tableData.GetRows().size(); i++) {
        const TRowData& rowData = tableData.GetRows(i);
        table.push_back(TRow());
        TRow& row = table.back();
        row.Name = rowData.GetName();
        for (int j = 0; j < rowData.GetFields().size(); j++) {
            const TFieldData& fieldData = rowData.GetFields(j);
            row[fieldData.GetKey()] = fieldData.GetValue();
        }
    }
    return table;
}

}
