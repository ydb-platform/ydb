package clickhouse

import "github.com/ClickHouse/clickhouse-go/lib/data"

func (ch *clickhouse) sendExternalTables(externalTables []ExternalTable) error {
	ch.logf("[send external tables] count %d", len(externalTables))
	if externalTables == nil || len(externalTables) == 0 {
		return nil
	}
	block := &data.Block{}
	sentTables := make(map[string]bool, 0)
	for _, externalTable := range externalTables {
		if _, ok := sentTables[externalTable.Name]; ok {
			continue
		}
		ch.logf("[send external table] name %s", externalTable.Name)
		sentTables[externalTable.Name] = true
		block.Columns = externalTable.Columns
		block.NumColumns = uint64(len(externalTable.Columns))
		for _, row := range externalTable.Values {
			err := block.AppendRow(row)
			if err != nil {
				return err
			}
		}
		if err := ch.writeBlock(block, externalTable.Name); err != nil {
			return err
		}
		if err := ch.encoder.Flush(); err != nil {
			return err
		}
		block.Reset()
	}
	return nil
}
