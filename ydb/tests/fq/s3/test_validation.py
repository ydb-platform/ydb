class TestSqlValidation:
    def test_invalid_sql(self, client):
        result = client.execute('SELECT FROM;').check(False)
        assert result.error

    def test_nested_issues(self, client):
        sql = '$t_0 = SELECT 1 - "f" AS data;\n'
        for i in range(100):
            sql += f'$t_{i + 1} = SELECT data FROM $t_{i} WHERE data != {i};\n'
        result = client.execute(sql + 'SELECT data FROM $t_100;').check(False)
        assert 'Cannot substract' in result.error

    def test_nested_type(self, client):
        sql = '$t_0 = SELECT 1 AS data;\n'
        for i in range(300):
            sql += f'$t_{i + 1} = SELECT <|data:data|> AS data FROM $t_{i};\n'
        result = client.execute(sql + 'SELECT data FROM $t_300;').check(False)
        assert 'Nesting depth' in result.error or 'nesting' in result.error.lower()
