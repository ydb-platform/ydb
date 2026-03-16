#!python3

import io
import json
import random
import unittest
import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import csv
import pyarrow.json
import pyarrow.parquet
import chdb

import yatest.common as yc


EXPECTED = """"auxten",9
"jerry",7
"tom",5
"""

EXPECTED_MULTILPE_TABLES = """1,"tom"
"""


SMALL_CSV = """score1,score2,score3
70906,0.9166144356547409,draw
580525,0.9944755780981678,lose
254703,0.5290208413632235,lose
522924,0.9837867058675329,lose
382278,0.4781036385988161,lose
380893,0.48907718034312386,draw
221497,0.32603538643678,draw
446521,0.1386178708257899,win
522220,0.6633602572635723,draw
717410,0.6095994785374601,draw
"""

SCORES_CSV = """score,result,dateOfBirth
758270,lose,1983-07-24
355079,win,2000-11-27
451231,lose,1980-03-11
854953,lose,1996-08-10
294257,lose,1966-12-12
756327,lose,1997-08-29
379755,lose,1981-10-24
916108,lose,1950-08-30
467033,win,2007-09-15
639860,win,1989-06-30
"""

ARROW_DATA_JSONL = """{"match_id": 3943077, "match_date": "2024-07-15", "kick_off": "04:15:00.000", "competition": {"competition_id": 223, "country_name": "South America", "competition_name": "Copa America"}, "season": {"season_id": 282, "season_name": "2024"}, "home_team": {"home_team_id": 779, "home_team_name": "Argentina", "home_team_gender": "male", "home_team_group": null, "country": {"id": 11, "name": "Argentina"}, "managers": [{"id": 5677, "name": "Lionel Sebasti\u00e1n Scaloni", "nickname": null, "dob": "1978-05-16", "country": {"id": 11, "name": "Argentina"}}]}, "away_team": {"away_team_id": 769, "away_team_name": "Colombia", "away_team_gender": "male", "away_team_group": null, "country": {"id": 49, "name": "Colombia"}, "managers": [{"id": 5905, "name": "N\u00e9stor Gabriel Lorenzo", "nickname": null, "dob": "1966-02-28", "country": {"id": 11, "name": "Argentina"}}]}, "home_score": 1, "away_score": 0, "match_status": "available", "match_status_360": "unscheduled", "last_updated": "2024-07-15T15:50:08.671355", "last_updated_360": null, "metadata": {"data_version": "1.1.0", "shot_fidelity_version": "2", "xy_fidelity_version": "2"}, "match_week": 6, "competition_stage": {"id": 26, "name": "Final"}, "stadium": {"id": 5337, "name": "Hard Rock Stadium", "country": {"id": 241, "name": "United States of America"}}, "referee": {"id": 2638, "name": "Raphael Claus", "country": {"id": 31, "name": "Brazil"}}}
{"match_id": 3943076, "match_date": "2024-07-14", "kick_off": "03:00:00.000", "competition": {"competition_id": 223, "country_name": "South America", "competition_name": "Copa America"}, "season": {"season_id": 282, "season_name": "2024"}, "home_team": {"home_team_id": 1833, "home_team_name": "Canada", "home_team_gender": "male", "home_team_group": null, "country": {"id": 40, "name": "Canada"}, "managers": [{"id": 165, "name": "Jesse Marsch", "nickname": null, "dob": "1973-11-08", "country": {"id": 241, "name": "United States of America"}}]}, "away_team": {"away_team_id": 783, "away_team_name": "Uruguay", "away_team_gender": "male", "away_team_group": null, "country": {"id": 242, "name": "Uruguay"}, "managers": [{"id": 269, "name": "Marcelo Alberto Bielsa Caldera", "nickname": "Marcelo Bielsa", "dob": "1955-07-21", "country": {"id": 11, "name": "Argentina"}}]}, "home_score": 2, "away_score": 2, "match_status": "available", "match_status_360": "unscheduled", "last_updated": "2024-07-15T07:57:02.660641", "last_updated_360": null, "metadata": {"data_version": "1.1.0", "shot_fidelity_version": "2", "xy_fidelity_version": "2"}, "match_week": 6, "competition_stage": {"id": 25, "name": "3rd Place Final"}, "stadium": {"id": 52985, "name": "Bank of America Stadium", "country": {"id": 241, "name": "United States of America"}}, "referee": {"id": 1849, "name": "Alexis Herrera", "country": {"id": 246, "name": "Venezuela\u00a0(Bolivarian Republic)"}}}
"""


class myReader(chdb.PyReader):
    def __init__(self, data):
        self.data = data
        self.cursor = 0
        super().__init__(data)

    def read(self, col_names, count):
        print("Python func read", col_names, count, self.cursor)
        if self.cursor >= len(self.data["a"]):
            return []
        block = [self.data[col] for col in col_names]
        self.cursor += len(block[0])
        return block


class TestQueryPy(unittest.TestCase):

    # def test_query_np(self):
    #     t3 = {
    #         "a": np.array([1, 2, 3, 4, 5, 6]),
    #         "b": np.array(["tom", "jerry", "auxten", "tom", "jerry", "auxten"]),
    #     }

    #     ret = chdb.query(
    #         "SELECT b, sum(a) FROM Python(t3) GROUP BY b ORDER BY b", "debug"
    #     )
    #     self.assertEqual(str(ret), EXPECTED)

    def test_query_py(self):
        reader = myReader(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
            }
        )

        ret = chdb.query("SELECT b, sum(a) FROM Python(reader) GROUP BY b ORDER BY b")
        self.assertEqual(str(ret), EXPECTED)

    def test_query_df(self):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
            }
        )

        ret = chdb.query("SELECT b, sum(a) FROM Python(df) GROUP BY b ORDER BY b")
        self.assertEqual(str(ret), EXPECTED)

    def test_query_arrow(self):
        table = pa.table(
            {
                "a": pa.array([1, 2, 3, 4, 5, 6]),
                "b": pa.array(["tom", "jerry", "auxten", "tom", "jerry", "auxten"]),
            }
        )

        ret = chdb.query(
            "SELECT b, sum(a) FROM Python(table) GROUP BY b ORDER BY b"
        )
        self.assertEqual(str(ret), EXPECTED)

    def test_query_arrow2(self):
        t2 = pa.table(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
            }
        )

        ret = chdb.query(
            "SELECT b, sum(a) FROM Python(t2) GROUP BY b ORDER BY b"
        )
        self.assertEqual(str(ret), EXPECTED)

    def test_query_arrow3(self):
        table = csv.read_csv(io.BytesIO(SCORES_CSV.encode()))
        ret = chdb.query(
            """
        SELECT sum(score), avg(score), median(score),
               avgIf(score, dateOfBirth > '1980-01-01') as avgIf,
               countIf(result = 'win') AS wins,
               countIf(result = 'draw') AS draws,
               countIf(result = 'lose') AS losses,
               count()
        FROM Python(table)
        """,
        )
        self.assertEqual(
            str(ret),
            "5872873,587287.3,553446.5,470878.25,3,0,7,10\n",
        )

    def test_query_arrow4(self):
        arrow_table = pa.json.read_json(io.BytesIO(ARROW_DATA_JSONL.encode()))
        # print(arrow_table.schema)
        ret = chdb.query("SELECT * FROM Python(arrow_table) LIMIT 10", "JSONEachRow")
        # print(ret)
        self.assertEqual("", ret.error_message())

    def test_query_arrow5(self):
        arrow_table = pa.parquet.read_table(
            yc.test_source_path("data/sample_2021-04-01_performance_mobile_tiles.parquet")
        )
        # print("Arrow Schema:\n", arrow_table.schema)
        ret = chdb.query("SELECT * FROM Python(arrow_table) LIMIT 1", "JSONCompact")
        # print("JSON:\n", ret)
        schema = json.loads(str(ret)).get("meta")
        # shema is array like:
        # [{"name":"quadkey","type":"String"},{"name":"tile","type":"String"}]
        schema_dict = {x["name"]: x["type"] for x in schema}
        self.assertDictEqual(
            schema_dict,
            {
                "quadkey": "String",
                "tile": "String",
                "tile_x": "Float64",
                "tile_y": "Float64",
                "avg_d_kbps": "Int64",
                "avg_u_kbps": "Int64",
                "avg_lat_ms": "Int64",
                "avg_lat_down_ms": "Float64",
                "avg_lat_up_ms": "Float64",
                "tests": "Int64",
                "devices": "Int64",
            },
        )
        ret = chdb.query(
            """
            WITH numericColumns AS (
            SELECT * EXCEPT ('tile.*') EXCEPT(quadkey)
            FROM Python(arrow_table)
            )
            SELECT * APPLY(max), * APPLY(median) APPLY(x -> round(x, 2))
            FROM numericColumns
            """,
            "JSONCompact",
        )
        # print("JSONCompact:\n", ret)
        self.assertDictEqual(
            {x["name"]: x["type"] for x in json.loads(str(ret)).get("meta")},
            {
                "max(avg_d_kbps)": "Int64",
                "max(avg_lat_down_ms)": "Float64",
                "max(avg_lat_ms)": "Int64",
                "max(avg_lat_up_ms)": "Float64",
                "max(avg_u_kbps)": "Int64",
                "max(devices)": "Int64",
                "max(tests)": "Int64",
                "round(median(avg_d_kbps), 2)": "Float64",
                "round(median(avg_lat_down_ms), 2)": "Float64",
                "round(median(avg_lat_ms), 2)": "Float64",
                "round(median(avg_lat_up_ms), 2)": "Float64",
                "round(median(avg_u_kbps), 2)": "Float64",
                "round(median(devices), 2)": "Float64",
                "round(median(tests), 2)": "Float64",
            },
        )

    def test_random_float(self):
        x = {"col1": [random.uniform(0, 1) for _ in range(0, 100000)]}
        ret = chdb.sql(
            """
        select avg(col1)
        FROM Python(x)
        """
        )
        print(ret.bytes())
        self.assertAlmostEqual(float(ret.bytes()), 0.5, delta=0.01)

    def test_query_dict(self):
        data = {
            "a": [1, 2, 3, 4, 5, 6],
            "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
        }

        ret = chdb.query(
            "SELECT b, sum(a) FROM Python(data) GROUP BY b ORDER BY b"
        )
        self.assertEqual(str(ret), EXPECTED)

    def test_query_dict_int(self):
        data = {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [1, 2, 3, 1, 2, 3],
        }

        ret = chdb.query(
            "SELECT b, sum(a) FROM Python(data) GROUP BY b ORDER BY b"
        )
        self.assertEqual(
            str(ret),
            """1,5
2,7
3,9
""",
            )

    def test_query_pd_csv(self):
        csv_data = pd.read_csv(io.StringIO(SMALL_CSV))
        ret = chdb.query(
            """
            SELECT sum(score1), avg(score1), median(score1),
                sum(toFloat32(score2)), avg(toFloat32(score2)), median(toFloat32(score2)),
                countIf(score3 = 'win') AS wins,
                countIf(score3 = 'draw') AS draws,
                countIf(score3 = 'lose') AS losses,
                count()
            FROM Python(csv_data)
            """,
        )
        self.assertEqual(
            str(ret),
            "4099877,409987.7,414399.5,6.128691345453262,0.6128691345453262,0.5693101584911346,1,5,4,10\n",
        )

    def test_query_multiple_df(self):
        df1 = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
            }
        )

        df2 = pd.DataFrame(
            {
                "a": [7, 8, 9, 10, 11, 12],
                "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
            }
        )

        df3 = pd.DataFrame(
            {
                "a": [13, 14, 15, 16, 17, 18],
                "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
            }
        )

        ret = chdb.query(
            """
            SELECT * FROM python(df1) WHERE a = 1
            UNION ALL
            SELECT * FROM python(df2) WHERE a = 98
            UNION ALL
            SELECT * FROM python(df3) WHERE a = 198
            """)

        self.assertEqual(str(ret), EXPECTED_MULTILPE_TABLES)


if __name__ == "__main__":
    unittest.main(verbosity=3)
