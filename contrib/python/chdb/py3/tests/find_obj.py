import gc
import time
import pandas
import duckdb
import chdb


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


def test():
    reader = myReader(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
        }
    )

    all_obj = gc.get_objects()
    for obj in all_obj:
        if hasattr(obj, "__name__") and obj.__name__ == "reader":
            print(obj)

    for obj in all_obj:
        try:
            if hasattr(obj, "__name__"):
                print(obj.__name__)
            print(obj.__class__, obj)
        except:
            pass


def duck_test():
    df_old = pandas.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": ["tom", "jerry", "auxten", "tom", "jerry", "auxten"],
        }
    )

    df_old1 = myReader("aaa")
    t = time.time()
    # query the old dataframe with duckdb
    con = duckdb.connect()
    # con.register("hits", df_old)
    ret = con.execute(
        """
    SELECT * FROM df_old1;
    """
    ).fetchdf()
    print(ret)
    print("Run duckdb on df_old. Time cost:", time.time() - t, "s")


duck_test()
# test()
