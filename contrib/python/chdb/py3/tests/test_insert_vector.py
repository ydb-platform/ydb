#!/usr/bin/env python3

import time
import unittest
import random
from chdb import session

chs = None


class TestInsertArray(unittest.TestCase):
    def setUp(self) -> None:
        def generate_embedding():
            embedding = [random.uniform(-1, 1) for _ in range(16)]
            return f'"{",".join(str(e) for e in embedding)}"'  # format: "[1.0,2.0,3.0,...]"

        with open("data.csv", "w", encoding="utf-8") as file:
            for movieId in range(1, 100001):
                embedding = generate_embedding()
                line = f"{movieId},{embedding}\n"
                file.write(line)

        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def test_01_insert_array(self):
        global chs
        chs = session.Session()
        chs.query("CREATE DATABASE IF NOT EXISTS movie_embeddings ENGINE = Atomic")
        chs.query("USE movie_embeddings")
        chs.query("DROP TABLE IF EXISTS embeddings")
        chs.query("DROP TABLE IF EXISTS embeddings_with_title")

        chs.query(
            """CREATE TABLE embeddings (
            movieId UInt32 NOT NULL,
            embedding Array(Float32) NOT NULL
        ) ENGINE = MergeTree()
        ORDER BY movieId"""
        )

        print("Inserting movie embeddings into the database")
        t0 = time.time()
        print(chs.query("INSERT INTO embeddings FROM INFILE 'data.csv' FORMAT CSV"))
        rows = chs.query("SELECT count(*) FROM embeddings")
        print(f"Inserted {rows} rows in {time.time() - t0} seconds")

        print("Select result:", chs.query("SELECT * FROM embeddings LIMIT 5"))

    def test_02_query_order_by_cosine_distance(self):
        # You can change the 100 to any movieId you want, but that is just an example
        # If you want to see a real world example, please check the
        # `examples/chDB_vector_search.ipynb`
        # the example is based on the MovieLens dataset and embeddings are generated
        # by the Word2Vec algorithm just extract the movie similarity info from
        # users' movie ratings without any extra data.
        global chs
        topN = chs.query(
            """
                  WITH
                    100 AS theMovieId,
                    (SELECT embedding FROM embeddings WHERE movieId = theMovieId LIMIT 1) AS targetEmbedding
                  SELECT
                    movieId,
                    cosineDistance(embedding, targetEmbedding) AS distance
                    FROM embeddings
                  WHERE movieId != theMovieId
                    ORDER BY distance ASC
                    LIMIT 5
                  """
        )
        print(
            f"Scaned {topN.rows_read()} rows, "
            f"Top 5 similar movies to movieId 100 in {topN.elapsed()}"
        )
        print(topN)

    def test_03_close_session(self):
        global chs
        chs.close()
        self.assertEqual(chs._conn, None)


if __name__ == "__main__":
    unittest.main()
