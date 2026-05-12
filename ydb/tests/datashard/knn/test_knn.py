from ydb.tests.datashard.lib.vector_base import VectorBase


class TestKnn(VectorBase):
    table_name = "TestTable"
    target_table_name = "TargetVectors"
    # Target embedding: 0x67, 0x71 (103, 113)
    target_embedding_hex = "677102"

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls._create_tables()

    @classmethod
    def teardown_class(cls):
        cls._cleanup_tables()
        super().teardown_class()

    @classmethod
    def _cleanup_tables(cls):
        try:
            cls.pool.execute_with_retries(f"DROP TABLE `{cls.table_name}`")
        except Exception:
            pass
        try:
            cls.pool.execute_with_retries(f"DROP TABLE `{cls.target_table_name}`")
        except Exception:
            pass

    @classmethod
    def _create_tables(cls):
        cls._cleanup_tables()

        cls.pool.execute_with_retries(f"""
            CREATE TABLE `{cls.table_name}` (
                pk Int64 NOT NULL,
                emb String NOT NULL,
                data String NOT NULL,
                PRIMARY KEY (pk)
            );
        """)

        cls.pool.execute_with_retries(f"""
            UPSERT INTO `{cls.table_name}` (pk, emb, data) VALUES
            (0, Unwrap(String::HexDecode("033002")), "0"),
            (1, Unwrap(String::HexDecode("133102")), "1"),
            (2, Unwrap(String::HexDecode("233202")), "2"),
            (3, Unwrap(String::HexDecode("533302")), "3"),
            (4, Unwrap(String::HexDecode("433402")), "4"),
            (5, Unwrap(String::HexDecode("506002")), "5"),
            (6, Unwrap(String::HexDecode("611102")), "6"),
            (7, Unwrap(String::HexDecode("126202")), "7"),
            (8, Unwrap(String::HexDecode("757602")), "8"),
            (9, Unwrap(String::HexDecode("767602")), "9");
        """)

        cls.pool.execute_with_retries(f"""
            CREATE TABLE `{cls.target_table_name}` (
                id Int64 NOT NULL,
                target_emb String,
                PRIMARY KEY (id)
            )
        """)

        cls.pool.execute_with_retries(f"""
            UPSERT INTO `{cls.target_table_name}` (id, target_emb) VALUES
            (1, String::HexDecode("{cls.target_embedding_hex}"))
        """)

    def test_knn_cosine_distance(self):
        """Test KNN with CosineDistance."""
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");
            SELECT pk FROM `{self.table_name}`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        """)
        assert len(result) == 3

    def test_knn_cosine_similarity(self):
        """Test KNN with CosineSimilarity (DESC order)."""
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");
            SELECT pk FROM `{self.table_name}`
            ORDER BY Knn::CosineSimilarity(emb, $TargetEmbedding) DESC
            LIMIT 3
        """)
        assert len(result) == 3

    def test_knn_inner_product_similarity(self):
        """Test KNN with InnerProductSimilarity (DESC order)."""
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");
            SELECT pk FROM `{self.table_name}`
            ORDER BY Knn::InnerProductSimilarity(emb, $TargetEmbedding) DESC
            LIMIT 3
        """)
        assert len(result) == 3

    def test_knn_manhattan_distance(self):
        """Test KNN with ManhattanDistance."""
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");
            SELECT pk FROM `{self.table_name}`
            ORDER BY Knn::ManhattanDistance(emb, $TargetEmbedding)
            LIMIT 3
        """)
        assert len(result) == 3

    def test_knn_euclidean_distance(self):
        """Test KNN with EuclideanDistance."""
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");
            SELECT pk FROM `{self.table_name}`
            ORDER BY Knn::EuclideanDistance(emb, $TargetEmbedding)
            LIMIT 3
        """)
        assert len(result) == 3

    def test_knn_verify_results(self):
        """
        Verify actual results - check that top 3 PKs are correct.
        Target vector is 0x67, 0x71 (103, 113)
        Expected cosine distances:
            pk=8 (117, 118): 0.000882 - closest
            pk=5 (80, 96):   0.000985
            pk=9 (118, 118): 0.001070
        """
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");
            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance
            FROM `{self.table_name}`
            ORDER BY distance
            LIMIT 3
        """)

        assert len(result) == 3
        pks = [row['pk'] for row in result]
        assert pks == [8, 5, 9], f"Expected PKs [8, 5, 9], got {pks}"

    def test_knn_two_stage_query(self):
        """
        Test two-stage query: quantized search followed by full precision reranking.
        """
        result = self.query(f"""
            $TargetEmbedding = String::HexDecode("{self.target_embedding_hex}");

            $Pks = SELECT pk
            FROM `{self.table_name}`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3;

            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance
            FROM `{self.table_name}`
            WHERE pk IN $Pks
            ORDER BY distance
            LIMIT 3;
        """)
        assert len(result) == 3

    def test_knn_subquery_target(self):
        """Test KNN with target vector from subquery."""
        result = self.query(f"""
            $TargetEmbedding = (SELECT target_emb FROM `{self.target_table_name}` WHERE id = 1);
            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance
            FROM `{self.table_name}`
            ORDER BY distance
            LIMIT 3
        """)
        assert len(result) == 3
