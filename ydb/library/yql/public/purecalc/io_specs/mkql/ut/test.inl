Y_UNIT_TEST_SUITE(TEST_SUITE_NAME) {
    using NYql::NPureCalc::NPrivate::GetSchema;

    Y_UNIT_TEST(TestAllTypes) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields {"int64", "uint64", "double", "bool", "string", "yson"};
        auto schema = GetSchema(fields);
        auto stream = GET_STREAM(fields);

        auto factory = MakeProgramFactory();

        {
            auto program = CREATE_PROGRAM(
                INPUT_SPEC {schema},
                OUTPUT_SPEC {schema},
                "SELECT * FROM Input",
                ETranslationMode::SQL, 1
            );

            auto input = TStringStream(stream);
            auto handle = program->Apply(&input);
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(stream, output);
        }

        // invalid table prefix
        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            CREATE_PROGRAM(
                INPUT_SPEC {schema},
                OUTPUT_SPEC {schema},
                "SELECT * FROM Table",
                ETranslationMode::SQL, 1
            );
        }(), TCompileError, "Failed to optimize");

        // invalid table suffix (input index)
        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            CREATE_PROGRAM(
                INPUT_SPEC {schema},
                OUTPUT_SPEC {schema},
                "SELECT * FROM Input1",
                ETranslationMode::SQL, 1
            );
        }(), TCompileError, "Failed to optimize");
    }

    Y_UNIT_TEST(TestColumnsFilter) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields {"int64", "uint64", "double", "bool", "string", "yson"};
        auto schema = GetSchema(fields);
        auto stream = GET_STREAM(fields);

        TVector<TString> someFields {"int64", "bool", "string"};
        auto someSchema = GetSchema(someFields);
        auto someStream = GET_STREAM(someFields);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {schema};
            auto outputSpec = OUTPUT_SPEC {someSchema};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                "SELECT int64, bool, string FROM Input",
                ETranslationMode::SQL, 1
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(),
                THashSet<TString>(someFields.begin(), someFields.end())
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(0),
                program->GetUsedColumns()
            );

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->GetUsedColumns(1);
            }()), yexception, "invalid input index (1) in GetUsedColumns call");

            auto input = TStringStream(stream);
            auto handle = program->Apply(&input);
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(someStream, output);

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TVector<IOutputStream*>({});
                program->Apply(&input)->Run(outputs);
            }()), yexception, "cannot be used with single-output programs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TVector<IOutputStream*>({&output});
                program->Apply(&input)->Run(outputs);
            }()), yexception, "cannot be used with single-output programs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TMap<TString, IOutputStream*>();
                program->Apply(&input)->Run(outputs);
            }()), yexception, "cannot be used with single-output programs");
        }
    }

#ifdef PULL_LIST_MODE
    Y_UNIT_TEST(TestColumnsFilterMultiInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields0 {"int64", "uint64", "double"};
        auto schema0 = GetSchema(fields0);
        TVector<TString> someFields0 {"int64", "uint64"};

        TVector<TString> fields1 {"bool", "string", "yson"};
        auto schema1 = GetSchema(fields1);
        TVector<TString> someFields1 {"bool", "yson"};

        TVector<TString> unitedFields {"int64", "uint64", "bool", "yson"};
        auto unitedSchema = GetSchema(unitedFields, unitedFields);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {{schema0, schema1}};
            auto outputSpec = OUTPUT_SPEC {unitedSchema};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
SELECT int64, uint64 FROM Input0
UNION ALL
SELECT bool, yson FROM Input1
                )",
                ETranslationMode::SQL, 1
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(0),
                THashSet<TString>(someFields0.begin(), someFields0.end())
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(1),
                THashSet<TString>(someFields1.begin(), someFields1.end())
            );

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->GetUsedColumns();
            }()), yexception, "GetUsedColumns() can be used only with single-input programs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->GetUsedColumns(2);
            }()), yexception, "invalid input index (2) in GetUsedColumns call");
        }
    }
#endif

    Y_UNIT_TEST(TestColumnsFilterWithOptionalFields) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields {"int64", "uint64", "double", "bool", "string", "yson"};
        auto schema = GetSchema(fields);
        auto stream = GET_STREAM(fields);

        TVector<TString> someFields {"int64", "bool", "string"};
        TVector<TString> someOptionalFields {"string"};

        auto someSchema = GetSchema(someFields);
        auto someStream = GET_STREAM(someFields, someOptionalFields);
        auto someOptionalSchema = GetSchema(someFields, someOptionalFields);

        auto factory = MakeProgramFactory();

        {
            auto program = CREATE_PROGRAM(
                INPUT_SPEC {schema},
            OUTPUT_SPEC {someOptionalSchema},
            "SELECT int64, bool, Nothing(String?) as string FROM Input",
            ETranslationMode::SQL, 1
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(),
                THashSet<TString>({"int64", "bool"})
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(),
                program->GetUsedColumns(0)
            );

            auto input = TStringStream(stream);
            auto handle = program->Apply(&input);
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(someStream, output);
        }

        UNIT_ASSERT_EXCEPTION_CONTAINS([&](){
            CREATE_PROGRAM(
                INPUT_SPEC {schema},
            OUTPUT_SPEC {someSchema},
            "SELECT int64, bool, Nothing(String?) as string FROM Input",
            ETranslationMode::SQL, 1
            );
        }(), TCompileError, "Failed to optimize");
    }

    Y_UNIT_TEST(TestOutputSpecInference) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields {"int64", "uint64", "double", "bool", "string"};
        auto schema = GetSchema(fields);
        auto stream = GET_STREAM(fields);

        TVector<TString> someFields {"bool", "int64", "string"};  // Keep this sorted...
        auto someSchema = GetSchema(someFields);
        auto someStream = GET_STREAM(someFields);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {schema};
            auto outputSpec = OUTPUT_SPEC {NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                "SELECT int64, bool, string FROM Input",
                ETranslationMode::SQL, 1
            );

            UNIT_ASSERT_EQUAL(program->MakeFullOutputSchema(), someSchema);

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(),
                THashSet<TString>(someFields.begin(), someFields.end())
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(),
                program->GetUsedColumns(0)
            );

            auto input = TStringStream(stream);
            auto handle = program->Apply(&input);
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(someStream, output);
        }
    }

#ifdef PULL_LIST_MODE
    Y_UNIT_TEST(TestJoinInputs) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields0 {"int64", "uint64", "double"};
        auto schema0 = GetSchema(fields0);
        auto stream0 = GET_STREAM(fields0);

        TVector<TString> fields1 {"int64", "bool", "string"};
        auto schema1 = GetSchema(fields1);
        auto stream1 = GET_STREAM(fields1);

        TVector<TString> joinedFields {"bool", "double", "int64", "string", "uint64"};  // keep this sorted
        auto joinedSchema = GetSchema(joinedFields);
        auto joinedStream = GET_STREAM(joinedFields);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {{schema0, schema1}};
            auto outputSpec = OUTPUT_SPEC {NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
SELECT
    t0.int64 AS int64,
    t0.uint64 AS uint64,
    t0.double AS double,
    t1.bool AS bool,
    t1.string AS string
FROM
    Input0 AS t0
INNER JOIN
    Input1 AS t1
ON t0.int64 == t1.int64
ORDER BY int64
                )",
                ETranslationMode::SQL, 1
            );

            UNIT_ASSERT_EQUAL(program->MakeFullOutputSchema(), joinedSchema);

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(0),
                THashSet<TString>(fields0.begin(), fields0.end())
            );

            UNIT_ASSERT_VALUES_EQUAL(
                program->GetUsedColumns(1),
                THashSet<TString>(fields1.begin(), fields1.end())
            );

            TStringStream input0(stream0);
            TStringStream input1(stream1);
            auto handle = program->Apply<TVector<IInputStream*>>({&input0, &input1});
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(joinedStream, output);
        }
    }
#endif

    Y_UNIT_TEST(TestMultiOutputOverTuple) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields {"int64", "uint64", "double", "bool", "string"};
        auto schema = GetSchema(fields);
        auto stream = GET_STREAM(fields, {}, 0, 10, 1);

        TVector<TString> someFields1 {"bool", "int64", "string"};
        auto someSchema1 = GetSchema(someFields1);
        auto someStream1 = GET_STREAM(someFields1, {}, 0, 10, 2);

        TVector<TString> someFields2 {"bool", "double"};
        auto someSchema2 = GetSchema(someFields2);
        auto someStream2 = GET_STREAM(someFields2, {}, 1, 10, 2);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {schema};
            auto outputSpec = OUTPUT_SPEC {NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
(
    (let vt (ParseType '"Variant<Struct<bool:Bool, int64:Int64, string:String>, Struct<bool:Bool, double:Double>>"))
    (return (Map (Self '0) (lambda '(x) (block '(
        (let r1 (Variant (AsStruct '('bool (Member x 'bool)) '('int64 (Member x 'int64)) '('string (Member x 'string))) '0 vt))
        (let r2 (Variant (AsStruct '('bool (Member x 'bool)) '('double (Member x 'double))) '1 vt))
        (return (If (Coalesce (== (% (Member x 'int64) (Int64 '2)) (Int64 '0)) (Bool 'false)) r1 r2))
    )))))
)
                )",
                ETranslationMode::SExpr
            );

            auto input = TStringStream(stream);
            auto handle = program->Apply(&input);
            TStringStream output1, output2;
            auto outputs = TVector<IOutputStream*>({&output1, &output2});
            handle->Run(outputs);
            ASSERT_EQUAL_STREAMS(someStream1, output1);
            ASSERT_EQUAL_STREAMS(someStream2, output2);

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                program->Apply(&input)->Run(&output1);
            }()), yexception, "cannot be used with multi-output programs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TVector<IOutputStream*>({});
                program->Apply(&input)->Run(outputs);
            }()), yexception, "Number of variant alternatives should match number of streams");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TVector<IOutputStream*>({&output1, &output1, &output1});
                program->Apply(&input)->Run(outputs);
            }()), yexception, "Number of variant alternatives should match number of streams");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TMap<TString, IOutputStream*>();
                program->Apply(&input)->Run(outputs);
            }()), yexception, "cannot be used to process variants over tuple");
        }
    }

    Y_UNIT_TEST(TestMultiOutputOverStruct) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields {"int64", "uint64", "double", "bool", "string"};
        auto schema = GetSchema(fields);
        auto stream = GET_STREAM(fields, {}, 0, 10, 1);

        TVector<TString> someFields1 {"bool", "int64", "string"};
        auto someSchema1 = GetSchema(someFields1);
        auto someStream1 = GET_STREAM(someFields1, {}, 0, 10, 2);

        TVector<TString> someFields2 {"bool", "double"};
        auto someSchema2 = GetSchema(someFields2);
        auto someStream2 = GET_STREAM(someFields2, {}, 1, 10, 2);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {schema};
            auto outputSpec = OUTPUT_SPEC {NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
(
    (let vt (ParseType '"Variant<A2:Struct<bool:Bool, double:Double>, A1:Struct<bool:Bool, int64:Int64, string:String>>"))
    (return (Map (Self '0) (lambda '(x) (block '(
        (let r1 (Variant (AsStruct '('bool (Member x 'bool)) '('int64 (Member x 'int64)) '('string (Member x 'string))) 'A1 vt))
        (let r2 (Variant (AsStruct '('bool (Member x 'bool)) '('double (Member x 'double))) 'A2 vt))
        (return (If (Coalesce (== (% (Member x 'int64) (Int64 '2)) (Int64 '0)) (Bool 'false)) r1 r2))
    )))))
)
                )",
                ETranslationMode::SExpr
            );

            auto input = TStringStream(stream);
            auto handle = program->Apply(&input);
            TStringStream output1, output2;
            auto outputs = TMap<TString, IOutputStream*>();
            outputs["A1"] = &output1;
            outputs["A2"] = &output2;
            handle->Run(outputs);
            ASSERT_EQUAL_STREAMS(someStream1, output1);
            ASSERT_EQUAL_STREAMS(someStream2, output2);

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                program->Apply(&input)->Run(&output1);
            }()), yexception, "cannot be used with multi-output programs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TVector<IOutputStream*>({});
                program->Apply(&input)->Run(outputs);
            }()), yexception, "cannot be used to process variants over struct");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TMap<TString, IOutputStream*>();
                outputs["A1"] = &output1;
                program->Apply(&input)->Run(outputs);
            }()), yexception, "Number of variant alternatives should match number of streams");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TMap<TString, IOutputStream*>();
                outputs["A1"] = &output1;
                outputs["A2"] = &output1;
                outputs["A3"] = &output1;
                program->Apply(&input)->Run(outputs);
            }()), yexception, "Number of variant alternatives should match number of streams");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto outputs = TMap<TString, IOutputStream*>();
                outputs["A1"] = &output1;
                outputs["B1"] = &output1;
                program->Apply(&input)->Run(outputs);
            }()), yexception, "Cannot find stream for alternative \"A2\"");
        }
    }

#ifdef GET_STREAM_WITH_STRUCT
    Y_UNIT_TEST(TestReadNativeStructs) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory(
            TProgramFactoryOptions().SetNativeYtTypeFlags(NYql::NTCF_PRODUCTION)
        );

        auto runProgram = [&factory](bool sorted) -> TStringStream {
            auto inputSchema = GET_SCHEMA_WITH_STRUCT(sorted);
            
            auto input0 = GET_STREAM_WITH_STRUCT(sorted, 0, 2);
            auto input1 = GET_STREAM_WITH_STRUCT(sorted, 2, 4);
            
            auto inputSpec = INPUT_SPEC{{inputSchema, inputSchema}}.SetUseOriginalRowSpec(!sorted);
            auto outputSpec = OUTPUT_SPEC{NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
(
    (return (Extend (Self '0) (Self '1)))
)
                )",
                ETranslationMode::SExpr
            );
            
            TStringStream result;

            auto handle = program->Apply(TVector<IInputStream*>({&input0, &input1}));
            handle->Run(&result);

            return result;
        };

        auto etalon = GET_STREAM_WITH_STRUCT(true, 0, 4);

        auto output0 = runProgram(true);
        auto output1 = runProgram(false);

        ASSERT_EQUAL_STREAMS(output0, etalon);
        ASSERT_EQUAL_STREAMS(output1, etalon);
    }
#endif

    Y_UNIT_TEST(TestIndependentProcessings) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields0 {"double", "int64", "string"};  // keep this sorted
        auto schema0 = GetSchema(fields0);
        auto stream0 = GET_STREAM(fields0, {}, 0, 10, 1);

        TVector<TString> someFields0 {"int64", "string"};
        auto someStream0 = GET_STREAM(someFields0, {}, 0, 10, 2);  // sample with even int64 numbers

        TVector<TString> fields1 {"bool", "int64", "uint64"}; // keep this sorted
        auto schema1 = GetSchema(fields1);
        auto stream1 = GET_STREAM(fields1, {}, 0, 10, 1);

        TVector<TString> someFields1 {"int64", "uint64"};
        auto someStream1 = GET_STREAM(someFields1, {}, 1, 10, 2);  // sample with odd int64 numbers

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {{schema0, schema1}};
            auto outputSpec = OUTPUT_SPEC {NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
(
    (let $type (ParseType '"Variant<Struct<int64: Int64, string:String>, Struct<int64:Int64, uint64: Uint64>>"))
    (let $stream0 (FlatMap (Self '0) (lambda '(x) (block '(
        (let $item (Variant (AsStruct '('int64 (Member x 'int64)) '('string (Member x 'string))) '0 $type))
        (return (ListIf (Coalesce (== (% (Member x 'int64) (Int64 '2)) (Int64 '0)) (Bool 'false)) $item))
    )))))
    (let $stream1 (FlatMap (Self '1) (lambda '(x) (block '(
        (let $item (Variant (AsStruct '('int64 (Member x 'int64)) '('uint64 (Member x 'uint64))) '1 $type))
        (return (ListIf (Coalesce (== (% (Member x 'int64) (Int64 '2)) (Int64 '1)) (Bool 'false)) $item))
    )))))
    (return (Extend $stream0 $stream1))
)
                )",
                ETranslationMode::SExpr
            );

            UNIT_ASSERT_EQUAL(program->MakeInputSchema(0), schema0);
            UNIT_ASSERT_EQUAL(program->MakeInputSchema(1), schema1);

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->MakeInputSchema(2);
            }()), yexception, "invalid input index (2) in MakeInputSchema call");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->MakeInputSchema();
            }()), yexception, "MakeInputSchema() can be used only with single-input programs");

            TStringStream input0(stream0);
            TStringStream input1(stream1);
            auto handle = program->Apply(TVector<IInputStream*>({&input0, &input1}));
            TStringStream output0, output1;
            handle->Run(TVector<IOutputStream*>({&output0, &output1}));

            ASSERT_EQUAL_STREAMS(someStream0, output0);
            ASSERT_EQUAL_STREAMS(someStream1, output1);

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->Apply(TVector<IInputStream*>());
            }()), yexception, "number of input streams should match number of inputs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->Apply(TVector<IInputStream*>({&input0}));
            }()), yexception, "number of input streams should match number of inputs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                TStringStream input2;
                auto unused = program->Apply(TVector<IInputStream*>({&input0, &input1, &input2}));
            }()), yexception, "number of input streams should match number of inputs");

            UNIT_ASSERT_EXCEPTION_CONTAINS(([&](){
                auto unused = program->Apply(&input0);
            }()), yexception, "number of input streams should match number of inputs");
        }
    }

    Y_UNIT_TEST(TestMergeInputs) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields0 {"double", "int64", "string", "uint64"};  // keep this sorted
        auto schema0 = GetSchema(fields0);
        auto stream0 = GET_STREAM(fields0, {}, 0, 5, 1);

        TVector<TString> fields1 {"double", "int64", "uint64", "yson"};  // keep this sorted
        auto schema1 = GetSchema(fields1);
        auto stream1 = GET_STREAM(fields1, {}, 5, 10, 1);

        TVector<TString> someFields {"double", "int64", "uint64"};  // keep this sorted
        auto mergedStream = GET_STREAM(someFields, {}, 0, 10, 1);
        auto mergedSchema = GetSchema(someFields);

        auto factory = MakeProgramFactory();

        {
            auto inputSpec = INPUT_SPEC {{schema0, schema1}};
            auto outputSpec = OUTPUT_SPEC {NYT::TNode::CreateEntity()};

            auto program = CREATE_PROGRAM(
                inputSpec,
                outputSpec,
                R"(
(
    (let $stream0 (Map (Self '0) (lambda '(x) (RemoveMember x 'string))))
    (let $stream1 (Map (Self '1) (lambda '(x) (RemoveMember x 'yson))))
    (return (Extend $stream0 $stream1))
)
                )",
                ETranslationMode::SExpr
            );

            UNIT_ASSERT_EQUAL(program->MakeInputSchema(0), schema0);
            UNIT_ASSERT_EQUAL(program->MakeInputSchema(1), schema1);
            UNIT_ASSERT_EQUAL(program->MakeFullOutputSchema(), mergedSchema);

            TStringStream input0(stream0);
            TStringStream input1(stream1);
            auto handle = program->Apply(TVector<IInputStream*>({&input0, &input1}));
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(mergedStream, output);
        }
    }

    Y_UNIT_TEST(TestTableName) {
        using namespace NYql::NPureCalc;

        TVector<TVector<int>> values = {{3, 5}};

        auto inputSchema = GetSchema({"int64"});
        auto stream = GET_MULTITABLE_STREAM(values);
        auto etalon = GET_MULTITABLE_STREAM(values, {"Input"});

        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseSystemColumns(true));

        {
            auto program = CREATE_PROGRAM(
                INPUT_SPEC(inputSchema),
                OUTPUT_SPEC(NYT::TNode::CreateEntity()),
                "SELECT int64, TableName() AS tname FROM Input",
                ETranslationMode::SQL
            );
            
            auto handle = program->Apply(&stream);
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(output, etalon);
        }
    }

    Y_UNIT_TEST(TestCustomTableName) {
        using namespace NYql::NPureCalc;

        TVector<TVector<int>> values = {{3, 5}, {2, 8}};
        TVector<TString> tableNames = {"One", "Two"};

        auto inputSchema = GetSchema({"int64"});
        auto stream = GET_MULTITABLE_STREAM(values);
        auto etalon = GET_MULTITABLE_STREAM(values, tableNames);

        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseSystemColumns(true));

        {
            auto program = CREATE_PROGRAM(
                INPUT_SPEC(inputSchema).SetTableNames(tableNames),
                OUTPUT_SPEC(NYT::TNode::CreateEntity()),
                "SELECT int64, TableName() AS tname FROM TABLES()",
                ETranslationMode::SQL
            );
            
            auto handle = program->Apply(&stream);
            TStringStream output;
            handle->Run(&output);

            ASSERT_EQUAL_STREAMS(output, etalon);
        }
    }

#ifdef PULL_LIST_MODE
    Y_UNIT_TEST(TestMultiinputTableName) {
        using namespace NYql::NPureCalc;

        TVector<TVector<int>> values0 = {{3, 5}};
        TVector<TVector<int>> values1 = {{7, 9}};

        auto inputSchema = GetSchema({"int64"});
        auto stream0 = GET_MULTITABLE_STREAM(values0);
        auto stream1 = GET_MULTITABLE_STREAM(values1);
        auto etalon = GET_MULTITABLE_STREAM(JoinVectors(values0, values1), {"Input0", "Input1"});
        
        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseSystemColumns(true));

        {
            auto program = CREATE_PROGRAM(
                INPUT_SPEC({inputSchema, inputSchema}),
                OUTPUT_SPEC(NYT::TNode::CreateEntity()),
                R"(
$union = (
    SELECT * FROM Input0
    UNION ALL
    SELECT * FROM Input1
);
SELECT TableName() AS tname, int64 FROM $union
                )"
            );
            
            auto handle = program->Apply(TVector<IInputStream*>{&stream0, &stream1});
            TStringStream output;
            handle->Run(&output);
            
            ASSERT_EQUAL_STREAMS(output, etalon);
        }
    }

    Y_UNIT_TEST(TestMultiinputCustomTableName) {
        using namespace NYql::NPureCalc;

        TVector<TVector<int>> values0 = {{1, 4}, {2, 8}};
        TVector<TVector<int>> values1 = {{3, 5}, {7, 9}};
        TVector<TString> tableNames0 = {"OneA", "TwoA"};
        TVector<TString> tableNames1 = {"OneB", "TwoB"};

        auto inputSchema = GetSchema({"int64"});
        auto stream0 = GET_MULTITABLE_STREAM(values0);
        auto stream1 = GET_MULTITABLE_STREAM(values1);
        auto etalon = GET_MULTITABLE_STREAM(JoinVectors(values0, values1), JoinVectors(tableNames0, tableNames1));
        
        auto factory = MakeProgramFactory(TProgramFactoryOptions().SetUseSystemColumns(true));

        {
            auto program = CREATE_PROGRAM(
                INPUT_SPEC({inputSchema, inputSchema}).SetTableNames(tableNames0, 0).SetTableNames(tableNames1, 1),
                OUTPUT_SPEC(NYT::TNode::CreateEntity()),
                R"(
$input0, $input1 = PROCESS TABLES();
$union = (
    SELECT * FROM $input0
    UNION ALL
    SELECT * FROM $input1
);
SELECT TableName() AS tname, int64 FROM $union
                )"
            );
            
            auto handle = program->Apply(TVector<IInputStream*>{&stream0, &stream1});
            TStringStream output;
            handle->Run(&output);
            
            ASSERT_EQUAL_STREAMS(output, etalon);
        }
    }
#endif
}
