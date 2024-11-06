#include "yql_type_string.h"
#include "yql_expr.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NYql;

Y_UNIT_TEST_SUITE(TTypeString)
{
    void TestFail(const TStringBuf& prog, ui32 column, const TStringBuf& expectedError) {
        TMemoryPool pool(4096);
        TIssues errors;
        auto res = ParseType(prog, pool, errors);
        UNIT_ASSERT(res == nullptr);
        UNIT_ASSERT(!errors.Empty());
        errors.PrintWithProgramTo(Cerr, "-memory-", TString(prog));
        UNIT_ASSERT_STRINGS_EQUAL(errors.begin()->GetMessage(), expectedError);
        UNIT_ASSERT_VALUES_EQUAL(errors.begin()->Position.Column, column);
    }

    void TestOk(const TStringBuf& prog, const TStringBuf& expectedType) {
        TMemoryPool pool(4096);
        TIssues errors;
        auto res = ParseType(prog, pool, errors);
        if (!res) {
            errors.PrintWithProgramTo(Cerr, "-memory-", TString(prog));
            UNIT_FAIL(TStringBuilder() << "Parsing failed:" << Endl << prog);
        }
        UNIT_ASSERT_STRINGS_EQUAL(res->ToString(), expectedType);
    }

    Y_UNIT_TEST(ParseEmpty) {
        TestFail("", 1, "Expected type");
    }

    Y_UNIT_TEST(ParseDataTypes) {
        TestOk("String", "(DataType 'String)");
        TestOk("Bool", "(DataType 'Bool)");
        TestOk("Uint8", "(DataType 'Uint8)");
        TestOk("Int8", "(DataType 'Int8)");
        TestOk("Uint16", "(DataType 'Uint16)");
        TestOk("Int16", "(DataType 'Int16)");
        TestOk("Int32", "(DataType 'Int32)");
        TestOk("Uint32", "(DataType 'Uint32)");
        TestOk("Int64", "(DataType 'Int64)");
        TestOk("Uint64", "(DataType 'Uint64)");
        TestOk("Float", "(DataType 'Float)");
        TestOk("Double", "(DataType 'Double)");
        TestOk("Yson", "(DataType 'Yson)");
        TestOk("Utf8", "(DataType 'Utf8)");
        TestOk("Json", "(DataType 'Json)");
        TestOk("Date", "(DataType 'Date)");
        TestOk("Datetime", "(DataType 'Datetime)");
        TestOk("Timestamp", "(DataType 'Timestamp)");
        TestOk("Interval", "(DataType 'Interval)");
        TestOk("TzDate", "(DataType 'TzDate)");
        TestOk("TzDatetime", "(DataType 'TzDatetime)");
        TestOk("TzTimestamp", "(DataType 'TzTimestamp)");
        TestOk("Uuid", "(DataType 'Uuid)");
        TestOk("Decimal(10,2)", "(DataType 'Decimal '10 '2)");
    }

    Y_UNIT_TEST(Multiline) {
        TestOk(R"(Struct
        <
            name : String
            ,
            age : Uint32
        >)", "(StructType "
                "'('\"age\" (DataType 'Uint32)) "
                "'('\"name\" (DataType 'String)))");
    }

    Y_UNIT_TEST(ParseNoArgsWithStringResult) {
        TestOk("()->String", "(CallableType '() '((DataType 'String)))");
        TestOk("()->Utf8", "(CallableType '() '((DataType 'Utf8)))");
    }

    Y_UNIT_TEST(ParseNoArgsWithOptionalStringResult) {
        TestOk("()->String?",
               "(CallableType '() '((OptionalType (DataType 'String))))");
        TestOk("()->Yson?",
               "(CallableType '() '((OptionalType (DataType 'Yson))))");
    }

    Y_UNIT_TEST(ParseOneArgWithDoubleResult) {
        TestOk("(Int32)->Double",
            "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Int32))"
               ")");
        TestOk("(Yson)->Double",
            "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Yson))"
               ")");
        TestOk("(Utf8)->Double",
            "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Utf8))"
               ")");
    }

    Y_UNIT_TEST(ParseTwoArgsWithOptionalByteResult) {
        TestOk("(Int32?, String)->Uint8?",
               "(CallableType '() '((OptionalType (DataType 'Uint8))) "
                    "'((OptionalType (DataType 'Int32))) "
                    "'((DataType 'String))"
               ")");
    }

    Y_UNIT_TEST(ParseWithEmptyOptionalArgsStringResult) {
        TestOk("([])->String", "(CallableType '() '((DataType 'String)))");
    }

    Y_UNIT_TEST(ParseWithOneOptionalArgDoubleResult) {
        TestOk("([Int32?])->Double",
               "(CallableType '('1) '((DataType 'Double)) "
                    "'((OptionalType (DataType 'Int32)))"
               ")");
    }

    Y_UNIT_TEST(ParseOneReqAndOneOptionalArgsWithDoubleResult) {
        TestOk("(String,[Int32?])->Double",
               "(CallableType '('1) '((DataType 'Double)) "
                    "'((DataType 'String)) "
                    "'((OptionalType (DataType 'Int32)))"
               ")");
    }

    Y_UNIT_TEST(ParseOneReqAndTwoOptionalArgsWithDoubleResult) {
        TestOk("(String,[Int32?, Uint8?])->Double",
               "(CallableType '('2) '((DataType 'Double)) "
                    "'((DataType 'String)) "
                    "'((OptionalType (DataType 'Int32))) "
                    "'((OptionalType (DataType 'Uint8)))"
               ")");
    }

    Y_UNIT_TEST(ParseCallableArgWithDoubleResult) {
        TestOk("(()->Uint8)->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((CallableType '() '((DataType 'Uint8))))"
               ")");
    }

    Y_UNIT_TEST(ParseCallableOptionalArgWithDoubleResult) {
        TestOk("([Optional<()->Uint8>])->Double",
               "(CallableType '('1) '((DataType 'Double)) "
                    "'((OptionalType (CallableType '() '((DataType 'Uint8)))))"
               ")");
    }

    Y_UNIT_TEST(ParseOptionalCallableArgWithDoubleResult) {
        TestOk("(Optional<()->Uint8>)->Double",
               "(CallableType '() '((DataType 'Double)) "
                "'((OptionalType (CallableType '() '((DataType 'Uint8)))))"
               ")");
    }

    Y_UNIT_TEST(ParseCallableWithNamedArgs) {
        TestOk("(a:Uint8)->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Uint8) 'a)"
               ")");
        TestOk("(List:Uint8)->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Uint8) 'List)"
               ")");
        TestOk("('Dict':Uint8)->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Uint8) '\"Dict\")"
               ")");
        TestOk("(a:Uint8,[b:Int32?])->Double",
               "(CallableType '('1) '((DataType 'Double)) "
                    "'((DataType 'Uint8) 'a) "
                    "'((OptionalType (DataType 'Int32)) 'b)"
               ")");
        TestOk("(Uint8,[b:Int32?])->Double",
               "(CallableType '('1) '((DataType 'Double)) "
                    "'((DataType 'Uint8)) "
                    "'((OptionalType (DataType 'Int32)) 'b)"
               ")");
    }

    Y_UNIT_TEST(ParseCallableWithArgFlags) {
        TestOk("(Int32{Flags:AutoMap})->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Int32) '\"\" '1)"
               ")");
        TestOk("(Int32?{Flags:AutoMap})->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((OptionalType (DataType 'Int32)) '\"\" '1)"
               ")");
        TestOk("(x:Int32{Flags:AutoMap})->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Int32) 'x '1)"
               ")");
        TestOk("(x:Int32{Flags:AutoMap}, [y:Uint32?{Flags: AutoMap}])->Double",
               "(CallableType '('1) '((DataType 'Double)) "
                    "'((DataType 'Int32) 'x '1) "
                    "'((OptionalType (DataType 'Uint32)) 'y '1)"
               ")");
        TestOk("(x:Int32{Flags: AutoMap | AutoMap})->Double",
               "(CallableType '() '((DataType 'Double)) "
                    "'((DataType 'Int32) 'x '1)"
               ")");
    }

    Y_UNIT_TEST(ParseCallableWithPayload) {
        TestOk("(Int32)->Double{Payload:MyFunction}",
               "(CallableType '('0 '\"MyFunction\") '((DataType 'Double)) "
                    "'((DataType 'Int32))"
               ")");
    }

    Y_UNIT_TEST(ParseOptional) {
        TestOk("Uint32?", "(OptionalType (DataType 'Uint32))");
        TestOk("Optional<Uint32>", "(OptionalType (DataType 'Uint32))");
        TestOk("Uint32??", "(OptionalType (OptionalType (DataType 'Uint32)))");
        TestOk("Optional<Uint32>?", "(OptionalType (OptionalType (DataType 'Uint32)))");
        TestOk("Optional<Uint32?>", "(OptionalType (OptionalType (DataType 'Uint32)))");
        TestOk("Optional<Optional<Uint32>>", "(OptionalType (OptionalType (DataType 'Uint32)))");
    }

    Y_UNIT_TEST(ParseCallableComplete) {
        TestFail("(Uint32)->", 11, "Expected type");
        TestFail("(,)->", 2, "Expected type or argument name");
        TestFail("(Int32 Int32)->Int32", 8, "Expected ','");
        TestFail("([],)->Uint32", 4, "Expected ')'");
        TestFail("([)->Uint32", 3, "Expected ']'");
        TestFail("(])->Uint32", 2, "Expected ')'");
        TestFail("([,)->Uint32", 3, "Expected type or argument name");
        TestFail("([,])->Uint32", 3, "Expected type or argument name");
        TestFail("(->Uint32", 2, "Expected type or argument name");
        TestFail("([Uint32],Uint8)->Uint32", 9, "Optionals are only allowed in the optional arguments");
        TestFail("([Uint32?],Uint8)->Uint32", 11, "Expected ')'");
        TestFail("Callable<()>", 12, "Expected '->' after arguments");
        TestFail("Callable<()->", 14, "Expected type");
        TestFail("Callable<()->Uint32", 20, "Expected '>'");
        TestFail("(:Uint32)->Uint32", 2, "Expected non empty argument name");
        TestFail("(a:)->Uint32", 4, "Expected type");
        TestFail("(:)->Uint32", 2, "Expected non empty argument name");
        TestFail("(a:Uint32,Uint32)->Uint32", 11, "Expected named argument, because of previous argument(s) was named");
        TestFail("(Uint32{)->Uint32", 9, "Expected Flags field");
        TestFail("(Uint32})->Uint32", 8, "Expected ','");
        TestFail("(Uint32{})->Uint32", 9, "Expected Flags field");
        TestFail("(Uint32{Flags})->Uint32", 14, "Expected ':'");
        TestFail("(Uint32{Flags:})->Uint32", 15, "Expected flag name");
        TestFail("(Uint32{Flags:Map})->Uint32", 15, "Unknown flag name: Map");
        TestFail("(Uint32{Flags:|})->Uint32", 15, "Expected flag name");
        TestFail("(Uint32{Flags:AutoMap|})->Uint32", 23, "Expected flag name");
        TestFail("(Uint32{NonFlags:AutoMap})->Uint32", 9, "Expected Flags field");
        TestFail("(Uint32)->Uint32{", 18, "Expected Payload field");
        TestFail("(Uint32)->Uint32{}", 18, "Expected Payload field");
        TestFail("(Uint32)->Uint32}", 17, "Expected end of string");
        TestFail("(Uint32)->Uint32{Payload}", 25, "Expected ':'");
        TestFail("(Uint32)->Uint32{Payload:}", 26, "Expected payload data");
    }

    Y_UNIT_TEST(ParseCallableWithKeyword) {
        TestOk("(Callable<()->String>) -> Callable<()->Uint32>",
                "(CallableType '() '((CallableType '() '((DataType 'Uint32)))) "
                    "'((CallableType '() '((DataType 'String))))"
               ")");
    }

    Y_UNIT_TEST(ParseListOfDataType) {
        TestOk("(List<String>)->String",
               "(CallableType '() '((DataType 'String)) "
                    "'((ListType (DataType 'String)))"
               ")");
    }

    Y_UNIT_TEST(ParseStreamOfDataType) {
        TestOk("(Stream<String>)->String",
            "(CallableType '() '((DataType 'String)) "
            "'((StreamType (DataType 'String)))"
            ")");
    }

    Y_UNIT_TEST(ParseFlowOfDataType) {
        TestOk("(Flow<String>)->String",
            "(CallableType '() '((DataType 'String)) "
            "'((FlowType (DataType 'String)))"
            ")");
    }

    Y_UNIT_TEST(ParseVariantType) {
        TestOk("Variant<String>",
                "(VariantType (TupleType "
                    "(DataType 'String)"
                "))");
        TestOk("Variant<String, Uint8>",
                "(VariantType (TupleType "
                    "(DataType 'String) "
                    "(DataType 'Uint8)"
                "))");
        TestOk("Variant<Name: String, Age: Int32>",
                "(VariantType (StructType "
                    "'('\"Age\" (DataType 'Int32)) "
                    "'('\"Name\" (DataType 'String))"
                "))");
        TestOk("Variant<'Some Name': String, 'Age': Int32>",
                "(VariantType (StructType "
                    "'('\"Age\" (DataType 'Int32)) "
                    "'('\"Some Name\" (DataType 'String))"
                "))");
    }

    Y_UNIT_TEST(ParseEnumType) {
        TestOk("Enum<Name, Age>",
                "(VariantType (StructType "
                    "'('\"Age\" (VoidType)) "
                    "'('\"Name\" (VoidType))"
                "))");
        TestOk("Enum<'Some Name', 'Age'>",
                "(VariantType (StructType "
                    "'('\"Age\" (VoidType)) "
                    "'('\"Some Name\" (VoidType))"
                "))");
    }

    Y_UNIT_TEST(ParseListAsReturnType) {
        TestOk("(String, String)->List<String>",
               "(CallableType '() '((ListType (DataType 'String))) "
                    "'((DataType 'String)) "
                    "'((DataType 'String))"
               ")");
    }

    Y_UNIT_TEST(ParseListOfOptionalDataType) {
        TestOk("(List<String?>)->String",
               "(CallableType '() '((DataType 'String)) "
                    "'((ListType "
                        "(OptionalType (DataType 'String))"
                    "))"
               ")");
    }

    Y_UNIT_TEST(ParseOptionalListOfDataType) {
        TestOk("(List<String>?)->String",
               "(CallableType '() '((DataType 'String)) "
                    "'((OptionalType "
                        "(ListType (DataType 'String))"
                    "))"
               ")");
    }

    Y_UNIT_TEST(ParseListOfListType) {
        TestOk("(List<List<Uint32>>)->Uint32",
               "(CallableType '() '((DataType 'Uint32)) "
                    "'((ListType "
                        "(ListType (DataType 'Uint32))"
                    "))"
               ")");
    }

    Y_UNIT_TEST(ParseDictOfDataTypes) {
        TestOk("(Dict<String, Uint32>)->Uint32",
               "(CallableType '() '((DataType 'Uint32)) "
                    "'((DictType "
                        "(DataType 'String) "
                        "(DataType 'Uint32)"
                    "))"
               ")");
    }

    Y_UNIT_TEST(ParseSetOfDataTypes) {
        TestOk("(Set<String>)->Uint32",
               "(CallableType '() '((DataType 'Uint32)) "
                    "'((DictType "
                        "(DataType 'String) "
                        "(VoidType)"
                    "))"
               ")");
    }

    Y_UNIT_TEST(ParseListComplete) {
        TestFail("(List<>)->Uint32", 7, "Expected type");
        TestFail("(List<Uint32,>)->Uint32", 13, "Expected '>'");
    }

    Y_UNIT_TEST(ParseVariantComplete) {
        TestFail("Variant<>", 9, "Expected type");
        TestFail("Variant<Uint32,>", 16, "Expected type");
        TestFail("Variant<Uint32", 15, "Expected '>' or ','");

        TestFail("Variant<name:>", 14, "Expected type");
        TestFail("Variant<name:String,>", 21, "Expected struct member name");
        TestFail("Variant<name:String", 20, "Expected '>' or ','");
    }

    Y_UNIT_TEST(ParseDictOfDictTypes) {
        TestOk("(Dict<String, Dict<Uint32, Uint32>>)->Uint32",
               "(CallableType '() '((DataType 'Uint32)) "
                   "'((DictType "
                       "(DataType 'String) "
                       "(DictType (DataType 'Uint32) (DataType 'Uint32))"
                   "))"
               ")");
    }

    Y_UNIT_TEST(ParseDictComplete) {
        TestFail("(Dict<>)->Uint32", 7, "Expected type");
        TestFail("(Dict<Uint32>)->Uint32", 13, "Expected ','");
        TestFail("(Dict<Uint32,>)->Uint32", 14, "Expected type");
        TestFail("(Dict<Uint32, String)->Uint32", 21, "Expected '>'");
    }

    Y_UNIT_TEST(ParseTupleOfDataTypes) {
        TestOk("(Tuple<String, Uint32, Uint8>)->Uint32",
            "(CallableType '() '((DataType 'Uint32)) "
                    "'((TupleType "
                        "(DataType 'String) "
                        "(DataType 'Uint32) "
                        "(DataType 'Uint8)"
                    "))"
                ")");
    }

    Y_UNIT_TEST(ParseTupleComplete) {
        TestFail("(Tuple<Uint32,>)->Uint32", 15, "Expected type");
        TestFail("(Tuple<Uint32)->Uint32", 14, "Expected '>' or ','");
    }

    Y_UNIT_TEST(ParseStructOfDataTypes) {
        TestOk("(Struct<Name: String, Age: Uint32, Male: Bool>)->Uint32",
               "(CallableType '() '((DataType 'Uint32)) "
                   "'((StructType "
                       "'('\"Age\" (DataType 'Uint32)) "
                       "'('\"Male\" (DataType 'Bool)) "
                       "'('\"Name\" (DataType 'String))"
                   "))"
               ")");
    }

    Y_UNIT_TEST(ParseStructWithEscaping) {
        TestOk("Struct<'My\\tName': String, 'My Age': Uint32>",
               "(StructType "
                    "'('\"My\\tName\" (DataType 'String)) "
                    "'('\"My Age\" (DataType 'Uint32))"
               ")");
    }

    Y_UNIT_TEST(ParseStructComplete) {
        TestFail("(Struct<name>)->Uint32", 13, "Expected ':'");
        TestFail("(Struct<name:>)->Uint32", 14, "Expected type");
        TestFail("(Struct<name:String,>)->Uint32", 21, "Expected struct member name");
        TestFail("(Struct<name:String)->Uint32", 20, "Expected '>' or ','");
    }

    Y_UNIT_TEST(ParseResource) {
        TestOk("Resource<aaa>", "(ResourceType 'aaa)");
        TestOk("(Resource<aaa>?)->Resource<bbb>",
               "(CallableType '() '((ResourceType 'bbb)) "
                    "'((OptionalType (ResourceType 'aaa)))"
               ")");
    }

    Y_UNIT_TEST(ParseVoid) {
        TestOk("Void", "(VoidType)");
        TestOk("Void?", "(OptionalType (VoidType))");
        TestOk("(Void?)->Void",
               "(CallableType '() '((VoidType)) "
                    "'((OptionalType (VoidType)))"
               ")");
    }

    Y_UNIT_TEST(ParseNull) {
        TestOk("Null", "(NullType)");
        TestOk("Null?", "(OptionalType (NullType))");
        TestOk("(Null?)->Null",
               "(CallableType '() '((NullType)) "
                    "'((OptionalType (NullType)))"
               ")");
    }

    Y_UNIT_TEST(ParseEmptyList) {
        TestOk("EmptyList", "(EmptyListType)");
        TestOk("EmptyList?", "(OptionalType (EmptyListType))");
        TestOk("(EmptyList?)->EmptyList",
               "(CallableType '() '((EmptyListType)) "
                    "'((OptionalType (EmptyListType)))"
               ")");
    }

    Y_UNIT_TEST(ParseEmptyDict) {
        TestOk("EmptyDict", "(EmptyDictType)");
        TestOk("EmptyDict?", "(OptionalType (EmptyDictType))");
        TestOk("(EmptyDict?)->EmptyDict",
               "(CallableType '() '((EmptyDictType)) "
                    "'((OptionalType (EmptyDictType)))"
               ")");
    }

    Y_UNIT_TEST(UnknownType) {
        TestFail("(Yson2)->String", 2, "Unknown type: 'Yson2'");
        TestFail("()->", 5, "Expected type");
    }

    Y_UNIT_TEST(ParseTagged) {
        TestOk("Tagged<Uint32, IdTag>", "(TaggedType (DataType 'Uint32) 'IdTag)");
    }

    Y_UNIT_TEST(ParseEmptyTuple) {
        TestOk("Tuple<>", "(TupleType)");
    }

    Y_UNIT_TEST(ParseEmptyStruct) {
        TestOk("Struct<>", "(StructType)");
    }

    void TestFormat(const TString& yql, const TString& expectedTypeStr) {
        TMemoryPool pool(4096);

        TAstParseResult astRes = ParseAst(yql, &pool);
        if (!astRes.IsOk()) {
            astRes.Issues.PrintWithProgramTo(Cerr, "-memory-", yql);
            UNIT_FAIL("Can't parse yql");
        }

        TExprContext ctx;
        const TTypeAnnotationNode* type = CompileTypeAnnotation(*astRes.Root->GetChild(0), ctx);
        if (!type) {
            ctx.IssueManager.GetIssues().PrintWithProgramTo(Cerr, "-memory-", yql);
            UNIT_FAIL("Can't compile types");
        }

        TString typeStr = FormatType(type);
        UNIT_ASSERT_STRINGS_EQUAL(typeStr, expectedTypeStr);
    }

    Y_UNIT_TEST(FormatUnit) {
        TestFormat("(Unit)", "Unit");
    }

    Y_UNIT_TEST(FormatTuple) {
        TestFormat("((Tuple "
                   "    (Data Int32) "
                   "    (Data Bool) "
                   "    (Data String)"
                   "))",
                   "Tuple<Int32,Bool,String>");
    }

    Y_UNIT_TEST(FormatDataStruct) {
        TestFormat("((Struct "
                   "    (Item Name (Data String))"
                   "    (Item Age  (Data Uint32))"
                   "    (Item Male (Data Bool))"
                   "))",
                   "Struct<'Age':Uint32,'Male':Bool,'Name':String>");
    }

    Y_UNIT_TEST(FormatDecimal) {
        TestFormat("((Data Decimal 10 3))", "Decimal(10,3)");
    }

    Y_UNIT_TEST(FormatList) {
        TestFormat("((List (Data String)))", "List<String>");
    }

    Y_UNIT_TEST(FormatStream) {
        TestFormat("((Stream (Data String)))", "Stream<String>");
    }

    Y_UNIT_TEST(FormatFlow) {
        TestFormat("((Flow (Data String)))", "Flow<String>");
    }

    Y_UNIT_TEST(FormatBlock) {
        TestFormat("((Block (Data String)))", "Block<String>");
    }

    Y_UNIT_TEST(FormatScalar) {
        TestFormat("((Scalar (Data String)))", "Scalar<String>");
    }

    Y_UNIT_TEST(FormatOptional) {
        TestFormat("((Optional (Data Uint32)))", "Optional<Uint32>");
        TestFormat("((List (Optional (Data Uint32))))", "List<Uint32?>");
    }

    Y_UNIT_TEST(FormatVariant) {
        TestFormat("((Variant (Tuple (Data String))))", "Variant<String>");
    }

    Y_UNIT_TEST(FormatEnum) {
        TestFormat("((Variant (Struct (Item a Void) (Item b Void) )))", "Enum<'a','b'>");
    }

    Y_UNIT_TEST(FormatDict) {
        TestFormat("((Dict "
                   "    (Data String)"
                   "    (Data Uint32)"
                   "))",
                   "Dict<String,Uint32>");
    }

    Y_UNIT_TEST(FormatSet) {
        TestFormat("((Dict "
                   "    (Data String)"
                   "    Void"
                   "))",
                   "Set<String>");
    }

    Y_UNIT_TEST(FormatCallable) {
        TestFormat("((Callable () "
                   "    ((Data String))"
                   "    ((Data Uint32))"
                   "    ((Optional (Data Uint8)))"
                   "))",
                   "Callable<(Uint32,Uint8?)->String>");
        TestFormat("((Callable (1) "
                   "    ((Data String))"
                   "    ((Data Uint32))"
                   "    ((Optional (Data Uint8)))"
                   "))",
                   "Callable<(Uint32,[Uint8?])->String>");
        TestFormat("((Callable (2) "
                   "    ((Data String))"
                   "    ((Optional (Data Uint32)))"
                   "    ((Optional (Data Uint8)))"
                   "))",
                   "Callable<([Uint32?,Uint8?])->String>");
    }

    Y_UNIT_TEST(FormatOptionalCallable) {
        TestFormat("((Optional (Callable () "
                   "    ((Data String))"
                   "    ((Optional (Data Uint8)))"
                   ")))",
                   "Optional<Callable<(Uint8?)->String>>");

        TestFormat("((Optional (Optional (Callable () "
                   "    ((Data String))"
                   "    ((Optional (Data Uint8)))"
                   "))))",
                   "Optional<Optional<Callable<(Uint8?)->String>>>");
    }

    Y_UNIT_TEST(FormatCallableWithNamedArgs) {
        TestFormat("((Callable () "
                   "    ((Data String))"
                   "    ((Data Uint32) x)"
                   "    ((Data Uint8) y)"
                   "))",
                   "Callable<('x':Uint32,'y':Uint8)->String>");
        TestFormat("((Callable () "
                   "    ((Data String))"
                   "    ((Optional (Data Uint8)) a 1)"
                   "))",
                   "Callable<('a':Uint8?{Flags:AutoMap})->String>");
    }

    Y_UNIT_TEST(FormatCallableWithPayload) {
        TestFormat("((Callable (0 MyFunction) "
                   "    ((Data String))"
                   "    ((Optional (Data Uint8)))"
                   "))",
                   "Callable<(Uint8?)->String{Payload:MyFunction}>");
    }

    Y_UNIT_TEST(FormatResource) {
        TestFormat("((Resource aaa))", "Resource<'aaa'>");
        TestFormat("((Resource \"a b\"))", "Resource<'a b'>");
        TestFormat("((Resource \"a\\t\\n\\x01b\"))", "Resource<'a\\t\\n\\x01b'>");
        TestFormat("((Optional (Resource aaa)))", "Optional<Resource<'aaa'>>");
    }

    Y_UNIT_TEST(FormatTagged) {
        TestFormat("((Tagged (Data String) aaa))", "Tagged<String,'aaa'>");
        TestFormat("((Tagged (Data String) \"a b\"))", "Tagged<String,'a b'>");
        TestFormat("((Tagged (Data String) \"a\\t\\n\\x01b\"))", "Tagged<String,'a\\t\\n\\x01b'>");
    }

    Y_UNIT_TEST(FormatPg) {
        TestFormat("((Pg int4))", "pgint4");
        TestFormat("((Pg _int4))", "_pgint4");
    }

    Y_UNIT_TEST(FormatOptionalPg) {
        TestFormat("((Optional (Pg int4)))", "Optional<pgint4>");
        TestFormat("((Optional (Pg _int4)))", "Optional<_pgint4>");
    }
}
