#include <ydb/core/formats/arrow/printer/printer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>


Y_UNIT_TEST_SUITE(Printer) {
    using namespace NKikimr::NArrow;

    Y_UNIT_TEST(SSAToPrettyString) {
        NKikimrSSA::TProgram program;
        TString programTxt = R"(
            Command {
              Assign {
                Column {
                  Id: 14
                }
                Constant {
                  Text: "$.\"client_xid\""
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 15
                }
                Function {
                  Arguments {
                    Id: 10
                  }
                  Arguments {
                    Id: 14
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 0
                  KernelName: "JsonValue"
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 16
                }
                Constant {
                  Bytes: "52175"
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 17
                }
                Function {
                  Arguments {
                    Id: 15
                  }
                  Arguments {
                    Id: 16
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 1
                  YqlOperationId: 11
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 18
                }
                Constant {
                  Uint8: 0
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 19
                }
                Function {
                  Arguments {
                    Id: 17
                  }
                  Arguments {
                    Id: 18
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 2
                  YqlOperationId: 17
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 20
                }
                Constant {
                  Text: "$.\"try_index\""
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 21
                }
                Function {
                  Arguments {
                    Id: 10
                  }
                  Arguments {
                    Id: 20
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 3
                  KernelName: "JsonExists"
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 22
                }
                Function {
                  Id: 23
                  Arguments {
                    Id: 21
                  }
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 23
                }
                Constant {
                  Uint8: 0
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 24
                }
                Function {
                  Arguments {
                    Id: 22
                  }
                  Arguments {
                    Id: 23
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 4
                  YqlOperationId: 17
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 25
                }
                Constant {
                  Text: "$.\"items\""
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 26
                }
                Function {
                  Arguments {
                    Id: 10
                  }
                  Arguments {
                    Id: 25
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 5
                  KernelName: "JsonValue"
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 27
                }
                Constant {
                  Bytes: "AAAA/"
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 28
                }
                Function {
                  Arguments {
                    Id: 26
                  }
                  Arguments {
                    Id: 27
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 6
                  YqlOperationId: 10
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 29
                }
                Constant {
                  Uint8: 0
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 30
                }
                Function {
                  Arguments {
                    Id: 28
                  }
                  Arguments {
                    Id: 29
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 7
                  YqlOperationId: 17
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 31
                }
                Function {
                  Arguments {
                    Id: 24
                  }
                  Arguments {
                    Id: 30
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 8
                  YqlOperationId: 0
                }
              }
            }
            Command {
              Assign {
                Column {
                  Id: 32
                }
                Function {
                  Arguments {
                    Id: 19
                  }
                  Arguments {
                    Id: 31
                  }
                  FunctionType: YQL_KERNEL
                  KernelIdx: 9
                  YqlOperationId: 0
                }
              }
            }
            Command {
              Filter {
                Predicate {
                  Id: 32
                }
              }
            }
            Command {
              Projection {
                Columns {
                  Id: 7
                }
                Columns {
                  Id: 11
                }
                Columns {
                  Id: 10
                }
                Columns {
                  Id: 8
                }
                Columns {
                  Id: 9
                }
                Columns {
                  Id: 6
                }
                Columns {
                  Id: 5
                }
                Columns {
                  Id: 13
                }
                Columns {
                  Id: 3
                }
                Columns {
                  Id: 2
                }
                Columns {
                  Id: 12
                }
                Columns {
                  Id: 4
                }
                Columns {
                  Id: 1
                }
              }
            }
        )";
        
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(programTxt, &program));
        
        TString result = R"($14 = Const Text: "$.\"client_xid\""
$15 = JsonValue $10 $14
$16 = Const Bytes: "52175"
$17 = Equals $15 $16
$18 = Const Uint8: 0
$19 = Coalesce $17 $18
$20 = Const Text: "$.\"try_index\""
$21 = JsonExists $10 $20
$22 = FUNC_CAST_TO_UINT8 $21
$23 = Const Uint8: 0
$24 = Coalesce $22 $23
$25 = Const Text: "$.\"items\""
$26 = JsonValue $10 $25
$27 = Const Bytes: "AAAA/"
$28 = StringContains $26 $27
$29 = Const Uint8: 0
$30 = Coalesce $28 $29
$31 = And $24 $30
$32 = And $19 $31
Filter $32
Projection $7 $11 $10 $8 $9 $6 $5 $13 $3 $2 $12 $4 $1
)";
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NArrow::NPrinter::SSAToPrettyString(program), result);        
    }
}
