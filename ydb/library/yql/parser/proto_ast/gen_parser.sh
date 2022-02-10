#/bin/sh -e

if ! [ -d "./org/antlr/codegen/templates/" ]; then
    echo "gen_parser.sh must be called from yql/library/proto_ast folder"
    exit 1;
fi

if [ -z $1 ] || ! [ -f $1 ]; then
    echo "Usage: gen_parser.sh <grammar_file>"
    exit 1
fi

ANTLR3="./antlr3/antlr-3.5.2-complete-no-st3.jar"

../../../../ya tool java -d64 -jar $ANTLR3 -lib ./ -language protobuf $1 
../../../../ya tool java -d64 -jar $ANTLR3 -lib ./ $1 
