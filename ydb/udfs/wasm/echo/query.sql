$input = SELECT "hello" AS message UNION ALL SELECT "" AS message;
$result = PROCESS $input USING EXTERNAL FUNCTION('Echo', 'Echo')
    WITH CONNECTION='echo_http',
         INPUT_TYPE=Struct<message:String>,
         OUTPUT_TYPE=Struct<value:String,length:Uint64,empty:Bool,delta:Int64>;
SELECT * FROM $result;
