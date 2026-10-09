$input = SELECT 42ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
    WITH CONNECTION='profiles_http',
         INPUT_TYPE=Struct<id:Uint64>,
         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT id, name, score FROM $profiles;
