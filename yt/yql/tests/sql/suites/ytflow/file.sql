use plato;

pragma Engine = "ytflow";

pragma Ytflow.Cluster = "plato";
pragma Ytflow.PipelinePath = "pipeline";

pragma Ytflow.YtConsumerPath = "consumers/main_consumer";
pragma Ytflow.YtProducerPath = "consumers/main_producer";

$suffix = FileContent("file.txt");
$delta = Cast(FileContent("http_file.txt") as Int64);
$path = FilePath("file.txt");

insert into Output
select 
    string_field || $suffix || "_" || $path as string_field,
    int64_field + $delta as int64_field,
    true as bool_field
from Input;
