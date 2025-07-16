use plato;

pragma Engine = "ytflow";

pragma Ytflow.Cluster = "plato";
pragma Ytflow.PipelinePath = "pipeline";

pragma Ytflow.YtConsumerPath = "consumers/main_consumer";
pragma Ytflow.YtProducerPath = "consumers/main_producer";

insert into Output
select 
    String::AsciiToUpper(string_field) as string_field,
    LENGTH(string_field) as int64_field,
    String::Contains(string_field, "bar") as bool_field
from Input;
