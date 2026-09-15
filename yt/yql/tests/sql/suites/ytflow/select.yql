use plato;

pragma Engine = "ytflow";

pragma Ytflow.Cluster = "plato";
pragma Ytflow.PipelinePath = "pipelines/test";

pragma Ytflow.YtConsumerPath = "yql_ytflow/consumers/main_consumer";
pragma Ytflow.YtProducerPath = "yql_ytflow/producers/main_producer";

insert into Output
select 
    string_field || "_ytflow" as string_field,
    int64_field * 100 as int64_field,
    int64_field > 10 as bool_field
from Input
where string_field = "foo" or int64_field >= 100;
