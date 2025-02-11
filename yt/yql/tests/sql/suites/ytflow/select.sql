use plato;

pragma Engine = "ytflow";
pragma Ytflow.TestPipelineFile = "test_pipeline.yson";

insert into Output
select value, codec from Input;
