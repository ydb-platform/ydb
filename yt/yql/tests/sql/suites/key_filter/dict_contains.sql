/* postgres can not */
select * from plato.Input where key in YQL::DictFromKeys(ParseType("String"), AsTuple("075", "023", "037")) order by key;
