USE plato;
PRAGMA DisableSimpleColumns;
PRAGMA yt.LookupJoinLimit="64k";
PRAGMA yt.LookupJoinMaxRows="100";

$campaigns_data = AsList(
    AsStruct(Just(1) as id),
    AsStruct(Just(2) as id));
    
$strategies_data = AsList(
    AsStruct(Just(1) as id),
    AsStruct(Just(2) as id));
    
$lottery_data = AsList(
    AsStruct(Just(1) as id, Just(2) as campaign_id, Just(3) as strategy_id));


INSERT INTO @campaigns SELECT * FROM AS_TABLE($campaigns_data) ORDER BY id;
INSERT INTO @strategies SELECT * FROM AS_TABLE($strategies_data) ORDER BY id;
INSERT INTO @lottery SELECT * FROM AS_TABLE($lottery_data) ORDER BY id;

COMMIT;

SELECT
    lottery.id AS lottery_id
FROM @lottery AS lottery
    JOIN @campaigns AS campaigns ON lottery.campaign_id = campaigns.id
    JOIN @strategies AS strategies ON lottery.strategy_id = strategies.id
