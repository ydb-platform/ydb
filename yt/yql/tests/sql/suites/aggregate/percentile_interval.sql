/* syntax version 1 */

$zero = unwrap( cast(0 as Interval) );
$one = unwrap( cast (1 as Interval ) );

-- we want to check both optional<interval> and plain interval
$prepared = select
              cast (key As Interval) ?? $zero as interval_data
              from plato.Input;

$source = select
              interval_data
              , interval_data + $one as interval_data2
              , just( interval_data ) as optional_interval_data
              from $prepared;

-- percentile factory can work with plain number and with tuple of numbers.
-- to achive second call we must make several percentile invocations with
-- same column name
$data_plain = select
  percentile(interval_data, 0.8) as result
  from $source;

-- optimization should unite this into one call to percentile with tuple as argument
$data_tuple = select
  percentile(interval_data2, 0.8) as result_1
  , percentile(interval_data2, 0.6) as result_2
  from $source;

$data_optional = select
  percentile(optional_interval_data, 0.4) as result
  from $source;

select EnsureType(result, Interval?) from $data_plain;
select EnsureType(result_1, Interval?) from $data_tuple;
select EnsureType(result_2, Interval?) from $data_tuple;
select result from $data_optional;

