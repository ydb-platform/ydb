/* postgres can not */
/* syntax version 1 */

$timeout = 60 * 30;

$init = ($row) -> (AsTuple($row.unixtime, $row.unixtime, $row.video_content_id));
$update = ($row, $state) -> {
  $is_end_session = (($row.unixtime - $state.1) >= $timeout) or ($row.video_content_id is not null and $row.video_content_id != ($state.2 ?? "-")) ?? false;
  $new_state = AsTuple(
      IF($is_end_session, $row.unixtime, $state.0),
      $row.unixtime,
      IF(
          $is_end_session,
          $row.video_content_id,
          $state.2
        )
    );
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> (
    AsTuple($row.unixtime, $state.2)
);

$source = [
    <|
        vsid: "v",
        unixtime: 1650624253,
        video_content_id: null,
    |>,
    <|
        vsid: "v",
        unixtime: 1650624255,
        video_content_id: "b",
    |>,
    <|
        vsid: "v",
        unixtime: 1650624256,
        video_content_id: null,
    |>,
    <|
        vsid: "v",
        unixtime: 1650624257,
        video_content_id: "b",
    |>,
    <|
        vsid: "v",
        unixtime: 1650634257,
        video_content_id: "b",
    |>,
    <|
        vsid: "v",
        unixtime: 1650634258,
        video_content_id: "c",
    |>
];


SELECT
  vsid,
  session_start,
  COUNT(*) AS session_size
FROM as_table($source)
GROUP BY vsid, SessionWindow(unixtime, $init, $update, $calculate) AS session_start
order by vsid, session_start
