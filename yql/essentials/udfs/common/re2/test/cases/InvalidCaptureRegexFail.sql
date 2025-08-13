/* syntax version 1 */
$invalidCaptureRegexp = Re2::Capture("[");

select $invalidCaptureRegexp("abc");
