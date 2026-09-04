$invalidCaptureRegexp = Re2::Capture("[");

select $invalidCaptureRegexp("abc");
