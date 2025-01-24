$id = ($json) -> { RETURN Json2::Serialize(Json2::Parse($json)); };

SELECT
    $id("[]"),
    $id("{}"),
    $id("[1, 3, 4, 5, 6]"),
    $id(@@{"x": 1234}@@);

$id_jd = ($json) -> { RETURN Json2::SerializeToJsonDocument(Json2::Parse($json)); };

SELECT
    $id_jd("[]"),
    $id_jd("{}"),
    $id_jd("[1, 3, 4, 5, 6]"),
    $id_jd(@@{"x": 1234}@@);
