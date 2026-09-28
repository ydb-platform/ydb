$invalidRe = Re2::FindAndConsume("[");

SELECT $invalidRe("abaa");
