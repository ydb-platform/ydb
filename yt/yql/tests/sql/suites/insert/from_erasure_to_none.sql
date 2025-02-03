USE plato;

PRAGMA yt.PublishedErasureCodec = 'none';

INSERT INTO Output
SELECT *
FROM Input
LIMIT 100;
