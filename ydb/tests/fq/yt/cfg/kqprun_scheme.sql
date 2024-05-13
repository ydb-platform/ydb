CREATE OBJECT yt_token (TYPE SECRET) WITH (value = "token");

CREATE EXTERNAL DATA SOURCE plato WITH (
    SOURCE_TYPE="YT",
    LOCATION="localhost",
    AUTH_METHOD="TOKEN",
    TOKEN_SECRET_NAME="yt_token"
);
