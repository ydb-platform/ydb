pkgs: attrs: with pkgs; rec {
  version = "3.12.2";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-TjUPySFwwTlD4fLpHoUywAeWvVvi7Hg1wxzgE9vohrs=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
