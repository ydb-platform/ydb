pkgs: attrs: with pkgs; rec {
  version = "3.11.2";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-MQexzJyxLst8MdZ2H2LN6OG7nqgYBW3V1Euudb979yY=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
