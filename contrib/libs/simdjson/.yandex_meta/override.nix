pkgs: attrs: with pkgs; rec {
  version = "4.1.0";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-N3NPE9R8VipspCwH2dY339WUGt51aqkYpLTr/PPVRQ4=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
