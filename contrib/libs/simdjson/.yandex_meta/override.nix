pkgs: attrs: with pkgs; rec {
  version = "3.11.1";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-iUbUkov8RAytdzkstc1Q1kkJtFLQxeRMjcEfi7NSzgs=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
