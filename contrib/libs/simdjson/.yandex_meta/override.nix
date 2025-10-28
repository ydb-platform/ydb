pkgs: attrs: with pkgs; rec {
  version = "4.0.7";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-8pmFtMpML7tTXbH1E3aIpSTQkNF8TFcIPOm2nwnKxkA=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
