pkgs: attrs: with pkgs; rec {
  version = "4.2.0";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-TtYGQmB9a+kuYYpq721grpSlIlzPcsuPTaUBNwvttXg=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
