pkgs: attrs: with pkgs; rec {
  version = "3.10.1";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-UfGt5lKmpqc21Hln4t/4KJfg+3V/hqX3UYgpCvlhkrM=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
