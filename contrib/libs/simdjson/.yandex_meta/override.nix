pkgs: attrs: with pkgs; rec {
  version = "3.12.1";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-ujeG3yidZJZV6x4RQQYXwbslQcRx3HaqjzgaU2A4cQU=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
