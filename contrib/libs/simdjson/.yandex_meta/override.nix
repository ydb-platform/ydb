pkgs: attrs: with pkgs; rec {
  version = "3.11.3";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-Gh9/vOfhEh3RXT4cSb6KpDqjYS0d1kje1JDbDiWTR0o=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
