pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.43.2";

  src = fetchFromGitHub {
    owner = "sqlite";
    repo = "sqlite";
    rev = "version-${version}";
    hash = "sha256-JAR8/xRit9sFahVWm9Lj3jsUFX6KD36+8yFvvdpjlVU=";
  };

  nativeBuildInputs = [ tcl ];
  postConfigure = ''
      make sqlite3.c
      cp ./src/test_multiplex.* ./
    '';

  preBuild = "";
  CFLAGS = "-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DHAVE_USLEEP";
  LDFLAGS = "-lm";

  patches = [];
}
