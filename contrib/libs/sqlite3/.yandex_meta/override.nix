pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.42.1";

  src = fetchFromGitHub {
    owner = "sqlite";
    repo = "sqlite";
    rev = "version-${version}";
    hash = "sha256-L+FV6Qb6uEOl14wFq1aCF7QoYv2QxX+AwhPNk29VLE4=";
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
