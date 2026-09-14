pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.44.5";

  src = fetchFromGitHub {
    owner = "sqlite";
    repo = "sqlite";
    rev = "version-${version}";
    hash = "sha256-nMNMTUynVtVukui5sYQPoGSbMZImjTyW0aqwGwuki4s=";
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
