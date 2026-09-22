pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.45.3";

  src = fetchFromGitHub {
    owner = "sqlite";
    repo = "sqlite";
    rev = "version-${version}";
    hash = "sha256-djlZvfQp08hQbrvy+wBQ9UaqOW0LIMqOrlU0tXuK/J0=";
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
