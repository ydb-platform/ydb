pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.41.2";

  src = fetchFromGitHub {
    owner = "sqlite";
    repo = "sqlite";
    rev = "version-${version}";
    hash = "sha256-3l2jnE7Dp5FhQa1mfYmIq9Mptc/Fe3J+JJsNDTF0gNA=";
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
