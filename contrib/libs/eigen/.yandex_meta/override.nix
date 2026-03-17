pkgs: attrs: with pkgs; rec {
  version = "2021-04-12";
  revision = "a2c0542010fcab7d8dee536918994df18660f151";

  src = fetchFromGitLab {
    owner = "libeigen";
    repo = "eigen";
    rev = "${revision}";
    hash = "sha256-IsNZsehJegmsyCsG8DQ0z6a9OD7OxqSWKebQ5NHVh/Q=";
  };

  patches = [ ];

  postPatch = ''
    # Make headers non-executable.
    find . -type f -name '*.h' -executable -exec chmod -x {} +
  '';
}
