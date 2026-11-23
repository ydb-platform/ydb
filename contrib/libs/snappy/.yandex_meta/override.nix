pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.1";

  src = fetchFromGitHub {
    owner = "google";
    repo = "snappy";
    rev = version;
    hash = "sha256-QVBzdVdQAHOBD7wwbOncYNbA4unRgTLI2Xac9RjERM4=";
  };

  patches = [];
}
