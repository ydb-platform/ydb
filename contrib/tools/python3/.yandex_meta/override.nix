pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.13.14";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-jqvOlqpH9gZtYhyLn+BeWeWj/YHZX3ine6X4NPc59fY=";
  };

  patches = [];
  postPatch = "";
}
