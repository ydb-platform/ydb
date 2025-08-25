pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.3.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-compression";
    rev = "v${version}";
    hash = "sha256-gpru+hnppgLHhcPfVBOaMdcT6e8wUjZmY7Caaa/KAW4=";
  };
}
