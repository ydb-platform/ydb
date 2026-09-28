pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.3.3";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-compression";
    rev = "v${version}";
    hash = "sha256-rYyodLQtfYwIICGTkEYJ/kOh/9gTWzLQPo5Pz1bbsRw=";
  };
}
