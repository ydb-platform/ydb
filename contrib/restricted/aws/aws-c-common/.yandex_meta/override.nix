pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.12.2";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-mywJ1FPCfXvZCJoonuwhXyjf+W/RvtqFl9v64cJoKrc=";
  };
}
