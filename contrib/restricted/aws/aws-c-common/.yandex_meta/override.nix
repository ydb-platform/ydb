pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.0";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-x4bwxV0MrX0sKxjUJGa/+fE492TnluAO5UafGDUp3D4=";
  };
}
