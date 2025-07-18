pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.2";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-LNIJRUd+5ob75MM8jX+lVoIsNeBinCcqLzCBJk6/j9Q=";
  };
}
