pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.5";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-87reH6rR3+R+2E4HBxjwlqa16gorzsfYgo2GgcW5kww=";
  };
}
