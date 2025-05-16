pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.5.4";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-event-stream";
    rev = "v${version}";
    hash = "sha256-Y4vyoNlYdIQg/NggBoYtX4CPiCzG24a4mKG8VGTdqy8=";
  };
}
