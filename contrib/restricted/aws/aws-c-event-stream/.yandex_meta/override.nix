pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.5.7";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-event-stream";
    rev = "v${version}";
    hash = "sha256-JvjUrIj1bh5WZEzkauLSLIolxrT8CKIMjO7p1c35XZI=";
  };
}
