pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.4.2";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-event-stream";
    rev = "v${version}";
    hash = "sha256-wj3PZshUay3HJy+v7cidDL4mDAqSDiX+MmQtJDK4rTI=";
  };
}
