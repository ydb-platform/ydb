pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.8";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-1Z2SMEut5YY9tTtrzPpmXcsIQKw5MGcGI4l0ysJbg28=";
  };

  patches = [];
  postPatch = "";
}
