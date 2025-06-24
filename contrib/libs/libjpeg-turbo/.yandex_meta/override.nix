pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.1.1";

  src = fetchFromGitHub {
    owner = "libjpeg-turbo";
    repo = "libjpeg-turbo";
    rev = "${version}";
    hash = "sha256-yGCMtAa0IjyeSBv3HxCQfYDSbNSbscj3choU6D2dlp8=";
  };

  patches = [
    ./issue817-add-wrappers.patch
  ];
}
