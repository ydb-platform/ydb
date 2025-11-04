pkgs: attrs: with pkgs; with attrs; rec {
  version = "22.5";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-sbxl9uUFvTgczzdv7UkJHjACXYLF2FHGmhZEE8lFLs4=";
  };
}
