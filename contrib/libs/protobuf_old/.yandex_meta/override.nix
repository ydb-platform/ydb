pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.19.0";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-70SSdx7wzcNBS50/2M64SU5LRZ1LgrRHfCBjeBrJpGc=";
  };
}

