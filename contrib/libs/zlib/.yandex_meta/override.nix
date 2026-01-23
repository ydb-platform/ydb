pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.1.2";

  src = fetchFromGitHub {
    owner = "madler";
    repo = "zlib";
    rev = "v${version}";
    hash = "sha256-5g/Jo8M/jvkgV0NofSAV4JdwJSk5Lyv9iGRb2Kz/CC0=";
  };
}
