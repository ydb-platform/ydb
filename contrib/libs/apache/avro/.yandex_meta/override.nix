pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.12.0";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "avro";
    rev = "release-${version}";
    hash = "sha256-n6NtSFGgpd0D6ZXN7xz2UTkKjdkbX7QwHwwq6OTwFg0=";
  };

  nativeBuildInputs = [
    cmake
    cacert
    git
    python3
  ];

  buildInputs = [
    boost
    snappy
  ];

  patches = [];
}
