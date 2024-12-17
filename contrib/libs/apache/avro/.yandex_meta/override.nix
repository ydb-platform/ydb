pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.11.3";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "avro";
    rev = "release-${version}";
    hash = "sha256-yHr/mQfGU8RjauE2Jkz6rHmoELlchLOjq0nPamU6bcM=";
  };

  buildInputs = [
    boost
    snappy
  ];

  patches = [];
}
