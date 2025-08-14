pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-04-29";

  src = fetchFromGitHub {
    owner = "google";
    repo = "boringssl";
    rev = "2db0eb3f96a5756298dcd7f9319e56a98585bd10";
    hash = "sha256-+G7BcdtU8AeNMY4NLQgKpgF28/CS9FIjf+vaOd+Wf6o=";
  };

#  preConfigure = "
#    find .. -type f -exec sed -i \'s/GOPROXY=off/GOPROXY=https:\\/\\/proxy\\.golang\\.org\\,direct/g\' {} \\;
#    find .. -type f -exec sed -i \'s/GOPROXY=\"off\"/GOPROXY=\"https:\\/\\/proxy\\.golang\\.org\\,direct\"/g\' {} \\;
#  ";

  patches = [
    ./boringssl_prefix_symbols_asm.patch
    ./boringssl_prefix_symbols_nasm.patch
    ./boringssl_prefix_symbols.patch
    ./boringssl-cmake.patch
    ./sources-cmake.patch
  ];

}
