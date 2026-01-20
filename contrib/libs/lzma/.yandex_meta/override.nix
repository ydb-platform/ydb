pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.8.2";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-51gKoWSNAEyGPjIcvKzYIQD+WbRAYvXYMBVP6/OVeH4=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
