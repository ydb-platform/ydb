pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.8.1";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-vGUNoX5VTM0aQ5GmBPXip97WGN9vaVrQLE9msToZyKs=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
