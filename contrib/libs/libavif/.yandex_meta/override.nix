pkgs: attrs: with pkgs; rec {
  version = "1.3.0";
  
  src = fetchFromGitHub {
    owner = "AOMediaCodec";
    repo = "libavif";
    rev = "v${version}";
    sha256 = "148l0fd1nsd88ivnx2rps66a4dck6r3faacmyihhbn6sjp17m7nh";
  };
  
  nativeBuildInputs = [ cmake ninja ];
  
  cmakeFlags = [
    # Минимальная конфигурация - только AOM кодек
    "-DAVIF_CODEC_AOM=SYSTEM"
    "-DAVIF_CODEC_AOM_DECODE=ON"
    "-DAVIF_CODEC_AOM_ENCODE=ON"
    
    # Отключить все опциональные кодеки
    "-DAVIF_CODEC_DAV1D=OFF"
    "-DAVIF_CODEC_LIBGAV1=OFF"
    "-DAVIF_CODEC_RAV1E=OFF"
    "-DAVIF_CODEC_SVT=OFF"
    "-DAVIF_CODEC_AVM=OFF"
    
    # Отключить все опциональные зависимости
    "-DAVIF_LIBYUV=OFF"                 # использовать встроенную реализацию
    "-DAVIF_LIBSHARPYUV=OFF"
    "-DAVIF_LIBXML2=OFF"
    "-DAVIF_ZLIBPNG=OFF"
    "-DAVIF_JPEG=OFF"
    
    # Отключить сборку приложений и тестов
    "-DAVIF_BUILD_APPS=OFF"
    "-DAVIF_BUILD_TESTS=OFF"
    "-DAVIF_BUILD_EXAMPLES=OFF"
    "-DAVIF_BUILD_MAN_PAGES=OFF"
    
    # Дополнительные настройки
    "-DAVIF_ENABLE_WERROR=OFF"
    "-DAVIF_ENABLE_COVERAGE=OFF"
    "-DAVIF_ENABLE_COMPLIANCE_WARDEN=OFF"
    
    # Отключить экспериментальные функции
    "-DAVIF_ENABLE_EXPERIMENTAL_MINI=OFF"
    "-DAVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM=OFF"
    "-DAVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI=OFF"
  ];
  
  buildInputs = [
    libaom    # единственная обязательная зависимость
  ];
}
